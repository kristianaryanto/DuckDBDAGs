import aiopg
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import time
import logging
import asyncio
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 1. Create DSN String for aiopg
def create_dsn(username, password, server, database):
    logger.info("Creating DSN string.")
    dsn = f"dbname={database} user={username} password={password} host={server} port=5432"
    return dsn

# 2. Create Connection Pool
async def create_connection_pool(dsn):
    logger.info("Creating a connection pool.")
    pool = await aiopg.create_pool(dsn)
    return pool

# 3. Fetch Data Function
async def fetch_data(pool, query, batch_size, total_rows, order_by, concurrency_limit):
    logger.info("Fetching data from the database.")
    semaphore = asyncio.Semaphore(concurrency_limit)
    all_batches = []
    column_names = None
    tasks = []

    for offset in range(0, total_rows, batch_size):
        batch_number = offset // batch_size + 1
        task = asyncio.create_task(fetch_batch(pool, query, batch_size, offset, batch_number, order_by, semaphore))
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    for rows, cols in results:
        if rows is None:
            continue
        if column_names is None:
            column_names = cols
        all_batches.extend(rows)

    if not all_batches:
        logger.warning("No data fetched from the database.")
        return None, None

    return all_batches, column_names

# 4. Fetch a Batch of Data
async def fetch_batch(pool, query, batch_size, offset, batch_number, order_by, semaphore):
    async with semaphore:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                paginated_query = f"""
                {query} ORDER BY {order_by}
                OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"""
                await cursor.execute(paginated_query)
                rows = await cursor.fetchall()
                if not rows:
                    return None, None
                logger.info(f"Fetched {len(rows)} rows in batch {batch_number}.")
                column_names = [desc[0] for desc in cursor.description]
                return rows, column_names

# 5. Insert or Update Data Function (using ON CONFLICT clause)
async def insert_or_update(pool, table_name, rows, column_names, conflict_column, semaphore):
    logger.info("Inserting or updating data using ON CONFLICT clause.")
    async with semaphore:
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Construct the insert query with ON CONFLICT
                columns_str = ", ".join(column_names)
                placeholders = ", ".join([f"%s" for _ in column_names])
                update_columns = ", ".join([f"{col} = EXCLUDED.{col}" for col in column_names if col != conflict_column])

                query = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_column})
                DO UPDATE SET {update_columns}
                """

                # Execute the insert/update for each row
                for row in rows:
                    await cursor.execute(query, row)
                logger.info(f"Inserted/Updated {len(rows)} rows into {table_name}.")

# 6. Update Query Function (for standalone updates)
import asyncio

# Updated function to run an UPDATE query with multiple SET values
async def run_update_query(pool, table_name, set_values, where_clause, where_params, concurrency_limit):
    semaphore = asyncio.Semaphore(concurrency_limit)
    async with semaphore:
        # Dynamically construct the SET clause with multiple columns
        set_clause = ", ".join([f"{col} = %s" for col in set_values.keys()])
        set_params = tuple(set_values.values())

        logger.info(f"Updating {table_name}: Setting {set_values} where {where_clause}")

        # Construct the SQL query with dynamic SET and WHERE clauses
        query = f"""
        UPDATE {table_name}
        SET {set_clause}
        WHERE {where_clause};
        """

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    # Execute the query with set values and where parameters
                    await cursor.execute(query, (*set_params, *where_params))
                    logger.info(f"Updated rows in {table_name} with {where_clause}.")
                except Exception as e:
                    logger.error(f"Error during update: {e}")
                    raise


# 7. Convert to PyArrow Table
def convert_to_pyarrow_table(rows, column_names):
    logger.info("Converting data to PyArrow table.")
    df = pa.Table.from_pylist([{col: row[i] for i, col in enumerate(column_names)} for row in rows])
    return df

# 7. Convert to PyArrow Table to list
def get_column_as_list(table, column_name):
    # Extract the column using the column name
    column = table[column_name]
    
    # Convert to Python list
    column_list = column.to_pylist()
    
    return column_list

# 8. Write to Parquet or DuckDB
def write_to_storage(df, parquet_file_path=None, duckdb_conn=None, duckdb_table=None):
    if parquet_file_path:
        logger.info(f"Writing data to Parquet file at {parquet_file_path}.")
        pq.write_table(df, parquet_file_path)
    elif duckdb_conn and duckdb_table:
        logger.info(f"Inserting data into DuckDB table {duckdb_table}.")
        # Convert PyArrow Table to DuckDB and insert into the table
        duckdb_conn.execute(f"CREATE TABLE IF NOT EXISTS {duckdb_table} AS SELECT * FROM df")
    else:
        logger.warning("No valid storage option provided (Parquet or DuckDB).")

# 9. Main Function (Orchestrating All Tasks)
async def orchestrate_data_tasks(username, password, server, database, query, parquet_file_path=None,
                                 total_rows=0, batch_size=1000, order_by='id', concurrency_limit=1,
                                 insert_duckdb=False, duckdb_path=None, duckdb_table=None,
                                 insert_update_db=False, db_table_name=None, conflict_column=None):
    
    logger.info("Starting orchestrate_data_tasks function.")
    dsn = create_dsn(username, password, server, database)
    pool = await create_connection_pool(dsn)

    semaphore = asyncio.Semaphore(concurrency_limit)

    try:
        start_time = time.time()

        # 1. Fetch Data
        rows, column_names = await fetch_data(pool, query, batch_size, total_rows, order_by, concurrency_limit)
        if not rows:
            logger.warning("No data fetched, exiting.")
            return

        # 2. Insert or Update Data if needed
        if insert_update_db and db_table_name and conflict_column:
            await insert_or_update(pool, db_table_name, rows, column_names, conflict_column, semaphore)

        # 3. Convert to PyArrow Table
        df = convert_to_pyarrow_table(rows, column_names)

        # 4. Write to Parquet or DuckDB
        if insert_duckdb and duckdb_path and duckdb_table:
            logger.info(f"Connecting to DuckDB at {duckdb_path}.")
            with duckdb.connect(duckdb_path) as conn:
                write_to_storage(df, duckdb_conn=conn, duckdb_table=duckdb_table)
        elif parquet_file_path:
            write_to_storage(df, parquet_file_path=parquet_file_path)
        else:
            logger.warning("No storage option specified (Parquet or DuckDB).")

        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Data processed and written in {elapsed_time:.2f} seconds.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        logger.info("Closing the connection pool.")
        pool.close()
        await pool.wait_closed()
        logger.info("Connection pool closed.")

