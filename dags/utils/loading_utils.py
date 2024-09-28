import pyarrow.parquet as pq
import asyncio
import aioodbc
import logging
import jaydebeapi
# from jtds_columns_converter imxport convert_parquet_schema_to_sql  # Import the converter
import datetime
import pyarrow as pa


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)




def convert_parquet_schema_to_sql(schema):
    """
    Converts Parquet schema to SQL Server data types for jTDS driver.
    
    Parameters:
        schema (pyarrow.Schema): The schema of the Parquet file.
        
    Returns:
        dict: A dictionary mapping column names to their SQL Server types.
    """
    
    type_mapping = {
        pa.string(): "VARCHAR(MAX)",
        pa.int32(): "INT",
        pa.int64(): "BIGINT",
        pa.float32(): "FLOAT",
        pa.float64(): "FLOAT",
        pa.timestamp('ns'): "DATETIME",
        pa.date32(): "DATE",
        pa.date64(): "DATE",
        pa.bool_(): "BIT"
    }

    sql_schema = {}
    for field in schema:
        sql_type = type_mapping.get(field.type, "VARCHAR(MAX)")  # Default to VARCHAR(MAX) for unknown types
        sql_schema[field.name] = sql_type

    return sql_schema



async def create_table_if_not_exists(cursor, table_name, schema):
    logging.info(f"Checking if table '{table_name}' exists.")
    columns = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
    create_table_query = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
    CREATE TABLE {table_name} ({columns})
    """
    await cursor.execute(create_table_query)
    logging.info(f"Table '{table_name}' created or already exists.")

async def truncate_table(cursor, table_name):
    logging.info(f"Truncating table '{table_name}'.")
    truncate_query = f"TRUNCATE TABLE {table_name}"
    await cursor.execute(truncate_query)
    logging.info(f"Table '{table_name}' truncated.")

async def merge_tables(cursor, main_table, temp_table, schema, merge_key):
    logging.info(f"Performing merge from temp table '{temp_table}' into '{main_table}'.")

    merge_conditions = " AND ".join(
        [f"target.{col} = source.{col}" for col in merge_key]
    )
    update_set = ", ".join([f"target.{col} = source.{col}" for col in schema.names])
    insert_columns = ", ".join(schema.names)
    insert_values = ", ".join([f"source.{col}" for col in schema.names])
    
    merge_query = f"""
    MERGE INTO {main_table} AS target
    USING {temp_table} AS source
    ON {merge_conditions}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values});
    """
    await cursor.execute(merge_query)
    logging.info(f"Merge operation completed from '{temp_table}' to '{main_table}'.")

async def insert_batch(dsn, insert_query, batch_data, batch_number, semaphore, driver_type):
    logging.info(f"Starting insertion for batch {batch_number} with {len(batch_data)} records.")
    batch_tuples = [tuple(row.values()) for row in batch_data]
    
    if driver_type == 'odbc':
        async with semaphore:
            async with aioodbc.connect(dsn=dsn) as conn:
                async with conn.cursor() as cursor:
                    await cursor.executemany(insert_query, batch_tuples)
    elif driver_type == 'jtds':
        # Use JayDeBeApi for jTDS driver (no concurrency here)
        conn = jaydebeapi.connect(dsn[0], dsn[1], dsn[2], dsn[3])
        cursor = conn.cursor()
        cursor.executemany(insert_query, batch_tuples)
        cursor.close()
        conn.close()

    logging.info(f"Completed insertion for batch {batch_number}.")

async def insert_parquet_to_sql(server, database, username, password, table_name, parquet_file, driver_type='odbc', ingestion_type='truncate/insert', merge_key=None, batch_size=1000, max_concurrent_inserts=5, schema_columns=None):
    logging.info(f"Connecting to SQL Server database '{database}' on server '{server}' using driver '{driver_type}'.")
    
    if driver_type == 'odbc':
        dsn = f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;'
    elif driver_type == 'jtds':
        # DSN for jTDS with JayDeBeApi (JDBC)
        jdbc_url = f"jdbc:jtds:sqlserver://{server}:1433/{database}"
        dsn = [ "net.sourceforge.jtds.jdbc.Driver", jdbc_url, [username, password], "/opt/airflow/jtds/jtds-1.3.1.jar"]

    semaphore = asyncio.Semaphore(max_concurrent_inserts) if driver_type == 'odbc' else None
    
    logging.info(f"Reading Parquet file '{parquet_file}'.")
    table = pq.read_table(parquet_file)
    schema = table.schema  # Define the schema here after reading the Parquet file
    logging.info(f"Parquet file '{parquet_file}' read successfully with {len(table)} rows.")

    # If schema_columns is provided, use it to map the schema
    if schema_columns:
        sql_schema = schema_columns
    else:
        sql_schema = convert_parquet_schema_to_sql(schema)  # Use the new converter

    # Call the processing function with the schema now defined
    await process_table_operations(parquet_file=parquet_file, table_name=table_name, ingestion_type=ingestion_type, schema=schema, merge_key=merge_key, batch_size=batch_size, semaphore=semaphore, dsn=dsn, driver_type=driver_type)

async def process_table_operations(parquet_file, table_name, ingestion_type, schema, merge_key, batch_size, semaphore, dsn, driver_type):
    sql_schema = {
        field.name: "VARCHAR(MAX)" if str(field.type) == "string" else "FLOAT" 
        for field in schema
    }

    # Establish the connection and cursor based on the driver type
    if driver_type == 'odbc':
        async with aioodbc.connect(dsn=dsn) as conn:
            async with conn.cursor() as cursor:
                await create_table_if_not_exists(cursor, table_name, sql_schema)

                if ingestion_type == 'truncate/insert':
                    await truncate_table(cursor, table_name)
                elif ingestion_type == 'update/insert':
                    temp_table_name = f"{table_name}_temp"
                    await create_table_if_not_exists(cursor, temp_table_name, sql_schema)

                columns = ", ".join(schema.names)
                placeholders = ", ".join("?" for _ in schema.names)
                target_table = temp_table_name if ingestion_type == 'update/insert' else table_name
                insert_query = f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders})"

                total_rows = len(pq.read_table(parquet_file))
                batches = [
                    pq.read_table(parquet_file).slice(i, batch_size).to_pylist()
                    for i in range(0, total_rows, batch_size)
                ]

                tasks = []
                for batch_number, batch_data in enumerate(batches, start=1):
                    task = insert_batch(dsn, insert_query, batch_data, batch_number, semaphore, driver_type)
                    tasks.append(task)

                await asyncio.gather(*tasks)

                if ingestion_type == 'update/insert':
                    if not merge_key:
                        raise ValueError("merge_key must be provided for 'update/insert' ingestion type")
                    await merge_tables(cursor, table_name, temp_table_name, schema, merge_key)
                    drop_temp_table_query = f"DROP TABLE {temp_table_name}"
                    await cursor.execute(drop_temp_table_query)

                logging.info(f"Committing transaction to the database.")
                await conn.commit()

    elif driver_type == 'jtds':
        # Use JayDeBeApi (non-async) for jTDS, processing sequentially without concurrency
        conn = jaydebeapi.connect(dsn[0], dsn[1], dsn[2], dsn[3])
        cursor = conn.cursor()
        create_table_if_not_exists_sync(cursor, table_name, sql_schema)  # No 'await' for jTDS

        if ingestion_type == 'truncate/insert':
            truncate_table_sync(cursor, table_name)  # No 'await' for jTDS
        elif ingestion_type == 'update/insert':
            temp_table_name = f"{table_name}_temp"
            create_table_if_not_exists_sync(cursor, temp_table_name, sql_schema)

        columns = ", ".join(schema.names)
        placeholders = ", ".join("?" for _ in schema.names)
        target_table = temp_table_name if ingestion_type == 'update/insert' else table_name
        insert_query = f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders})"

        total_rows = len(pq.read_table(parquet_file))
        batches = [
            pq.read_table(parquet_file).slice(i, batch_size).to_pylist()
            for i in range(0, total_rows, batch_size)
        ]

        for batch_number, batch_data in enumerate(batches, start=1):
            # Process sequentially (no asyncio for jTDS)
            logging.info(f"Inserting batch {batch_number} (jTDS).")
            insert_batch_sync(dsn, insert_query, batch_data)  # No 'await' for jTDS

        if ingestion_type == 'update/insert':
            if not merge_key:
                raise ValueError("merge_key must be provided for 'update/insert' ingestion type")
            merge_tables_sync(cursor, table_name, temp_table_name, schema, merge_key)
            drop_temp_table_query = f"DROP TABLE {temp_table_name}"
            cursor.execute(drop_temp_table_query)

        logging.info(f"Committing transaction to the database.")
        # conn.commit()
        cursor.close()
        conn.close()

def create_table_if_not_exists_sync(cursor, table_name, schema):
    logging.info(f"Checking if table '{table_name}' exists.")
    columns = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
    create_table_query = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
    CREATE TABLE {table_name} ({columns})
    """
    cursor.execute(create_table_query)
    logging.info(f"Table '{table_name}' created or already exists.")

def truncate_table_sync(cursor, table_name):
    logging.info(f"Truncating table '{table_name}'.")
    truncate_query = f"TRUNCATE TABLE {table_name}"
    cursor.execute(truncate_query)
    logging.info(f"Table '{table_name}' truncated.")

def merge_tables_sync(cursor, main_table, temp_table, schema, merge_key):
    logging.info(f"Performing merge from temp table '{temp_table}' into '{main_table}'.")

    merge_conditions = " AND ".join(
        [f"target.{col} = source.{col}" for col in merge_key]
    )
    update_set = ", ".join([f"target.{col} = source.{col}" for col in schema.names])
    insert_columns = ", ".join(schema.names)
    insert_values = ", ".join([f"source.{col}" for col in schema.names])
    
    merge_query = f"""
    MERGE INTO {main_table} AS target
    USING {temp_table} AS source
    ON {merge_conditions}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values});
    """
    cursor.execute(merge_query)
    logging.info(f"Merge operation completed from '{temp_table}' to '{main_table}'.")


def insert_batch_sync(dsn, insert_query, batch_data):
    logging.info(f"Starting insertion with {len(batch_data)} records.")
    
    # Convert datetime objects to ISO 8601 strings and handle type conversions
    batch_tuples = []
    for row in batch_data:
        converted_row = []
        for column_name, value in row.items():
            # Skip the 'ID' column since it is auto-incremented
            if isinstance(value, datetime.datetime):
                value = value.isoformat()
            converted_row.append(value)
        batch_tuples.append(tuple(converted_row))
    
    # Connect using JayDeBeApi and disable auto-commit
    conn = jaydebeapi.connect(dsn[0], dsn[1], dsn[2], dsn[3])
    conn.jconn.setAutoCommit(False)  # Disable auto-commit
    
    cursor = conn.cursor()
    
    try:
        cursor.executemany(insert_query, batch_tuples)
        conn.commit()  # Explicitly commit the transaction
    except Exception as e:
        conn.rollback()  # Roll back the transaction in case of an error
        raise e
    finally:
        cursor.close()
        conn.close()

    logging.info(f"Completed insertion.")


# parquet_file = "/home/yance/Downloads/airflow_new/airflow_new/data-lake/bridyna_penalty.parquet"
# table = pq.read_table(parquet_file)
# schema = table.schema

# sql_schema = convert_parquet_schema_to_sql(schema)

# asyncio.run(insert_parquet_to_sql("10.35.65.167",
#  "BRI_DYNAMIC",
#  "sa",
#  "starbuck",
#  "LAS_T_PENJUALAN",
#  "/home/yance/Downloads/airflow_new/airflow_new/data-lake/bridyna_penalty.parquet",
#  driver_type="jtds"))
