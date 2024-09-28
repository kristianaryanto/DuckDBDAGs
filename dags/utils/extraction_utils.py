import aioodbc
import pyarrow as pa
import pyarrow.parquet as pq
import time
import logging
import asyncio
from datetime import datetime
from utils.api_minio import upload_to_minio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 1. Create DSN String
def create_dsn(username, password, server, database, trusted_conn='yes'):
    logger.info("Creating DSN string.")
    dsn = (f"DRIVER=ODBC Driver 18 for SQL Server;"
           f"SERVER={server};"
           "TrustServerCertificate=yes;"
           "PORT=1433;"
           f"DATABASE={database};"
           f"UID={username};"
           f"PWD={password};")
    return dsn

# 2. Create Connection Pool
async def create_connection_pool(dsn):
    logger.info("Creating a connection pool.")
    pool = await aioodbc.create_pool(dsn=dsn)
    return pool

# 3. Fetch a Batch of Data
async def fetch_batch(dsn, query, batch_size=1000, offset, batch_number, order_by, semaphore):
    async with semaphore:
        async with aioodbc.connect(dsn=dsn) as conn:
            async with conn.cursor() as cursor:
                paginated_query = f"""
                {query} ORDER BY {order_by} ASC
                OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"""
                await cursor.execute(paginated_query)
                rows = await cursor.fetchall()
                if not rows:
                    return None, None
                logger.info(f"Fetched {len(rows)} rows in batch {batch_number}.")
                column_names = [column[0] for column in cursor.description]
                return rows, column_names

# 4. Convert to PyArrow Table
def convert_to_pyarrow_table(rows, column_names):
    logger.info("Converting data to PyArrow table.")
    df = pa.Table.from_pylist([{col: row[i] for i, col in enumerate(column_names)} for row in rows])
    return df

# 5. Write to Parquet File
def write_to_parquet(df, parquet_file_path):
    logger.info(f"Writing data to Parquet file at {parquet_file_path}.")
    pq.write_table(df, parquet_file_path)

# 6. Main Function (Orchestrating All Tasks)
async def fetch_data_to_parquet(username, password, server, database, query, parquet_file_path,minio_client, 
                                bucket_name, table_minio, parquet_file_path_minio=None, batch_size=1000, 
                                total_rows=0, trusted_conn='yes', order_by='id', concurrency_limit=1):
    logger.info("Starting fetch_data_to_parquet function.")
    dsn = create_dsn(username, password, server, database, trusted_conn)
    pool = await create_connection_pool(dsn)

    semaphore = asyncio.Semaphore(concurrency_limit)

    try:
        start_time = time.time()

        all_batches = []
        column_names = None

        tasks = []
        for offset in range(0, int(total_rows), int(batch_size)):
            batch_number = offset // batch_size + 1
            task = asyncio.create_task(fetch_batch(dsn, query, batch_size, offset, batch_number, order_by, semaphore))
            tasks.append(task)
            
        results = await asyncio.gather(*tasks)

        for rows, cols in results:
            if rows is None:
                continue
            if column_names is None:
                column_names = cols
            all_batches.extend(rows)

        if not all_batches:
            logger.warning("No data to write to Parquet. Exiting function.")
            return

        # Convert the accumulated rows to a single PyArrow table
        df = convert_to_pyarrow_table(all_batches, column_names)
        write_to_parquet(df, parquet_file_path)

        partition_by_date = datetime.now().strftime('%Y-%m-%d')
        
        if parquet_file_path_minio is None:
            logger.info(f"Starting upload {table_minio} to minio bucket {bucket_name}.")
            parquet_file_path_minio = f'datalake/airflow/{table_minio}/{partition_by_date}/{table_minio}.parquet'

        # upload to minio
        upload_to_minio(minio_client, bucket_name, parquet_file_path, parquet_file_path_minio)
        logger.info(f"Success upload {table_minio} to minio bucket {bucket_name}.")
    
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Query executed and data written to Parquet in {elapsed_time:.2f} seconds.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        logger.info("Closing the connection pool.")
        pool.close()
        await pool.wait_closed()
        logger.info("Connection pool closed.")
