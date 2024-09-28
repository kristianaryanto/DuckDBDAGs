import duckdb
import logging
from dbt.cli.main import dbtRunner, dbtRunnerResult
from utils.api_minio import upload_to_minio
from datetime import datetime
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Initialize and run DBT transformations
def run_dbt_transformation(dbt_model_name):
    logger.info("Initializing DBT Runner.")
    dbt = dbtRunner()
    dbt_project_path = "/opt/airflow/dbt_gt"

    # Create CLI args as a list of strings
    cli_args = ["run", "--select", f"{dbt_model_name}", "--project-dir", f"{dbt_project_path}", "--profiles-dir", f"{dbt_project_path}"]

    logger.info(f"Running DBT model: {dbt_model_name} with CLI args: {cli_args}")

    # Run the DBT command
    try:
        result = dbt.invoke(cli_args)
        logger.info(f"DBT model {dbt_model_name} completed with status: {result.success}")
    except Exception as e:
        logger.error(f"Failed to run DBT model {dbt_model_name}: {e}")
        raise

# Export DuckDB table to Parquet
def export_duckdb_to_parquet(table_name):
    logger.info(f"Connecting to DuckDB database and exporting table {table_name} to Parquet.")
    conn = duckdb.connect("/opt/airflow/duckdb.db")
    try:
        query = f"SELECT * FROM {table_name}"
        parquet_file_path = f"data-lake/{table_name}.parquet"
        
        logger.info(f"Executing query: {query}")
        conn.execute(f"""COPY ({query}) TO '{parquet_file_path}' (FORMAT PARQUET)""")
        
        logger.info(f"Table {table_name} exported to Parquet file: {parquet_file_path}")

        # Drop the table after exporting
        logger.info(f"Dropping table {table_name} from DuckDB.")
        conn.execute(f"""DROP TABLE IF EXISTS {table_name}""")
    except Exception as e:
        logger.error(f"Failed to export table {table_name} to Parquet: {e}")
        raise
    finally:
        conn.close()
        logger.info("DuckDB connection closed.")
    
    return parquet_file_path

# Main function to run the full transformation and export process
def run_transformation_and_export(dbt_model_name,minio_client,bucket_name):
    logger.info(f"Starting transformation and export process for DBT model: {dbt_model_name}")
    
    # Run DBT transformation
    run_dbt_transformation(dbt_model_name)

    # Export the transformed data to Parquet
    parquet_file_path = export_duckdb_to_parquet(dbt_model_name)

    logger.info(f"Transformation and export process completed for DBT model: {dbt_model_name}")


    partition_by_date = datetime.now().strftime('%Y-%m-%d')
        
    logger.info(f"Starting upload {dbt_model_name} to minio bucket {bucket_name}.")
    parquet_file_path_minio = f'datalake/airflow/{dbt_model_name}/{partition_by_date}/{dbt_model_name}.parquet'


        # upload to minio
    upload_to_minio(minio_client, bucket_name, parquet_file_path, parquet_file_path_minio)
    logger.info(f"Success upload {dbt_model_name} to minio bucket {bucket_name}.")
    
