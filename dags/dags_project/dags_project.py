from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import asyncio
import logging
from utils.extraction_utils import fetch_data_to_parquet
from utils.loading_utils import insert_parquet_to_sql
from utils.transformation_utils import run_transformation_and_export
from airflow.models import Variable
from datetime import timedelta
from minio import Minio
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 28),
    # 'retries': 1
}

with DAG('Project_etl_pipeline',
         default_args=default_args,
         description='Project_etl_pipeline',
         schedule_interval=None,
         catchup=False) as dag:

    @task(retries=1, retry_delay=timedelta(seconds=5))
    def fetch_data(query,
     parquet_file_path,
     username,
     password,
     server,
     database,
     order_by,
     total_rows,
    concurrency_limit,
    minio_client,
    bucket_name,
    table_minio
    ):
        asyncio.run(fetch_data_to_parquet(username,
         password,
         server,
         database,
         query,
         parquet_file_path,
         order_by=order_by,
         total_rows=total_rows,
        concurrency_limit=concurrency_limit,
        minio_client=minio_client,
        bucket_name=bucket_name,
        table_minio=table_minio
        ))

    @task
    def transform_data(dbt_model_name,minio_client,bucket_name):
        run_transformation_and_export(dbt_model_name,minio_client,bucket_name)

    @task
    def load_data(parquet_file_path, target_table, server, database, username, password,ingestion_type,merge_key,driver_type):
        asyncio.run(insert_parquet_to_sql(
    server=server, 
    database=database, 
    username=username, 
    password=password, 
    table_name=target_table, 
    parquet_file=parquet_file_path, 
    ingestion_type=ingestion_type,
    merge_key=merge_key,
    driver_type=driver_type
))

    ################################################################ FLOW #################################################
    # Task parameters

    tag_env = Variable.get("airflow_tag_env")

    if tag_env == "prod":
        # Initialize MinIO client
        minio_client = Minio(
            Variable.get("minio_endpoint"),  # Replace with your MinIO endpoint
            access_key=Variable.get("access_key_minio"),  # Replace with your MinIO access key
            secret_key=Variable.get("secret_key_minio"),  # Replace with your MinIO secret key
        )
        bucket_name = Variable.get("bucket_name_minio")


        #source
        username_source = Variable.get("user_Project_133_110")
        password_source = Variable.get("password_Project_133_110")
        server_source = Variable.get("IP_Project_133_110")
        database_source = 'LAS'

        #target
        username_target = Variable.get("user_Project_65_167")
        password_target = Variable.get("password_Project_65_167")
        server_target = Variable.get("IP_Project_65_167")
        database_target = "databse"

        #parameter
        total_rows = 1000000
        concurrency_limit = 2
        concurrency_limit_fetch_priority = 4

        dbt_model_name = f"LAS_T_PENJUALAN_prod"

    elif tag_env == "dev":

        # Initialize MinIO client
        minio_client = Minio(
            Variable.get("minio_endpoint"),  # Replace with your MinIO endpoint
            access_key=Variable.get("access_key_minio"),  # Replace with your MinIO access key
            secret_key=Variable.get("secret_key_minio"),  # Replace with your MinIO secret key
            
        )
        bucket_name = Variable.get("bucket_name_minio")
        
        #source
        username_source = Variable.get("user_Project_133_110")
        password_source = Variable.get("password_Project_133_110")
        server_source = Variable.get("IP_Project_133_110")
        database_source = 'LAS'

        #target
        username_target = Variable.get("user_Project_65_167")
        password_target = Variable.get("password_Project_65_167")
        server_target = Variable.get("IP_Project_65_167")
        database_target = "databse"

        #parameter
        total_rows = 100000
        concurrency_limit = 1
        concurrency_limit_fetch_priority = 2

        dbt_model_name = f"LAS_T_PENJUALAN_dev"

    # # Fetch Data Tasks
    task_fetch_LAS_T_RUGI_LABA = fetch_data.override(task_id="fetch_data_task_LAS_T_RUGI_LABA")(
        query="""SELECT * FROM LAS.dbo.LAS_T_RUGI_LABA""", #change this
        parquet_file_path='/opt/airflow/data-lake/LAS_T_RUGI_LABA.parquet', #change this
        username=username_source,
        password=password_source,
        server=server_source,
        database=database_source,
        order_by="ID_RUGI_LABA", #change this
        total_rows=total_rows,
        concurrency_limit=concurrency_limit,
        minio_client = minio_client,
        bucket_name = bucket_name,
        table_minio = "LAS_T_RUGI_LABA" #change this
    )

    task_fetch_LAS_T_NERACA = fetch_data.override(task_id="fetch_data_task_LAS_T_NERACA")(
        query="""SELECT * FROM LAS.dbo.LAS_T_NERACA""",
        parquet_file_path='/opt/airflow/data-lake/LAS_T_NERACA.parquet',
        username=username_source,
        password=password_source,
        server=server_source,
        database=database_source,
        order_by="ID_NERACA",
        total_rows=total_rows,
        concurrency_limit=concurrency_limit,
        minio_client = minio_client,
        bucket_name = bucket_name,
        table_minio = "LAS_T_NERACA" #change this
    )

    task_fetch_LAS_T_NERACA_MENENGAH = fetch_data.override(task_id="fetch_data_task_LAS_T_NERACA_MENENGAH")(
        query="""SELECT * FROM LAS.dbo.LAS_T_NERACA_MENENGAH""",
        parquet_file_path='/opt/airflow/data-lake/LAS_T_NERACA_MENENGAH.parquet',
        username=username_source,
        password=password_source,
        server=server_source,
        database=database_source,
        order_by="ID_NERACA",
        total_rows=total_rows,
        concurrency_limit=concurrency_limit,
        minio_client = minio_client,
        bucket_name = bucket_name,
        table_minio = "LAS_T_NERACA_MENENGAH" #change this
    )

    task_fetch_LAS_T_APLIKASI = fetch_data.override(task_id="fetch_data_task_LAS_T_APLIKASI")(
        query="""SELECT * FROM LAS.dbo.LAS_T_APLIKASI""",
        parquet_file_path='/opt/airflow/data-lake/LAS_T_APLIKASI.parquet',
        username=username_source,
        password=password_source,
        server=server_source,
        database=database_source,
        order_by="ID_APLIKASI",
        total_rows=total_rows,
        concurrency_limit=concurrency_limit_fetch_priority,
        minio_client = minio_client,
        bucket_name = bucket_name,
        table_minio = "LAS_T_APLIKASI" #change this
    )

    task_fetch_LAS_T_KREDIT = fetch_data.override(task_id="fetch_data_task_LAS_T_KREDIT")(
        query="""SELECT * FROM LAS.dbo.LAS_T_KREDIT""",
        parquet_file_path='/opt/airflow/data-lake/LAS_T_KREDIT.parquet',
        username=username_source,
        password=password_source,
        server=server_source,
        database=database_source,
        order_by="ID_KREDIT",
        total_rows=total_rows,
        concurrency_limit=concurrency_limit_fetch_priority,
        minio_client = minio_client,
        bucket_name = bucket_name,
        table_minio = "LAS_T_KREDIT" #change this
    )

    task_fetch_LAS_T_CIF = fetch_data.override(task_id="fetch_data_task_LAS_T_CIF")(
        query="""SELECT * FROM LAS.dbo.LAS_T_CIF""",
        parquet_file_path='/opt/airflow/data-lake/LAS_T_CIF.parquet',
        username=username_source,
        password=password_source,
        server=server_source,
        database=database_source,
        order_by="CIF_LAS",
        total_rows=total_rows,
        concurrency_limit=concurrency_limit,
        minio_client = minio_client,
        bucket_name = bucket_name,
        table_minio = "LAS_T_CIF" #change this
    )


    # Transformation Task
    task_transform_data = transform_data(
        dbt_model_name = dbt_model_name,
        minio_client = minio_client,
        bucket_name = bucket_name
        )

    # Load Data Task
    task_load_data = load_data(
        parquet_file_path=f'/opt/airflow/data-lake/{dbt_model_name}.parquet',
        target_table='LAS_T_PENJUALAN',
        database=database_target,
        username=username_target,
        password=password_target,
        server=server_target,
        ingestion_type="truncate/insert",
        merge_key=None,
        driver_type="jtds"
    )


    # Define task dependencies
    [
        task_fetch_LAS_T_RUGI_LABA, 
        task_fetch_LAS_T_NERACA
        ] >> task_fetch_LAS_T_APLIKASI >> task_fetch_LAS_T_KREDIT >> [
        task_fetch_LAS_T_NERACA_MENENGAH,
        task_fetch_LAS_T_CIF
        ] >>  task_transform_data  >> task_load_data
