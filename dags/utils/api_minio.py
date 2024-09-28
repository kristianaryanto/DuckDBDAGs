import requests
from requests.auth import HTTPBasicAuth
import os
from airflow.models import Variable
from minio import Minio
from utils.fetch_data import



def api_get_file_csv_minio(filepath,filename):

    # Construct the full URL
    url = f"{base_url}{mcr_gl_casa_base_path}/download?filename={filename}.csv"

    print(filename)
    # Make a GET request to download the file
    response = requests.get(url, headers=headers, auth=HTTPBasicAuth(username, password))
    
    filename = f"validated_{filename}"
    file_path = f"{filepath}/{filename}.csv"
    print(file_path)
   
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # Check if the request was successful
    if response.status_code == 200:
        # Save the file locally
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"File '{file_path}' downloaded successfully.")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

    return filename




def api_upload_file_csv_minio(file_path,filename):
    files = {
        "file": (f"datalake/test/python_{filename}.csv", open(file_path, "rb"),"text/csv"),
    }
    data = {
        "userUpload": "54321",
        "userUploadName": "Awikwok",
        "isHeadquarters": "true",
        "costCenter": "PS85000",
        "branchCode": "229",
    }

    url = f"{base_url}{mcr_gl_casa_base_path}/upload"
    response = requests.post(url, files=files, data=data, auth=HTTPBasicAuth(username, password),headers=headers)

    if response.status_code == 200:
        print("File uploaded successfully.")
        print(response.json()['data']['fileName'])
    else:
        print(f"Failed to upload the file. Status code: {response.status_code}")

    return None


def upload_to_minio(minio_client, bucket_name, parquet_file_path, parquet_file_path_minio):
    """
    Upload a file to MinIO.
    
    :param minio_client: The initialized MinIO client.
    :param bucket_name: The name of the bucket to upload the file to.
    :param parquet_file_path: The local path of the parquet file.
    :param parquet_file_path_minio: The target path inside MinIO (including directories and filename).
    """
    minio_client.fput_object(bucket_name, parquet_file_path_minio, parquet_file_path)



def download_files_from_minio(pool,bucket_name, files_array, destination_folder, minio_client,fidbatch):
    """
    Downloads files from a MinIO bucket.

    Args:
        bucket_name (str): The name of the bucket to download files from.
        files_array (list): List of file names to download.
        destination_folder (str): Path to the local folder where files will be saved.
        minio_client (Minio): An initialized MinIO client instance.

    Returns:
        bool: True if all files were successfully downloaded, False otherwise.
    """
    try:
        for file_name in files_array:
            try:
                # Specify the local path to save the downloaded file
                local_file_path = f"{destination_folder}/{file_name}"
             
            set_values = {
                "batchtime": "now()",
            }
            
            where_params = (fidbatch)  # Assuming rows contain filenames
            where_clause = "fidbatch IN %s"
                await run_update_query(
            pool,
            table_name="batch",
            set_value=set_values,
            where_clause=where_clause,
            where_params=where_params,
            concurrency_limit=1
        )

                # Download the file from MinIO
                minio_client.fget_object(bucket_name, file_name, local_file_path)
                print(f"Downloaded {file_name} successfully to {local_file_path}")
            except S3Error as err:
                print(f"Error downloading {file_name}: {err}")
                return False
        return True  # All files downloaded successfully
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False