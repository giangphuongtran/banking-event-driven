import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Minio configuration
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# Snowflake configuration
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["transactions", "accounts", "customers"]

def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    local_files = {}
    for table in TABLES:
        prefix = f"{table}/"
        response = s3.list_objects_v2(Bucket=MINIO_BUCKET_NAME, Prefix=prefix)
        local_files[table] = []
        for obj in response.get('Contents', []):
            key = obj['Key']
            local_path = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(MINIO_BUCKET_NAME, key, local_path)
            local_files[table].append(local_path)
            print(f"Downloaded {key} to {local_path}")
    return local_files

def load_to_snowflake(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_from_minio")
    if not local_files:
        print("No files found in MinIO to load.")
        return
    
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()
    
    for table, files in local_files.items():
        if not files:
            print(f"No files found for table {table} in MinIO.")
            continue
        
        for file in files:
            cur.execute(f"PUT file://{file} @%{table}")
            print(f"Uploaded {file} to Snowflake stage for table {table}")
            
        copy_sql = f"""
        COPY INTO {table}
        FROM @%{table}
        FILE_FORMAT = (TYPE = 'PARQUET')
        ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_sql)
        print(f"Data loaded into Snowflake table {table} from files: {files}")
        
    cur.close()
    conn.close()
    
# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='minio_to_snowflake',
    default_args=default_args,
    description='DAG to load parquet data from MinIO to Snowflake RAW tables',
    schedule_interval="*/15 * * * *",  # Every minute
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    
    task1 = PythonOperator(
        task_id='download_from_minio',
        python_callable=download_from_minio,
    )
    
    task2 = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True,
    )
    
    task1 >> task2