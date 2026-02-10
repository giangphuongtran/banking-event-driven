import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka configuration
consumer = KafkaConsumer(
    "banking_server.public.transactions",
    "banking_server.public.accounts",
    "banking_server.public.customers",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=os.getenv("KAFKA_GROUP_ID"),
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# Minio configuration
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET_NAME")

# Create bucket if it doesn't exist
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)
    
# Consume messages from Kafka and upload to Minio
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_path = f"{table_name}_{date_str}.parquet"
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f"{table_name}/date={date_str}/{table_name}_{datetime.now().strftime('%H%M%S%f')}.parquet"
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f"Uploaded {len(records)} records to s3://{bucket}/{s3_key}")
    
# Batch consume
batch_size = 50
buffer = {
    "banking_server.public.transactions": [],
    "banking_server.public.accounts": [],
    "banking_server.public.customers": []
}

print("Connected to Kafka, waiting for messages...")

for message in consumer:
    topic = message.topic
    value = message.value
    payload = value.get("payload", {})
    record = payload.get("after")
    
    if record:
        buffer[topic].append(record)
        print(f"[{topic}] -> {record}")
    
    if len(buffer[topic]) >= batch_size:
        write_to_minio(topic.split('.')[-1], buffer[topic])
        buffer[topic] = []