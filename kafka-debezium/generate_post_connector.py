import json
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Build connector JSON
connector_config = {
    "name": "banking-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv('POSTGRES_HOST'),
        "database.port": os.getenv('POSTGRES_PORT'),
        "database.user": os.getenv('POSTGRES_USER'),
        "database.password": os.getenv('POSTGRES_PASSWORD'),
        "database.dbname": os.getenv('POSTGRES_DB'),
        "topic.prefix": os.getenv('KAFKA_TOPIC_PREFIX'),
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": os.getenv('POSTGRES_SLOT_NAME'),
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}

# Send request to Debezium Connect
url = os.getenv('DEBEZIUM_CONNECT_URL') + "/connectors"
headers = {"Content-Type": "application/json"}

response = requests.post(url, headers=headers, data=json.dumps(connector_config))

# Debug/Output
if response.status_code == 201:
    print("Connector created successfully!")
elif response.status_code == 409:
    print("Connector already exists.")
else:
    print(f"Failed to create connector: {response.status_code} - {response.text}")