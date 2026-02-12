## Banking Modern Data Stack

End-to-end event-driven banking pipeline using PostgreSQL, Debezium, Kafka, MinIO, Snowflake, Airflow, and dbt.

### Project Status

Completed implementation:
- Transactional source tables in PostgreSQL
- CDC from PostgreSQL to Kafka via Debezium
- Kafka consumer writing parquet files to MinIO
- Airflow DAG loading MinIO parquet to Snowflake `raw` schema
- dbt staging, snapshots (SCD2), and marts orchestrated by Airflow

## Architecture

1. `postgres` stores source OLTP tables (`customers`, `accounts`, `transactions`).
2. Debezium connector streams row-level changes to Kafka topics.
3. `consumer/kafka_to_minio.py` consumes Kafka events and stores parquet files in MinIO.
4. Airflow DAG `minio_to_snowflake` loads parquet files from MinIO to Snowflake `raw` tables.
5. Airflow DAG `SCD2_snapshots` runs:
   - `dbt run --select models/staging`
   - `dbt snapshot`
   - `dbt run --select marts`

![Architecture Overview](.docs/bankingDataPipeline.png)

## Repository Structure

- `docker-compose.yml`: full local stack orchestration
- `docker/dags/minio_to_snowflake_dag.py`: MinIO -> Snowflake loader DAG
- `docker/dags/replication.py`: dbt staging/snapshot/marts DAG
- `banking_dbt/`: dbt project (sources, staging, marts, snapshots)
- `postgres/01_create_tables.sql`: source schema
- `data-generator/faker_generator.py`: synthetic source data generator
- `kafka-debezium/generate_post_connector.py`: Debezium connector bootstrap
- `consumer/kafka_to_minio.py`: Kafka -> MinIO parquet consumer

## Prerequisites

- Docker + Docker Compose
- Python 3.10+ (for local utility scripts)
- Snowflake account and role with:
  - read access to `raw` schema objects
  - write access to dbt target schema (for staging/marts/snapshots)

## Configuration

Create and fill these files with your own credentials.

### 1) Root `.env` (used by Docker Compose services)

Required keys:
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `AIRFLOW_DB_USER`
- `AIRFLOW_DB_PASSWORD`
- `AIRFLOW_DB_NAME`

### 2) `docker/dags/.env` (used by Airflow DAG runtime)

Required keys:
- MinIO:
  - `MINIO_ENDPOINT_URL`
  - `MINIO_ACCESS_KEY`
  - `MINIO_SECRET_KEY`
  - `MINIO_BUCKET_NAME`
  - `MINIO_LOCAL_DIR`
- Snowflake:
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_PASSWORD`
  - `SNOWFLAKE_ACCOUNT`
  - `SNOWFLAKE_WAREHOUSE`
  - `SNOWFLAKE_DATABASE`
  - `SNOWFLAKE_SCHEMA` (for raw load DAG)
  - `DBT_TARGET_SCHEMA` (for dbt models/snapshots)
  - `SNOWFLAKE_ROLE`

## Quick Start

### 1) Start infrastructure

```bash
docker compose up -d
```

Airflow UI: `http://localhost:8080`  
Default credentials (from compose init): `admin / admin`

MinIO Console: `http://localhost:9001`

### 2) Initialize source tables

```bash
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < postgres/01_create_tables.sql
```

If needed, use explicit values instead of env vars (for example `-U banking_user -d banking_db`).

### 3) Create Debezium connector

```bash
python kafka-debezium/generate_post_connector.py
```

### 4) Generate source data

```bash
python data-generator/faker_generator.py
```

Run once:

```bash
python data-generator/faker_generator.py --once
```

### 5) Start Kafka -> MinIO consumer

```bash
python consumer/kafka_to_minio.py
```

### 6) Run Airflow DAGs

- `minio_to_snowflake` (every 15 minutes) loads `raw` tables.
- `SCD2_snapshots` (daily) builds staging, snapshots, and marts.

You can trigger both manually from the Airflow UI for first-time bootstrap.

## dbt Notes

- dbt project: `banking_dbt/`
- profile name in `dbt_project.yml`: `banking_dbt`
- local profile file used in Airflow: `banking_dbt/.dbt/profiles.yml`
- dbt target schema should be controlled via `DBT_TARGET_SCHEMA` to keep `raw` ingestion separate from transformed layers.

## Common Troubleshooting

- `Could not find profile named 'banking_dbt'`
  - Ensure `banking_dbt/.dbt/profiles.yml` exists and is mounted to `/home/airflow/.dbt`.
- Snowflake auth errors
  - Verify values in `docker/dags/.env`.
  - Recreate Airflow services after env changes:
    ```bash
    docker compose up -d --force-recreate airflow-init airflow-scheduler airflow-webserver
    ```
- `Insufficient privileges to operate on schema`
  - Ensure the role used by dbt has write permissions on `DBT_TARGET_SCHEMA`.
  - Keep MinIO load schema (`SNOWFLAKE_SCHEMA`) and dbt target schema (`DBT_TARGET_SCHEMA`) distinct if permissions differ.

## Security

- Do not commit real credentials.
- Rotate credentials immediately if secrets were ever exposed in logs, screenshots, or committed history.
