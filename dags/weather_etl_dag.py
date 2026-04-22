from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys
sys.path.insert(0, "/opt/airflow/plugins")

from extract import extract_from_kafka
from transform import transform_weather
from load import load_to_postgres

# ─── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner":            "weather_pipeline",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ─── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="weather_etl_pipeline",
    description="Kafka → Transform → PostgreSQL weather ETL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/15 * * * *",   # every 15 min — matches producer interval
    catchup=False,                       # don't backfill missed runs
    max_active_runs=1,                   # only 1 run at a time
    tags=["weather", "kafka", "etl"],
) as dag:

    # ── Task 1: Start marker ──────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Task 2: Extract — read messages from Kafka ────────────────
    extract = PythonOperator(
        task_id="extract_from_kafka",
        python_callable=extract_from_kafka,
        op_kwargs={
            "topic":   "weather.raw",
            "servers": "kafka:29092",    # internal Docker network address
            "group":   "weather_etl_group",
            "timeout": 30,               # seconds to poll Kafka
        },
    )

    # ── Task 3: Transform — clean, validate, enrich ───────────────
    transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    # ── Task 4: Load — upsert into PostgreSQL ─────────────────────
    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        op_kwargs={
            "host":     "postgres",      # Docker service name
            "port":     5432,            # internal port (not 5433)
            "dbname":   "weather_db",
            "user":     "airflow",
            "password": "airflow",
        },
    )

    # ── Task 5: End marker ────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── Pipeline: linear dependency chain ────────────────────────
    start >> extract >> transform >> load >> end