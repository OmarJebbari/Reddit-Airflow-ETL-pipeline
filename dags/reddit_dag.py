from airflow import DAG
from datetime import datetime, timedelta
import os
import sys
import logging
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reddit_pipeline import reddit_pipeline
from etls.quality_checks import validate_bronze_parquet
from etls.reddit_etl import load_latest_bronze_parquet_to_postgres


def task_failure_callback(context):
    task_instance = context.get("task_instance")
    logging.error(
        "Task failure | dag_id=%s task_id=%s run_id=%s ts=%s",
        context.get("dag").dag_id if context.get("dag") else "unknown",
        task_instance.task_id if task_instance else "unknown",
        context.get("run_id", "unknown"),
        context.get("ts", "unknown"),
    )

default_args = {
    'owner': 'Omar Jebbari',
    'start_date': datetime(2026, 4, 5),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_callback,
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='elt_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    tags=['reddit', 'elt', 'pipeline']
)

# Extraction from reddit
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    dag=dag,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': ['dataengineering', 'datascience', 'aws', 'azure', 'python'],
        'time_filter': 'year',
        'limit': 100,
        'max_posts': 1000,
    }
)

validate_bronze = PythonOperator(
    task_id='validate_bronze_parquet',
    python_callable=validate_bronze_parquet,
    dag=dag,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
    }
)

dbt_run = BashOperator(
    task_id='dbt_run_gold',
    bash_command='dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt',
    dag=dag,
)

load_gold_to_postgres = PythonOperator(
    task_id='load_gold_to_postgres',
    python_callable=load_latest_bronze_parquet_to_postgres,
    dag=dag,
    op_kwargs={
        'table_name': 'reddit_gold',
    }
)

extract >> validate_bronze >> dbt_run >> load_gold_to_postgres
