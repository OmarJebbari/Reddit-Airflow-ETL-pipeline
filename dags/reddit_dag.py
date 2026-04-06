from airflow import DAG
from datetime import datetime 
import os
import sys
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reddit_pipeline import reddit_pipeline
from etls.quality_checks import validate_bronze_parquet
from etls.reddit_etl import load_latest_bronze_parquet_to_postgres

default_args = {
    'owner': 'Omar Jebbari',
    'start_date': datetime(2026, 4, 5),
    
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='elt_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
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
