from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from spark.jobs.extract_transform_load import extract_transform_load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'toys_and_models_etl',
    default_args=default_args,
    description='ETL process for Toys and Models',
    schedule_interval=timedelta(days=1),
)

etl_task = PythonOperator(
    task_id='extract_transform_load',
    python_callable=extract_transform_load,
    dag=dag,
)

etl_task