# airflow_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import run_pipeline

default_args = {
    'owner': 'Name of the amazing data eng :)',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Schedule at 1 AM daily
dag = DAG(
    'wind_data_pipeline',
    default_args=default_args,
    description='Daily wind turbine data processing pipeline',
    schedule_interval='0 1 * * *',
    catchup=False,
)

run_pipeline_task = PythonOperator(
    task_id='run_pipeline_task',
    python_callable=run_pipeline,
    dag=dag
)

run_pipeline_task