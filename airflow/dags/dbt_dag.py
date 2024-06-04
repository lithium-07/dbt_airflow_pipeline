from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# Determine the base path for the project
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# Define the path to the dbt project relative to the DAG file
DBT_PROJECT_PATH = os.path.join(BASE_PATH, '../../dbt_pipeline')

default_args = {
    'owner': 'lithium',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_incremental_update',
    default_args=default_args,
    description='Run dbt incremental updates every 5 minutes',
    schedule_interval=timedelta(minutes=5),
    start_date=days_ago(1),
    catchup=False,
)

run_dbt_incremental = BashOperator(
    task_id='run_dbt_incremental',
    bash_command=f'cd {DBT_PROJECT_PATH} && dbt run',
    dag=dag,
)

run_dbt_incremental
