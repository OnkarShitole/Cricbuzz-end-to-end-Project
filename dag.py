# import all modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # datetime(2025, 9, 3),
    'depends_on_past': False,
    'email': ['gcp.lab.practice.7@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# define the DAG

with DAG(
    "cricket_stats",
    default_args=default_args,
    description='Runs an external Python script',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
# define the tasks
    
    run_script_task = BashOperator(
        task_id='run_script',
        bash_command='python /home/airflow/gcs/dags/scripts/extract_and_push_gcs.py',
    )

# define the dependency
run_script_task