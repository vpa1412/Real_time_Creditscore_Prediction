from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def message():
    print("First DAG executed Successfully!!")

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="FirstDAG",
    default_args=default_args,
    description="A simple DAG to print a message every minute",
    schedule="*/1 * * * *",
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="task",
        python_callable=message
    )
