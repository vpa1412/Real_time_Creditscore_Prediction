from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from redis import Redis
from rq import Queue
from submit_spark import submitSpark,startSpark


def submit_Spark():
    # Replace these values with your actual Redis configuration
    r = Redis(host='192.168.80.58', port=6379)
    q = Queue('spark', connection=r)
    q.enqueue(submitSpark)
def start_Spark():
    # Replace these values with your actual Redis configuration
    r = Redis(host='192.168.80.58', port=6379)
    q = Queue('spark', connection=r)
    q.enqueue(startSpark)

# Define default_args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'run_hadoop_tasks',
    default_args=default_args,
    description='A simple DAG to start Hadoop every day at 1:01 AM',
    schedule="1 1 * * *",  # Cron expression for every day at 1:01 AM
    catchup=False,
)

run_hadoop_task = PythonOperator(
    task_id='enqueue_hadoop_task',
    python_callable=start_Spark,
    dag=dag,
)
