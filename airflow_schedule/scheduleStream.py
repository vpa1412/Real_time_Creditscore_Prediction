from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from redis import Redis
from rq import Queue

from task_streaming import startStreaming
from task_spark import submitSpark


def streaming():
    r = Redis(host='192.168.80.58', port=6379)
    q = Queue('stream', connection=r)
    q.enqueue(startStreaming)

def submit():
    r = Redis(host='192.168.80.58', port=6379)
    q = Queue('spark', connection=r)
    q.enqueue(submitSpark)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'streaming_submit',
    default_args=default_args,
    description='streaming kafka and submit spark model',
    schedule_interval='0 1 * * *',  # Run once a day at 1:00 AM
)

# Define tasks
streaming_task = PythonOperator(
    task_id='streaming',
    python_callable=streaming,
    dag=dag,
)
submit_task = PythonOperator(
    task_id='submit',
    python_callable=submit,
    dag=dag,
)
