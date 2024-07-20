from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from redis import Redis
from rq import Queue

from task_spark import submitDeltable

# host redis server
host_redis = "192.168.80.91"
port_redis = 6379
def submit_Deltable():
    r = Redis(host=host_redis, port=port_redis)
    q = Queue('spark', connection=r)
    q.enqueue(submitDeltable)

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
    'Show Predict Table',
    default_args=default_args,
    description='Just show table result',
)

submit_Deltable_task = PythonOperator(
    task_id='submit',
    python_callable=submit_Deltable,
    dag=dag,
)
