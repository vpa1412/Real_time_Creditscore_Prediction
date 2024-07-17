from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from redis import Redis
from rq import Queue
from task_spark import submitSpark,startSpark
from task_hadoop import startHadoop

def submit_Spark():
    r = Redis(host='192.168.80.58', port=6379)
    q = Queue('spark', connection=r)
    q.enqueue(submitSpark)

def start_Spark():
    r = Redis(host='192.168.80.58', port=6379)
    q = Queue('spark', connection=r)
    q.enqueue(startSpark)

def start_Hadoop():
    r = Redis(host='192.168.80.58', port=6379)
    q = Queue('hadoop', connection=r)
    q.enqueue(startHadoop)
def start_Steaming():
    r = Redis(host='192.168.80.58', port=6379)
    q = Queue('stream', connection=r)
    q.enqueue(startHadoop)
def start_Cassandra():
    r = Redis(host='192.168.80.58', port=6379)
    q = Queue('cassandra', connection=r)
    q.enqueue(startHadoop)
def decide_task_to_run():
    run_task_type = Variable.get("run_task_type", default_var="submit_spark")
    if run_task_type == "start_spark":
        return "enqueue_start_spark_task"
    elif run_task_type == "start_hadoop":
        return "enqueue_start_hadoop_task"
    else:
        return "enqueue_submit_spark_task"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_tasks',
    default_args=default_args,
    description='A simple DAG to run tasks based on Airflow Variable',
    schedule_interval='1 1 * * *',
    catchup=False,
)

decide_task = BranchPythonOperator(
    task_id='decide_task',
    python_callable=decide_task_to_run,
    dag=dag,
)

submit_spark_task = PythonOperator(
    task_id='submit_Spark',
    python_callable=submit_Spark,
    dag=dag,
)

start_spark_task = PythonOperator(
    task_id='start_Spark',
    python_callable=start_Spark,
    dag=dag,
)

start_hadoop_task = PythonOperator(
    task_id='start_Hadoop',
    python_callable=start_Hadoop,
    dag=dag,
)

# end_task = DummyOperator(
#     task_id='end',
#     dag=dag
# )
#
# decide_task >> [submit_spark_task, start_spark_task, start_hadoop_task]
# submit_spark_task >> end_task
# start_spark_task >> end_task
# start_hadoop_task >> end_task
