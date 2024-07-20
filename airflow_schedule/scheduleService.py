from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from redis import Redis
from rq import Queue

from task_hadoop import startHadoop
from task_spark import startSpark
from task_streaming import startKafka
from task_cassandra import startCassandra

# host redis server
host_redis = "192.168.80.91"
port_redis = 6379
def start_Hadoop():
    r = Redis(host=host_redis, port=port_redis)
    q = Queue('hadoop', connection=r)
    q.enqueue(startHadoop)
def start_Kafka():
    r = Redis(host=host_redis, port=port_redis)
    q = Queue('steam', connection=r)
    q.enqueue(startKafka)
def start_Spark():
    r = Redis(host=host_redis, port=port_redis)
    q = Queue('spark', connection=r)
    q.enqueue(startSpark)
def start_Cassandra():
    r = Redis(host=host_redis, port=port_redis)
    q = Queue('cassandra', connection=r)
    q.enqueue(startSpark)

# start 1 time
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
    'startService',
    default_args=default_args,
    description='start_hadoop_spark_kafka',
    schedule_interval=None, 
)

# Define tasks
startSpark_task = PythonOperator(
    task_id='startSpark',
    python_callable=start_Spark,
    dag=dag,
)

startHadoop_task = PythonOperator(
    task_id='startHadoop',
    python_callable=start_Hadoop,
    dag=dag,
)

startKafka_task = PythonOperator(
    task_id='startKafka',
    python_callable=start_Kafka,
    dag=dag,
)
startCassandra_task = PythonOperator(
    task_id='startCassandra',
    python_callable=start_Cassandra,
    dag=dag,
)

