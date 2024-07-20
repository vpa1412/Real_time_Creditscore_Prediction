# final_bigdata

# Requirement
- [Java-11](https://www.oracle.com/java/technologies/downloads/#java11)
- [Hadoop-3.4.0](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Spark-3.5.1](https://spark.apache.org/downloads.html)
- [Cassandra-4.1.5](https://cassandra.apache.org/doc/stable/cassandra/getting_started/installing.html)
- [Kafka-2.1.3-3.7.0](https://kafka.apache.org/quickstart)
- [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Redis](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/docker/)
# Train Model
### submit spark to processing data
```

```
### submit spark to create model after train
#  Airflow:
### start
```
docker compose up
```
### stop
```
docker compose down
```
# Redis server:
Example install:
```
docker run -d --name redisServer -p 192.168.80.91:6379:6379 redis
```
### start
```
docker start redisServer
```
### stop
```
docker stop redisServer
```
# worker:
```
cd /home/user/path/to/folder/task.py
```
### Hadoop
```
rq worker -u redis://192.168.80.91:6379 hadoop
```
### Spark 
```
rq worker -u redis://192.168.80.91:6379 spark
```
### Stream-kafka
```
rq worker -u redis://192.168.80.91:6379 stream
```
### Cassandra
```
rq worker -u redis://192.168.80.91:6379 cassandra
```
Example: put ``task_hadoop.py`` in /home/user/orschestrator/
````
cd /home/user/orschestrator/
rq worker -u redis://192.168.80.91:6379 hadoop
````