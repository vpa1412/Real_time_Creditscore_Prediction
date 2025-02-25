# <div align="center">Real-time Credit Score Prediction</div>

# Requirement
- [Java-11](https://www.oracle.com/java/technologies/downloads/#java11)
- [Hadoop-3.4.0](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Spark-3.5.1](https://spark.apache.org/downloads.html)
- [Cassandra-4.1.5](https://cassandra.apache.org/doc/stable/cassandra/getting_started/installing.html)
- [Kafka-2.1.3-3.7.0](https://kafka.apache.org/quickstart)
- [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Redis](https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/docker/)
- Install requirements
````
pip install -r requirements.txt
````
# Usage
###  Airflow:
#### start
```
docker compose up
```
##### stop
```
docker compose down
```
### Redis server:
Example install:
```
docker run -d --name redisServer -p 192.168.80.91:6379:6379 redis
```
#### start
```
docker start redisServer
```
#### stop
```
docker stop redisServer
```
### Worker:
```
cd /home/user/path/to/folder/task.py
```
#### Hadoop
```
rq worker -u redis://192.168.80.91:6379 hadoop
```
#### Spark 
```
rq worker -u redis://192.168.80.91:6379 spark
```
#### Stream-kafka
```
rq worker -u redis://192.168.80.91:6379 stream
```
Example: put ``task_hadoop.py`` in /home/user/orschestrator/
````
cd /home/user/orschestrator/
rq worker -u redis://192.168.80.91:6379 hadoop
````
## Train Model
### submit spark to processing data
```
./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 /home/user/path/to/processingData.py
```
### submit spark to create model after train
```
./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 /home/user/path/to/trainModel.py
```
## Notes
- Ensure all paths are correctly set according to your project structure.
- Verify that all services (Hadoop, Spark, Cassandra, Kafka, Airflow, Redis) are running and properly configured before starting the workers and submitting jobs.
- Adjust the IP addresses, ports, and paths according to your specific setup.
