import os
import subprocess
import time

def startKafka():
    try:
        os.chdir("/home/user/path/to/kafka_2.13-3.7.0")

        # Start Zookeeper
        zookeeper_cmd = ["bin/zookeeper-server-start.sh", "config/zookeeper.properties"]
        zookeeper_process = subprocess.Popen(zookeeper_cmd)
        print("Zookeeper process started with PID:", zookeeper_process.pid)
        time.sleep(3)
        # Start Kafka
        kafka_cmd = ["bin/kafka-server-start.sh", "config/server.properties"]
        kafka_process = subprocess.Popen(kafka_cmd)
        print("Kafka process started with PID:", kafka_process.pid)

    except Exception as e:
        print(f"Error in starting Kafka: {e}")

def startStreaming():
    try:
        os.chdir("/home/user/to/path/credit_folder")

        # Start the streaming script
        stream_cmd = ["python3", "credit.py"]
        stream_process = subprocess.Popen(stream_cmd)
        print("Streaming process started with PID:", stream_process.pid)

    except Exception as e:
        print(f"Error in starting streaming: {e}")
