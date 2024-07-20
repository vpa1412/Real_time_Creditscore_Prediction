import os
import subprocess

def startSpark():
    try:
        os.chdir("/home/ktinh/Documents/ah/spark-3.5.1-bin-hadoop3-scala2.13/sbin")
        subprocess.run("./start-all.sh")
        print("Spark Server: ON")
    except Exception as e:
        print(f"Error in start Spark: {e}")
def submitSpark():
    try:
        os.chdir("/home/ktinh/Documents/ah/spark-3.5.1-bin-hadoop3-scala2.13/bin/")
        subprocess.run(["./spark-submit "
                       ,"--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1"
                       ,"/home/ktinh/PycharmProjects/final_bigdata/streamPredict.py"])
        print("submit Spark: done")
    except Exception as e:
        print(f"Error in submit Spark: {e}")