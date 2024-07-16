import os
import subprocess

def startHadoop():
    try:
        os.chdir("/home/ktinh/Documents/")
        subprocess.run("./start-all.sh")
        print("hadoop on")
    except:
        print("error")
def submitSpark():
    try:
        os.chdir("/home/ktinh/Documents/ah/spark-3.5.1-bin-hadoop3-scala2.13/bin/")
        subprocess.run(["./bin/spark-submit "
                       ,"--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,com.datastax.spark:spark-cassandra-connector_2.13:3.5.1"
                       ,"/home/ktinh/PycharmProjects/final_bigdata/predict_credit.py"])
    except:
        print("error")