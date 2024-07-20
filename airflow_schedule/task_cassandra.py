import os
import subprocess
import time

def startCassandra():
    try:
        os.chdir("/home/caodien/CS411/apache/apache-cassandra-4.1.5/bin/")
        subprocess.run("./cassandra")

        time.sleep(3)
        subprocess.run("./cqlsh")

        print("Cassandra: ON")
    except Exception as e:
        print(f"Error in start Hadoop: {e}")