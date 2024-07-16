import os
import subprocess
import ex


def startCassandra():
    try:
        os.chdir("/home/caodien/")
        subprocess.run("./cassandra")

        os.chdir("/home/caodien/")
        subprocess.run("./")

        print("Cassandra: ON")
    except Exception as e:
        print(f"Error in start Cassandra: {e}")
