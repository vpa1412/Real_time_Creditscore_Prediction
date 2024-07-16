import os
import subprocess



def startCassandra():
    try:
        os.chdir("/home/caodien/")
        subprocess.run("./cassandra")

        os.chdir("/home/caodien/")
        subprocess.run("./")

        print("Cassandra: ON")
    except:
        print("error")
