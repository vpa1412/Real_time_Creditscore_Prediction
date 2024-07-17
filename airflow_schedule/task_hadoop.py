import os
import subprocess

def startHadoop():
    try:
        os.chdir("/home/ktinh/Documents/ah/hadoop-3.4.0/sbin/")
        subprocess.run(["./start-all.sh"])
        print("Start Hadoop: ON")
    except Exception as e:
        print(f"Error in Streaming: {e}")