import os
import subprocess

def startHadoop():
    try:
        os.chdir("/home/ktinh/Documents/")
        subprocess.run("./start-all.sh")
        print("hadoop on")
    except Exception as e:
        print(f"Error in start Hadoop: {e}")