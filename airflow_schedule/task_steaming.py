import os
import subprocess

def startStreaming():
    try:
        os.chdir("/home/labsoe/Documents/steam")
        subprocess.run(["python3","filepy"])

        print("Streaming")
    except Exception as e:
        print(f"Error in start Hadoop: {e}")