import os
import subprocess

def startSteam():
    try:
        os.chdir("/home/labseo/Documents/stream")
        subprocess.run(["python3","credit.py"])
        print("Streaming data: Done")
    except Exception as e:
        print(f"Error in Streaming: {e}")