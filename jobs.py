import subprocess
import os

def start_client0():
    print("Lam gi do")


def start_client():
    try:
        os.chdir("/home/ktinh/Documents/ah/hadoop-3.4.0")
        subprocess.run(['./sbin/start-all.sh'])
        # subprocess.run(["hadoop", "fs", "-put", local_file_path, hadoop_dest_path], check=True)
        # print(f"File {local_file_path} successfully copied to {hadoop_dest_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error copying file to Hadoop: {e}")

def check_hadoop():
    os.chdir("/home/ktinh/Documents/ah/hadoop-3.4.0")
    subprocess.run(['pwd'])
    subprocess.run(["./bin/hdfs", "dfs", "-ls", "/"])


