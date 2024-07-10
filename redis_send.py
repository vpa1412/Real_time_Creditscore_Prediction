import subprocess
def start_client():
   subprocess.run(["docker", "run",  "--name", "some-mongo", "-d", "mongo:latest"])