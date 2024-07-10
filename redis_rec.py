# redis-cli -h 192.168.80.74 -p 6379 -a yourpassword ping

import redis
import subprocess

# Connect to the Redis server
r = redis.Redis(host='192.168.80.74', port=6379, password='12345')

# Subscribe to a channel
p = r.pubsub()
p.subscribe('start_hadoop')

print('Listening for messages...')

for message in p.listen():
    if message['type'] == 'message':
        command = message['data'].decode('utf-8')
        print(f"Received command: {command}")

        # Execute the command
        subprocess.run(command, shell=True)

