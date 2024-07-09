# redis-cli -h 192.168.80.74 -p 6379 -a yourpassword ping

import redis

# Connect to the Redis server
client = redis.StrictRedis(host='192.168.80.74', port=6379, password='yourpassword', db=0)

def receive_from_queue(queue_name):
    while True:
        message = client.brpop(queue_name)
        if message:
            print(f"Received: {message[1].decode('utf-8')}")
        else:
            print("No more messages in the queue")

# Example usage
queue_name = 'my_queue'
receive_from_queue(queue_name)
