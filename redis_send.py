import redis

# Connect to the Redis server
client = redis.StrictRedis(host='192.168.80.74', port=6379, password='yourpassword', db=0)

def send_to_queue(queue_name, message):
    client.lpush(queue_name, message)
    print(f"Sent: {message}")

# Example usage
queue_name = 'my_queue'
messages = ['message 1', 'message 2', 'message 3']

for msg in messages:
    send_to_queue(queue_name, msg)
