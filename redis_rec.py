import redis

# Connect to the local Redis server
client = redis.StrictRedis(host='localhost', port=6379, db=0)

def send_to_queue(queue_name, message):
    client.lpush(queue_name, message)
    print(f"Sent: {message}")

# Example usage
queue_name = 'my_queue'
messages = ['message 1', 'message 2', 'message 3']

for msg in messages:
    send_to_queue(queue_name, msg)
