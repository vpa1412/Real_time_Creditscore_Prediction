import redis

# Connect to the Redis server
r = redis.Redis(host='192.168.80.74', port=6379, password='12345')

# Publish the command to the channel
r.publish('start_hadoop', 'sh /path/to/hadoop/start_script.sh')
