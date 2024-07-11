from redis import Redis
from rq import Queue
from jobs import start_client, check_hadoop

redis = Redis("192.168.80.78", 9000)
queue = Queue("tinh", connection=redis)

queue.enqueue(start_client)
