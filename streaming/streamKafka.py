import csv
import json
import time
import random
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers="192.168.80.83:9092",
                        value_serializer=lambda m: json.dumps(m).encode('utf-8'))


with open('testing.csv', 'r') as f:
   reader = csv.DictReader(f)
   for row in reader:
       try:
           producer.send("credit_testing", row)
           time.sleep(random.randint(7, 20))
       except Exception as e:
           print(f"Error: {e}")
