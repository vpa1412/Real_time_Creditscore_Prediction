# final_bigdata

# start airflow:
- docker compose up

# start redis server:
- docker start redis-container
- host: redis://192.168.80.58:6379/ 

# worker:
- rq worker -u redis://192.168.80.58:6379 name
