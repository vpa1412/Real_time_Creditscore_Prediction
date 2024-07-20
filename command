# readDeltable
deltable save on hadoop, read deltable
./bin/spark-submit --packages io.delta:delta-spark_2.13:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 /home/ktinh/PycharmProjects/final_bigdata/streaming/readDeltable.py


#streamPredict
submit this file to read stream and save deltable
./bin/spark-submit --packages io.delta:delta-spark_2.13:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 /home/ktinh/PycharmProjects/final_bigdata/streaming/streamPredict.py


Submit spark:
processingData.py: prrocess data and save processed.csv on hadoop
./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 /home/ktinh/PycharmProjects/final_bigdata/data_preparation/processingData.py

trainModel: save credit_model on hadoop
./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 /home/ktinh/PycharmProjects/final_bigdata/data_preparation/trainModel.py
