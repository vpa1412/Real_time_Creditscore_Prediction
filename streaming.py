from pyspark.ml.linalg import DenseVector
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType

# Define schema for incoming test data
schema = StructType([
    StructField("field1", FloatType(), True),
    StructField("field2", FloatType(), True),
    # ... add all other fields ...
    StructField("label", IntegerType(), True)
])

# Build the SparkSession
spark = SparkSession.builder \
    .appName("KafkaTestStream") \
    .getOrCreate()

# Load the model from HDFS
model = RandomForestClassificationModel.load('hdfs://192.168.80.84:9000/kt/model')

# Subscribe to the Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test_topic") \
    .load()

# Convert the value column to String and parse JSON
df = df.selectExpr("CAST(value AS STRING)")
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Prepare the data for prediction
input_data = df.rdd.map(lambda x: (DenseVector(x[:-1]), x[-1]))
df1 = spark.createDataFrame(input_data, ["X", "Y"])

# Make predictions on streaming data
predictions = model.transform(df1)

# Write the predictions to Cassandra
predictions.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "your_keyspace") \
    .option("table", "predictions") \
    .start() \
    .awaitTermination()
