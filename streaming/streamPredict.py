from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, from_json, when

# Create Spark session
spark = SparkSession.builder \
    .appName("Prediction") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define schema
schema = StructType([
    StructField("ID", StringType()),
    StructField("Age", StringType()),  # Initially read as StringType
    StructField("Annual_Income", StringType()),  # Initially read as StringType
    StructField("Monthly_Inhand_Salary", StringType()),  # Initially read as StringType
    StructField("Num_Bank_Accounts", StringType()),  # Initially read as StringType
    StructField("Num_Credit_Card", StringType()),  # Initially read as StringType
    StructField("Interest_Rate", StringType()),  # Initially read as StringType
    StructField("Num_of_Loan", StringType()),  # Initially read as StringType
    StructField("Delay_from_due_date", StringType()),  # Initially read as StringType
    StructField("Num_of_Delayed_Payment", StringType()),  # Initially read as StringType
    StructField("Changed_Credit_Limit", StringType()),  # Initially read as StringType
    StructField("Num_Credit_Inquiries", StringType()),  # Initially read as StringType
    StructField("Outstanding_Debt", StringType()),  # Initially read as StringType
    StructField("Credit_Utilization_Ratio", StringType()),  # Initially read as StringType
    StructField("Total_EMI_per_month", StringType()),  # Initially read as StringType
    StructField("Amount_invested_monthly", StringType()),  # Initially read as StringType
    StructField("Monthly_Balance", StringType()),  # Initially read as StringType
    StructField("Occupation_Numeric", StringType()),  # Initially read as StringType
    StructField("Credit_History_Age_Numeric", StringType()),  # Initially read as StringType
    StructField("Payment_of_Min_Amount_Numeric", StringType()),  # Initially read as StringType
    StructField("Payment_Behaviour_Numeric", StringType())  # Initially read as StringType
])

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.80.83:9092") \
    .option("subscribe", "credit_testing") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", False) \
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("json")) \
    .select("json.*")

# Convert columns from StringType to desired data types
converted_df = kafka_df \
    .withColumn("Age", col("Age").cast(FloatType())) \
    .withColumn("Annual_Income", col("Annual_Income").cast(FloatType())) \
    .withColumn("Monthly_Inhand_Salary", col("Monthly_Inhand_Salary").cast(FloatType())) \
    .withColumn("Num_Bank_Accounts", col("Num_Bank_Accounts").cast(IntegerType())) \
    .withColumn("Num_Credit_Card", col("Num_Credit_Card").cast(IntegerType())) \
    .withColumn("Interest_Rate", col("Interest_Rate").cast(IntegerType())) \
    .withColumn("Num_of_Loan", col("Num_of_Loan").cast(FloatType())) \
    .withColumn("Delay_from_due_date", col("Delay_from_due_date").cast(IntegerType())) \
    .withColumn("Num_of_Delayed_Payment", col("Num_of_Delayed_Payment").cast(FloatType())) \
    .withColumn("Changed_Credit_Limit", col("Changed_Credit_Limit").cast(FloatType())) \
    .withColumn("Num_Credit_Inquiries", col("Num_Credit_Inquiries").cast(FloatType())) \
    .withColumn("Outstanding_Debt", col("Outstanding_Debt").cast(FloatType())) \
    .withColumn("Credit_Utilization_Ratio", col("Credit_Utilization_Ratio").cast(FloatType())) \
    .withColumn("Total_EMI_per_month", col("Total_EMI_per_month").cast(FloatType())) \
    .withColumn("Amount_invested_monthly", col("Amount_invested_monthly").cast(FloatType())) \
    .withColumn("Monthly_Balance", col("Monthly_Balance").cast(FloatType())) \
    .withColumn("Occupation_Numeric", col("Occupation_Numeric").cast(IntegerType())) \
    .withColumn("Credit_History_Age_Numeric", col("Credit_History_Age_Numeric").cast(IntegerType())) \
    .withColumn("Payment_of_Min_Amount_Numeric", col("Payment_of_Min_Amount_Numeric").cast(IntegerType())) \
    .withColumn("Payment_Behaviour_Numeric", col("Payment_Behaviour_Numeric").cast(IntegerType()))

# Replace null values with default values
converted_df = converted_df.na.fill(0)

converted_df.printSchema()

# Load model from HDFS
model = PipelineModel.load("hdfs://192.168.80.41:9000/kt/model_ok")

# Predict with the model
predictions = model.transform(converted_df)

# Display prediction results
output_df = predictions.select("*")

output_df.writeStream \
    .trigger(processingTime="10 seconds") \
    .format("console") \
    .option("checkpointLocation", "/home/ktinh/checkpoint") \
    .outputMode("update") \
    .start()

output_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/home/ktinh/checkpoint1") \
    .start("hdfs://192.168.80.41:9000/kt/deltable")

spark.streams.awaitAnyTermination()
spark.stop()