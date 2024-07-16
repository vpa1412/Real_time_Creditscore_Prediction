from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, last, coalesce, udf
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, count, avg, min, max
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel



spark = SparkSession.builder.appName("Cassandra")\
  .config('spark.cassandra.connection.host', '127.0.0.1:9042')\
  .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')


# def save_to_cassandra(batch_df, batch_id):
#     batch_df.write \
#         .format("org.apache.spark.sql.cassandra") \
#         .options(table="test3", keyspace="mykeyspace")\
#         .mode("append")\
#         .save()

schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", StringType(), True),  # To be converted to IntegerType
    StructField("SSN", StringType(), True),
    StructField("Occupation", StringType(), True),
    StructField("Annual_Income", StringType(), True),  # To be converted to FloatType
    StructField("Monthly_Inhand_Salary", FloatType(), True),
    StructField("Num_Bank_Accounts", IntegerType(), True),
    StructField("Num_Credit_Card", IntegerType(), True),
    StructField("Interest_Rate", IntegerType(), True),
    StructField("Num_of_Loan", StringType(), True),  # To be converted to FloatType
    StructField("Type_of_Loan", StringType(), True),
    StructField("Delay_from_due_date", IntegerType(), True),
    StructField("Num_of_Delayed_Payment", StringType(), True),  # To be converted to FloatType
    StructField("Changed_Credit_Limit", StringType(), True),  # To be converted to FloatType
    StructField("Num_Credit_Inquiries", FloatType(), True),
    StructField("Credit_Mix", StringType(), True),
    StructField("Outstanding_Debt", StringType(), True),  # To be converted to FloatType
    StructField("Credit_Utilization_Ratio", FloatType(), True),
    StructField("Credit_History_Age", StringType(), True),
    StructField("Payment_of_Min_Amount", StringType(), True),
    StructField("Total_EMI_per_month", FloatType(), True),
    StructField("Amount_invested_monthly", StringType(), True),  # To be converted to FloatType
    StructField("Payment_Behaviour", StringType(), True),
    StructField("Monthly_Balance", StringType(), True)  # To be converted to FloatType
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.80.83:9092") \
    .option("subscribe", "credit_card") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", False) \
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
    .select(from_json("value", schema).alias("json")) \
    .select("json.*")

df_qom = df.filter("ID is not null") \
    .select(
    "Age",
    "Annual_Income",
    "Monthly_Inhand_Salary",
    "Num_Bank_Accounts",
    "Num_Credit_Card",
    "Interest_Rate",
    "Num_of_Loan",
    "Delay_from_due_date",
    "Num_of_Delayed_Payment",
    "Changed_Credit_Limit",
    "Num_Credit_Inquiries",
    "Outstanding_Debt",
    "Credit_Utilization_Ratio",
    "Total_EMI_per_month",
    "Amount_invested_monthly",
    "Monthly_Balance",
    "Occupation",
    "Credit_History_Age",
    "Payment_of_Min_Amount",
    "Payment_Behaviour"
    )



df_qom.writeStream \
    .trigger(processingTime="10 seconds") \
    .format("console") \
    .option("checkpointLocation", "/home/ktinh/checkpoint") \
    .outputMode("update") \
    .start()



spark.streams.awaitAnyTermination()
spark.stop()
