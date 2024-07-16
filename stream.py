from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, last, coalesce, udf
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, count, avg, min, max
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel



spark = SparkSession.builder.appName("KafkaDataProcessing")\
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
    StructField("ID", StringType()),
    StructField("Age", StringType()),  # Age is a float type
    StructField("Annual_Income", StringType()),  # Annual_Income is a float type
    StructField("Monthly_Inhand_Salary", StringType()),  # Monthly_Inhand_Salary is a float type
    StructField("Num_Bank_Accounts", StringType()),  # Num_Bank_Accounts is an integer type
    StructField("Num_Credit_Card", StringType()),  # Num_Credit_Card is an integer type
    StructField("Interest_Rate", StringType()),  # Interest_Rate is an integer type
    StructField("Num_of_Loan", StringType()),  # Num_of_Loan is a float type
    StructField("Delay_from_due_date", StringType()),  # Delay_from_due_date is an integer type
    StructField("Num_of_Delayed_Payment", StringType()),  # Num_of_Delayed_Payment is a float type
    StructField("Changed_Credit_Limit", StringType()),  # Changed_Credit_Limit is a float type
    StructField("Num_Credit_Inquiries", StringType()),  # Num_Credit_Inquiries is a float type
    StructField("Outstanding_Debt", StringType()),  # Outstanding_Debt is a float type
    StructField("Credit_Utilization_Ratio", StringType()),  # Credit_Utilization_Ratio is a float type
    StructField("Total_EMI_per_month", StringType()),  # Total_EMI_per_month is a float type
    StructField("Amount_invested_monthly", StringType()),  # Amount_invested_monthly is a float type
    StructField("Monthly_Balance", StringType()),  # Monthly_Balance is a float type
    StructField("Occupation_Numeric", StringType()),  # Occupation_Numeric is an integer type
    StructField("Credit_History_Age_Numeric", StringType()),  # Credit_History_Age_Numeric is an integer type
    StructField("Payment_of_Min_Amount_Numeric", StringType()),  # Payment_of_Min_Amount_Numeric is an integer type
    StructField("Payment_Behaviour_Numeric", StringType())  # Payment_Behaviour_Numeric is an integer type
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.80.83:9092") \
    .option("subscribe", "credit_testing") \
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
    "Occupation_Numeric",
    "Credit_History_Age_Numeric",
    "Payment_of_Min_Amount_Numeric",
    "Payment_Behaviour_Numeric"
    )



df_qom.writeStream \
    .trigger(processingTime="10 seconds") \
    .format("console") \
    .option("checkpointLocation", "/home/phuonganh/CS411/checkpoint3") \
    .outputMode("update") \
    .start()



spark.streams.awaitAnyTermination()
spark.stop()

