# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, when, last, coalesce, udf
# from pyspark.sql.types import *
# from pyspark.sql.functions import from_json, col, count, avg, min, max
# from pyspark.sql.window import Window
# from pyspark.ml import PipelineModel
#
#
#
# spark = SparkSession.builder.appName("Cassandra")\
#   .config('spark.cassandra.connection.host', '127.0.0.1:9042')\
#   .getOrCreate()
#
# spark.sparkContext.setLogLevel('ERROR')
#
#
# # def save_to_cassandra(batch_df, batch_id):
# #     batch_df.write \
# #         .format("org.apache.spark.sql.cassandra") \
# #         .options(table="test3", keyspace="mykeyspace")\
# #         .mode("append")\
# #         .save()
#
# schema = StructType([
#     StructField("ID", StringType(), True),
#     StructField("Customer_ID", StringType(), True),
#     StructField("Month", StringType(), True),
#     StructField("Name", StringType(), True),
#     StructField("Age", StringType(), True),  # To be converted to IntegerType
#     StructField("SSN", StringType(), True),
#     StructField("Occupation", StringType(), True),
#     StructField("Annual_Income", StringType(), True),  # To be converted to FloatType
#     StructField("Monthly_Inhand_Salary", FloatType(), True),
#     StructField("Num_Bank_Accounts", IntegerType(), True),
#     StructField("Num_Credit_Card", IntegerType(), True),
#     StructField("Interest_Rate", IntegerType(), True),
#     StructField("Num_of_Loan", StringType(), True),  # To be converted to FloatType
#     StructField("Type_of_Loan", StringType(), True),
#     StructField("Delay_from_due_date", IntegerType(), True),
#     StructField("Num_of_Delayed_Payment", StringType(), True),  # To be converted to FloatType
#     StructField("Changed_Credit_Limit", StringType(), True),  # To be converted to FloatType
#     StructField("Num_Credit_Inquiries", FloatType(), True),
#     StructField("Credit_Mix", StringType(), True),
#     StructField("Outstanding_Debt", StringType(), True),  # To be converted to FloatType
#     StructField("Credit_Utilization_Ratio", FloatType(), True),
#     StructField("Credit_History_Age", StringType(), True),
#     StructField("Payment_of_Min_Amount", StringType(), True),
#     StructField("Total_EMI_per_month", FloatType(), True),
#     StructField("Amount_invested_monthly", StringType(), True),  # To be converted to FloatType
#     StructField("Payment_Behaviour", StringType(), True),
#     StructField("Monthly_Balance", StringType(), True)  # To be converted to FloatType
# ])
#
#
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "192.168.80.83:9092") \
#     .option("subscribe", "credit_card") \
#     .option("startingOffsets", "latest") \
#     .option("failOnDataLoss", False) \
#     .load() \
#     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
#     .select(from_json("value", schema).alias("json")) \
#     .select("json.*")
#
# df_qom = df.filter("ID is not null") \
#     .select(
#     "Age",
#     "Annual_Income",
#     "Monthly_Inhand_Salary",
#     "Num_Bank_Accounts",
#     "Num_Credit_Card",
#     "Interest_Rate",
#     "Num_of_Loan",
#     "Delay_from_due_date",
#     "Num_of_Delayed_Payment",
#     "Changed_Credit_Limit",
#     "Num_Credit_Inquiries",
#     "Outstanding_Debt",
#     "Credit_Utilization_Ratio",
#     "Total_EMI_per_month",
#     "Amount_invested_monthly",
#     "Monthly_Balance",
#     "Occupation",
#     "Credit_History_Age",
#     "Payment_of_Min_Amount",
#     "Payment_Behaviour"
#     )
#
# # Fill NA values with mode
# mode_dict = {}
# for column in df_qom.columns:
#     mode_value = df_qom.groupBy(column).count().orderBy("count", ascending=False).first()[0]
#     if mode_value is not None:
#         df_qom = df_qom.fillna({column: mode_value})
#
# # Convert relevant columns to float
# df_qom = df_qom.withColumn("Annual_Income", col("Annual_Income").cast("float")) \
#                .withColumn("Num_of_Loan", col("Num_of_Loan").cast("float")) \
#                .withColumn("Num_of_Delayed_Payment", col("Num_of_Delayed_Payment").cast("float")) \
#                .withColumn("Changed_Credit_Limit", col("Changed_Credit_Limit").cast("float")) \
#                .withColumn("Outstanding_Debt", col("Outstanding_Debt").cast("float")) \
#                .withColumn("Age", col("Age").cast("float")) \
#                .withColumn("Amount_invested_monthly", col("Amount_invested_monthly").cast("float")) \
#                .withColumn("Monthly_Balance", col("Monthly_Balance").cast("float"))
#
# # Calculate IQR and remove outliers
# numeric_columns = [column for column in df_qom.columns if df_qom.schema[column].dataType != StringType()]
#
# for column in numeric_columns:
#     q1 = df_qom.approxQuantile(column, [0.25], 0.01)[0]
#     q3 = df_qom.approxQuantile(column, [0.75], 0.01)[0]
#     iqr = q3 - q1
#     lower_bound = q1 - 1.5 * iqr
#     upper_bound = q3 + 1.5 * iqr
#     df_qom = df_qom.filter((col(column) >= lower_bound) & (col(column) <= upper_bound))
#
# # Define window specification
# window_spec = Window.orderBy("Age").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
#
# # Fill missing 'Occupation' values using window functions
# df_qom = df_qom.withColumn("Occupation",
#                            coalesce(
#                                when(col("Occupation") == "_______", last(col("Occupation"), True).over(window_spec)),
#                                col("Occupation")
#                            ))
#
# df_qom = df_qom.withColumn("Occupation",
#                            coalesce(
#                                when(col("Occupation") == "_______", last(col("Occupation"), False).over(window_spec)),
#                                col("Occupation")
#                            ))
#
# # Define occupation mappings
# occupation_mappings = {"Scientist": 1, "Media_Manager": 2, "Musician": 3, "Lawyer": 4, "Teacher": 5, "Developer": 6,
#                        "Writer": 7, "Architect": 8, "Mechanic": 9, "Entrepreneur": 10, "Journalist": 11,
#                        "Doctor": 12, "Engineer": 13, "Accountant": 14, "Manager": 15}
#
# occupation_udf = udf(lambda x: occupation_mappings.get(x, -1), IntegerType())
# df_qom = df_qom.withColumn("Occupation_Numeric", occupation_udf(col("Occupation"))).drop("Occupation")
#
# # Define UDF for converting credit history age to numeric
# def credit_history_age_to_numeric(cha):
#     if cha == "NA":
#         return 0
#     else:
#         parts = cha.split(" ")
#         years = int(parts[0]) if "Years" in parts[1] else 0
#         months = int(parts[2]) if "Months" in parts[3] else 0
#         return years * 12 + months
#
# credit_history_age_udf = udf(credit_history_age_to_numeric, IntegerType())
# df_qom = df_qom.withColumn("Credit_History_Age_Numeric", credit_history_age_udf(col("Credit_History_Age"))).drop("Credit_History_Age")
#
# # Define mappings for Payment_of_Min_Amount
# payment_min_amount_mappings = {"Yes": 1, "No": 2, "NM": 3}
# payment_min_amount_udf = udf(lambda x: payment_min_amount_mappings.get(x, 0), IntegerType())
# df_qom = df_qom.withColumn("Payment_of_Min_Amount_Numeric", payment_min_amount_udf(col("Payment_of_Min_Amount"))).drop("Payment_of_Min_Amount")
#
# # Define mappings and UDF for Payment_Behaviour
# valid_payment_behaviours = [
#     "Low_spent_Small_value_payments", "High_spent_Medium_value_payments",
#     "High_spent_Small_value_payments", "Low_spent_Large_value_payments",
#     "Low_spent_Medium_value_payments", "High_spent_Large_value_payments"
# ]
#
# payment_behaviour_window = Window.orderBy("Age").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
#
# df_qom = df_qom.withColumn("Payment_Behaviour",
#                            coalesce(
#                                when(~col("Payment_Behaviour").isin(valid_payment_behaviours),
#                                     last(col("Payment_Behaviour"), True).over(payment_behaviour_window)),
#                                col("Payment_Behaviour")
#                            ))
#
# df_qom = df_qom.withColumn("Payment_Behaviour",
#                            coalesce(
#                                when(~col("Payment_Behaviour").isin(valid_payment_behaviours),
#                                     last(col("Payment_Behaviour"), False).over(payment_behaviour_window)),
#                                col("Payment_Behaviour")
#                            ))
#
# payment_behaviour_mappings = {
#     "High_spent_Small_value_payments": 1,
#     "High_spent_Medium_value_payments": 2,
#     "High_spent_Large_value_payments": 3,
#     "Low_spent_Small_value_payments": 4,
#     "Low_spent_Medium_value_payments": 5,
#     "Low_spent_Large_value_payments": 6
# }
#
# payment_behaviour_udf = udf(lambda x: payment_behaviour_mappings.get(x, 0), IntegerType())
# df_qom = df_qom.withColumn("Payment_Behaviour_Numeric", payment_behaviour_udf(col("Payment_Behaviour"))).drop("Payment_Behaviour")
#
# # Load the model from HDFS
# model_path = "hdfs://192.168.80.66:9000/kt/model"
# model = PipelineModel.load(model_path)
#
# # Predict credit score
# predictions = model.transform(df_qom)
#
# # Generate a new transaction ID and print the results
# from pyspark.sql.functions import monotonically_increasing_id
#
# predictions = predictions.withColumn("Transaction_ID", monotonically_increasing_id())
# predictions.select("Transaction_ID", "prediction").writeStream \
#     .format("console") \
#     .option("checkpointLocation", "/home/ktinh/checkpoint1") \
#     .outputMode("append") \
#     .start()
#
#
#
# df_qom.writeStream \
#     .trigger(processingTime="10 seconds") \
#     .format("console") \
#     .option("checkpointLocation", "/home/ktinh/checkpoint") \
#     .outputMode("update") \
#     .start()
#
#
#
# spark.streams.awaitAnyTermination()
# spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, last, coalesce, udf, count, monotonically_increasing_id
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
import logging

spark = SparkSession.builder.appName("Cassandra") \
    .config('spark.cassandra.connection.host', '127.0.0.1:9042') \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", StringType(), True),
    StructField("SSN", StringType(), True),
    StructField("Occupation", StringType(), True),
    StructField("Annual_Income", StringType(), True),
    StructField("Monthly_Inhand_Salary", FloatType(), True),
    StructField("Num_Bank_Accounts", IntegerType(), True),
    StructField("Num_Credit_Card", IntegerType(), True),
    StructField("Interest_Rate", IntegerType(), True),
    StructField("Num_of_Loan", StringType(), True),
    StructField("Type_of_Loan", StringType(), True),
    StructField("Delay_from_due_date", IntegerType(), True),
    StructField("Num_of_Delayed_Payment", StringType(), True),
    StructField("Changed_Credit_Limit", StringType(), True),
    StructField("Num_Credit_Inquiries", FloatType(), True),
    StructField("Credit_Mix", StringType(), True),
    StructField("Outstanding_Debt", StringType(), True),
    StructField("Credit_Utilization_Ratio", FloatType(), True),
    StructField("Credit_History_Age", StringType(), True),
    StructField("Payment_of_Min_Amount", StringType(), True),
    StructField("Total_EMI_per_month", FloatType(), True),
    StructField("Amount_invested_monthly", StringType(), True),
    StructField("Payment_Behaviour", StringType(), True),
    StructField("Monthly_Balance", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.80.83:9092") \
    .option("subscribe", "credit_card") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", False) \
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
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


def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    mode_dict = {}
    for column in batch_df.columns:
        mode_value = batch_df.groupBy(column).count().orderBy("count", ascending=False).first()
        if mode_value is not None and mode_value[0] is not None:
            batch_df = batch_df.fillna({column: mode_value[0]})

    batch_df = batch_df.withColumn("Annual_Income", col("Annual_Income").cast("float")) \
        .withColumn("Num_of_Loan", col("Num_of_Loan").cast("float")) \
        .withColumn("Num_of_Delayed_Payment", col("Num_of_Delayed_Payment").cast("float")) \
        .withColumn("Changed_Credit_Limit", col("Changed_Credit_Limit").cast("float")) \
        .withColumn("Outstanding_Debt", col("Outstanding_Debt").cast("float")) \
        .withColumn("Age", col("Age").cast("float")) \
        .withColumn("Amount_invested_monthly", col("Amount_invested_monthly").cast("float")) \
        .withColumn("Monthly_Balance", col("Monthly_Balance").cast("float"))

    numeric_columns = [column for column in batch_df.columns if batch_df.schema[column].dataType != StringType()]
    for column in numeric_columns:
        quantiles = batch_df.approxQuantile(column, [0.25, 0.75], 0.01)
        if len(quantiles) == 2:
            q1, q3 = quantiles
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            batch_df = batch_df.filter((col(column) >= lower_bound) & (col(column) <= upper_bound))

    window_spec = Window.orderBy("Age").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    batch_df = batch_df.withColumn("Occupation",
                                   coalesce(
                                       when(col("Occupation") == "_______",
                                            last(col("Occupation"), True).over(window_spec)),
                                       col("Occupation")
                                   ))

    batch_df = batch_df.withColumn("Occupation",
                                   coalesce(
                                       when(col("Occupation") == "_______",
                                            last(col("Occupation"), False).over(window_spec)),
                                       col("Occupation")
                                   ))

    occupation_mappings = {"Scientist": 1, "Media_Manager": 2, "Musician": 3, "Lawyer": 4, "Teacher": 5, "Developer": 6,
                           "Writer": 7, "Architect": 8, "Mechanic": 9, "Entrepreneur": 10, "Journalist": 11,
                           "Doctor": 12, "Engineer": 13, "Accountant": 14, "Manager": 15}

    occupation_udf = udf(lambda x: occupation_mappings.get(x, -1), IntegerType())
    batch_df = batch_df.withColumn("Occupation_Numeric", occupation_udf(col("Occupation"))).drop("Occupation")

    def credit_history_age_to_numeric(cha):
        if cha == "NA":
            return 0
        else:
            parts = cha.split(" ")
            years = int(parts[0]) if "Years" in parts[1] else 0
            months = int(parts[2]) if "Months" in parts[3] else 0
            return years * 12 + months

    credit_history_age_udf = udf(credit_history_age_to_numeric, IntegerType())
    batch_df = batch_df.withColumn("Credit_History_Age",
                                   credit_history_age_udf(col("Credit_History_Age"))).drop("Credit_History_Age")

    payment_min_amount_mappings = {"Yes": 1, "No": 2, "NM": 3}
    payment_min_amount_udf = udf(lambda x: payment_min_amount_mappings.get(x, 0), IntegerType())
    batch_df = batch_df.withColumn("Payment_of_Min_Amount",
                                   payment_min_amount_udf(col("Payment_of_Min_Amount"))).drop("Payment_of_Min_Amount")

    valid_payment_behaviours = [
        "Low_spent_Small_value_payments", "High_spent_Medium_value_payments",
        "High_spent_Small_value_payments", "Low_spent_Large_value_payments",
        "Low_spent_Medium_value_payments", "High_spent_Large_value_payments"
    ]

    payment_behaviour_window = Window.orderBy("Age").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    batch_df = batch_df.withColumn("Payment_Behaviour",
                                   coalesce(
                                       when(~col("Payment_Behaviour").isin(valid_payment_behaviours),
                                            last(col("Payment_Behaviour"), True).over(payment_behaviour_window)),
                                       col("Payment_Behaviour")
                                   ))

    batch_df = batch_df.withColumn("Payment_Behaviour",
                                   coalesce(
                                       when(~col("Payment_Behaviour").isin(valid_payment_behaviours),
                                            last(col("Payment_Behaviour"), False).over(payment_behaviour_window)),
                                       col("Payment_Behaviour")
                                   ))

    payment_behaviour_mappings = {
        "High_spent_Small_value_payments": 1,
        "High_spent_Medium_value_payments": 2,
        "High_spent_Large_value_payments": 3,
        "Low_spent_Small_value_payments": 4,
        "Low_spent_Medium_value_payments": 5,
        "Low_spent_Large_value_payments": 6
    }

    payment_behaviour_udf = udf(lambda x: payment_behaviour_mappings.get(x, 0), IntegerType())
    batch_df = batch_df.withColumn("Payment_Behaviour", payment_behaviour_udf(col("Payment_Behaviour"))).drop(
        "Payment_Behaviour")

    # Assemble features into a single vector column
    feature_columns = ["Age", "Annual_Income", "Monthly_Inhand_Salary", "Num_Bank_Accounts", "Num_Credit_Card",
                       "Interest_Rate", "Num_of_Loan", "Delay_from_due_date", "Num_of_Delayed_Payment",
                       "Changed_Credit_Limit", "Num_Credit_Inquiries", "Outstanding_Debt",
                       "Credit_Utilization_Ratio", "Total_EMI_per_month", "Amount_invested_monthly",
                       "Monthly_Balance", "Occupation_Numeric", "Credit_History_Age_Numeric",
                       "Payment_of_Min_Amount_Numeric", "Payment_Behaviour_Numeric"]

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    batch_df = assembler.transform(batch_df)

    # Print the schema of the DataFrame before transformation
    batch_df.printSchema()

    # Load the model from HDFS
    model_path = "hdfs://192.168.80.66:9000/kt/model"
    model = RandomForestClassificationModel.load(model_path)

    # Predict credit score
    predictions = model.transform(batch_df)

    # Generate a new transaction ID and print the results
    predictions = predictions.withColumn("Transaction_ID", monotonically_increasing_id())
    predictions.select("Transaction_ID", "prediction").show()


df_qom.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/home/ktinh/checkpoint") \
    .start()

spark.streams.awaitAnyTermination()
spark.stop()
