from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, from_json

# Tạo Spark session
spark = SparkSession.builder \
    .appName("BankCardPrediction") \
    .getOrCreate()

# Định nghĩa schema
schema = StructType([
    StructField("Age", IntegerType(), True),
    StructField("Annual_Income", IntegerType(), True),
    StructField("Monthly_Inhand_Salary", FloatType(), True),
    StructField("Num_Bank_Accounts", IntegerType(), True),
    StructField("Num_Credit_Card", IntegerType(), True),
    StructField("Interest_Rate", IntegerType(), True),
    StructField("Num_of_Loan", IntegerType(), True),
    StructField("Delay_from_due_date", IntegerType(), True),
    StructField("Num_of_Delayed_Payment", IntegerType(), True),
    StructField("Changed_Credit_Limit", IntegerType(), True),
    StructField("Num_Credit_Inquiries", IntegerType(), True),
    StructField("Outstanding_Debt", IntegerType(), True),
    StructField("Credit_Utilization_Ratio", FloatType(), True),
    StructField("Total_EMI_per_month", FloatType(), True),
    StructField("Amount_invested_monthly", IntegerType(), True),
    StructField("Monthly_Balance", IntegerType(), True),
    StructField("Occupation_Numeric", IntegerType(), True),
    StructField("Credit_History_Age_Numeric", IntegerType(), True),
    StructField("Payment_of_Min_Amount_Numeric", IntegerType(), True),
    StructField("Payment_Behaviour_Numeric", IntegerType(), True)
])

# Đọc dữ liệu từ Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank_cards") \
    .load()

# Phân tích cú pháp giá trị JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Tải mô hình đã huấn luyện
model = PipelineModel.load("credit_model")

# Dự đoán
predictions = model.transform(parsed_df)

# Chọn các cột cần thiết
output_df = predictions.select("Age", "Customer_ID", "prediction")

# Ghi kết quả dự đoán ra console (hoặc các nơi khác)
query = output_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
