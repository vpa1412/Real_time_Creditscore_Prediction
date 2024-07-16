from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# Tạo Spark session
spark = SparkSession.builder \
    .appName("BankCardModelTraining") \
    .getOrCreate()

# Đọc dữ liệu từ file CSV và tự động suy luận schema
df = spark.read.csv("processed.csv", header=True, inferSchema=True)

# In ra schema để kiểm tra
df.printSchema()

# Tập hợp các đặc trưng vào một cột 'features'
assembler = VectorAssembler(
    inputCols=[
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
    ],
    outputCol="features"
)

# Định nghĩa mô hình RandomForest
rf = RandomForestClassifier(featuresCol="features", labelCol="Credit_Score_Numeric")

# Tạo pipeline
pipeline = Pipeline(stages=[assembler, rf])

# Huấn luyện mô hình
model = pipeline.fit(df)

# Lưu mô hình đã huấn luyện
model.write().overwrite().save("credit_model")

# Dừng Spark session
spark.stop()