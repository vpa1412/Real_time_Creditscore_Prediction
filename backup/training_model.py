from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# Tạo Spark session
spark = SparkSession.builder \
    .appName("BankCardModelTraining") \
    .getOrCreate()

# Đọc dữ liệu từ file CSV và tự động suy luận schema
df = spark.read.csv("processed.csv", header=True, inferSchema=True)

# Chuyển đổi các cột phân loại thành số
indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(df) for column in ["Occupation", "Credit_Mix", "Payment_of_Min_Amount", "Payment_Behaviour"]]

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
        "Occupation_index",
        "Credit_History_Age",
        "Payment_of_Min_Amount_index",
        "Payment_Behaviour_index"
    ],
    outputCol="features"
)

# Chuyển đổi cột đích thành số
label_indexer = StringIndexer(inputCol="target", outputCol="label").fit(df)

# Định nghĩa mô hình RandomForest
rf = RandomForestClassifier(featuresCol="features", labelCol="label")

# Tạo pipeline
pipeline = Pipeline(stages=indexers + [assembler, label_indexer, rf])

# Huấn luyện mô hình
model = pipeline.fit(df)

# Lưu mô hình đã huấn luyện
model.write().overwrite().save("bank_card_model")

# Dừng Spark session
spark.stop()
