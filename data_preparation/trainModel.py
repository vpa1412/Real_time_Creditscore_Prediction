from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# Create Spark session
spark = SparkSession.builder \
    .appName("BankCardModelTraining") \
    .getOrCreate()

# Read data from CSV file and infer schema automatically
df = spark.read.csv("/home/ktinh/PycharmProjects/final_bigdata/data/processed.csv", header=True, inferSchema=True)

# Print schema to checkvv
df.printSchema()

# Assemble features into a 'features' column
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

# Define RandomForest model
rf = RandomForestClassifier(featuresCol="features", labelCol="Credit_Score_Numeric")

# Create pipeline
pipeline = Pipeline(stages=[assembler, rf])

# Train the model
model = pipeline.fit(df)

# Save the trained model
# model.write().overwrite().save("hdfs://192.168.80.41:9000/kt/model_ok")

# Stop Spark session
spark.stop()