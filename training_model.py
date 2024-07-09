from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, count, isnan
from pyspark.sql.types import StringType

# Create Spark session
spark = SparkSession.builder \
    .appName("CreditDataPreprocessing") \
    .getOrCreate()

# Load the dataset
df = spark.read.csv('train.csv', header=True, inferSchema=True)

# Drop unwanted columns
df = df.drop("ID", "Customer_ID", "Month", "Name", "SSN", "Type_of_Loan")

# Replace '_' with '' in string columns

for column in df.columns:
    if df.schema[column].dataType == StringType():
        df = df.withColumn(column, regexp_replace(column, '_', ''))

# Fill NA values with mode
from pyspark.sql.functions import col, count, isnan

mode_dict = {}
for column in df.columns:
    mode_value = df.groupBy(column).count().orderBy("count", ascending=False).first()[0]
    df = df.fillna({column: mode_value})

# Convert relevant columns to float
df = df.withColumn("Annual_Income", col("Annual_Income").cast("float")) \
       .withColumn("Num_of_Loan", col("Num_of_Loan").cast("float")) \
       .withColumn("Num_of_Delayed_Payment", col("Num_of_Delayed_Payment").cast("float")) \
       .withColumn("Changed_Credit_Limit", col("Changed_Credit_Limit").cast("float")) \
       .withColumn("Outstanding_Debt", col("Outstanding_Debt").cast("float")) \
       .withColumn("Age", col("Age").cast("float")) \
       .withColumn("Amount_invested_monthly", col("Amount_invested_monthly").cast("float")) \
       .withColumn("Monthly_Balance", col("Monthly_Balance").cast("float"))

# Calculate IQR and remove outliers

numeric_columns = [column for column in df.columns if df.schema[column].dataType != StringType()]

for column in numeric_columns:
    q1 = df.approxQuantile(column, [0.25], 0.01)[0]
    q3 = df.approxQuantile(column, [0.75], 0.01)[0]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    df = df.filter((col(column) >= lower_bound) & (col(column) <= upper_bound))



# List of categorical columns
categorical_columns = ["Occupation", "Credit_Mix", "Credit_History_Age", "Payment_of_Min_Amount", "Payment_Behaviour"]

# Indexing and encoding stages
stages = []
for column in categorical_columns:
    string_indexer = StringIndexer(inputCol=column, outputCol=column + "_Index")
    one_hot_encoder = OneHotEncoder(inputCol=column + "_Index", outputCol=column + "_OHE")
    stages += [string_indexer, one_hot_encoder]

# Create a pipeline to execute indexing and encoding
pipeline = Pipeline(stages=stages)
df = pipeline.fit(df).transform(df)

# Drop original and indexed columns, keep only the OHE columns
indexed_columns = [column + "_Index" for column in categorical_columns]
ohe_columns = [column + "_OHE" for column in categorical_columns]
df = df.drop(*categorical_columns).drop(*indexed_columns)

df.show()

