from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, last, regexp_replace, lag, lead, first, when, coalesce, udf
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder \
    .appName("CreditDataPreprocessing") \
    .getOrCreate()

# Load the dataset
df = spark.read.csv('train.csv', header=True, inferSchema=True)

# Drop unwanted columns
df = df.drop("ID", "Customer_ID", "Month", "Name", "SSN", "Type_of_Loan", "Credit_Mix")

# Define a UDF to convert Credit_Score text to integers
# def credit_score_to_numeric(credit_score):
#     mappings = {'Good': 1, 'Standard': 2, 'Poor': 3}
#     return mappings.get(credit_score, 0)  # Return 0 if none of these values match
#
# # Register UDF
# credit_score_udf = udf(credit_score_to_numeric, IntegerType())
#
# # Apply UDF to create a new column
# df = df.withColumn("Credit_Score_Numeric", credit_score_udf(col("Credit_Score")))
#
# df = df.drop("Credit_Score")

# Fill NA values with mode
mode_dict = {}
for column in df.columns:
    mode_value = df.groupBy(column).count().orderBy("count", ascending=False).first()[0]
    if mode_value is not None:
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

# # List of categorical columns
# categorical_columns = ["Occupation", "Credit_Mix", "Credit_History_Age", "Payment_of_Min_Amount", "Payment_Behaviour"]
# # Indexing and encoding stages
# stages = []
# for column in categorical_columns:
#     string_indexer = StringIndexer(inputCol=column, outputCol=column + "_Index")
#     one_hot_encoder = OneHotEncoder(inputCol=column + "_Index", outputCol=column + "_OHE")
#     stages += [string_indexer, one_hot_encoder]
#
# # Create a pipeline to execute indexing and encoding
# pipeline = Pipeline(stages=stages)
# df = pipeline.fit(df).transform(df)
#
# # Drop original and indexed columns, keep only the OHE columns
# indexed_columns = [column + "_Index" for column in categorical_columns]
# ohe_columns = [column + "_OHE" for column in categorical_columns]
# df = df.drop(*categorical_columns).drop(*indexed_columns)

#Occupation
window_spec = Window.orderBy("Age").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df = df.withColumn("Occupation",
                   coalesce(
                       when(col("Occupation") == "_______", last(col("Occupation"), True).over(window_spec)),
                       col("Occupation")
                   ))

df = df.withColumn("Occupation",
                   coalesce(
                       when(col("Occupation") == "_______", last(col("Occupation"), False).over(window_spec)),
                       col("Occupation")
                   ))


# Preprocessing for Occupation
occupation_mappings = {"Scientist": 1, "Media_Manager": 2, "Musician": 3, "Lawyer": 4, "Teacher": 5, "Developer": 6, "Writer": 7, "Architect": 8, "Mechanic": 9, "Entrepreneur": 10, "Journalist": 11, "Doctor": 12, "Engineer": 13, "Accountant": 14, "Manager": 15}
occupation_udf = udf(lambda x: occupation_mappings.get(x, -1), IntegerType())
df = df.withColumn("Occupation_Numeric", occupation_udf(col("Occupation"))).drop("Occupation")

#Credit_History_Age
def credit_history_age_to_numeric(cha):
    if cha == "NA":
        return 0
    else:
        parts = cha.split(" ")
        years = int(parts[0]) if "Years" in parts[1] else 0
        months = int(parts[2]) if "Months" in parts[3] else 0
        return years * 12 + months

credit_history_age_udf = udf(credit_history_age_to_numeric, IntegerType())
df = df.withColumn("Credit_History_Age_Numeric", credit_history_age_udf(col("Credit_History_Age"))).drop("Credit_History_Age")

#Payment_of_Min_Amount
payment_min_amount_mappings = {"Yes": 1, "No": 2, "NM": 3}
payment_min_amount_udf = udf(lambda x: payment_min_amount_mappings.get(x, 0), IntegerType())
df = df.withColumn("Payment_of_Min_Amount_Numeric", payment_min_amount_udf(col("Payment_of_Min_Amount"))).drop("Payment_of_Min_Amount")

#Payment_Behaviour
valid_payment_behaviours = [
    "Low_spent_Small_value_payments",
    "High_spent_Medium_value_payments",
    "High_spent_Small_value_payments",
    "Low_spent_Large_value_payments",
    "Low_spent_Medium_value_payments",
    "High_spent_Large_value_payments"
]

payment_behaviour_window = Window.orderBy("Age").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df = df.withColumn("Payment_Behaviour",
                   coalesce(
                       when(~col("Payment_Behaviour").isin(valid_payment_behaviours), last(col("Payment_Behaviour"), True).over(payment_behaviour_window)),
                       col("Payment_Behaviour")
                   ))

df = df.withColumn("Payment_Behaviour",
                   coalesce(
                       when(~col("Payment_Behaviour").isin(valid_payment_behaviours), last(col("Payment_Behaviour"), False).over(payment_behaviour_window)),
                       col("Payment_Behaviour")
                   ))

# Preprocessing for Payment_Behaviour
payment_behaviour_mappings = {
    "High_spent_Small_value_payments": 1,
    "High_spent_Medium_value_payments": 2,
    "High_spent_Large_value_payments": 3,
    "Low_spent_Small_value_payments": 4,
    "Low_spent_Medium_value_payments": 5,
    "Low_spent_Large_value_payments": 6
}

payment_behaviour_udf = udf(lambda x: payment_behaviour_mappings.get(x, 0), IntegerType())
df = df.withColumn("Payment_Behaviour_Numeric", payment_behaviour_udf(col("Payment_Behaviour"))).drop("Payment_Behaviour")

def credit_score_to_numeric(credit_score):
    mappings = {'Good': 1, 'Standard': 2, 'Poor': 3}
    return mappings.get(credit_score, 0)  # Return 0 if none of these values match

# Register UDF
credit_score_udf = udf(credit_score_to_numeric, IntegerType())

# Apply UDF to create a new column
df = df.withColumn("Credit_Score_Numeric", credit_score_udf(col("Credit_Score")))

df = df.drop("Credit_Score")
df.show()

# # Specify the path where you want to save the CSV file
# output_path = "~/CS411/final_bigdata/processed.csv"
# #
# # # Save the DataFrame to CSV
# df.write.csv(path=output_path, mode="overwrite", header=True)
# #
# # Informative print statement
# print(f"DataFrame is saved as CSV at {output_path}")
