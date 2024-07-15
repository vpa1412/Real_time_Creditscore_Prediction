from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, count, isnan, regexp_replace, udf
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.types import StringType, IntegerType
from pyspark.ml.linalg import DenseVector
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import findspark

findspark.init()

# Build the SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("ModelTraining") \
    .config("spark.executor.memory", "1gb") \
    .getOrCreate()

sc = spark.sparkContext

rdd = sc.textFile('hdfs://192.168.80.66:9000/vpa2003/processed.csv')
# rdd.take(5)
df = spark.read.csv('hdfs://192.168.80.66:9000/vpa2003/processed.csv', header=True)
# df.show(5)
def splitdt(x):
    return (DenseVector(x[:20]), x[20])
# Define the `input_data`
# input_data = df.rdd.map(lambda x: (DenseVector(x[:4]), x[4]))

input_data = df.rdd.map(splitdt)

# Replace `df` with the new DataFrame
df1 = spark.createDataFrame(input_data, ["X", "Y"])

indexer = StringIndexer(inputCol="Y", outputCol="Yn")
indexed = indexer.fit(df1).transform(df1)
(trainingData, testData) = indexed.randomSplit([0.7, 0.3])

rf = RandomForestClassifier(labelCol="Yn",
                            featuresCol="X",
                            numTrees=10)
model = rf.fit(trainingData)
predictions = model.transform(testData)
predictions.select("Yn", "prediction")

evaluator = MulticlassClassificationEvaluator(
    labelCol="Yn", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
# print("Test Accuracy = %g" % (accuracy))
model.save('hdfs://192.168.80.66:9000/kt/model')
# df.write.format("delta").save("/path/to/delta-table")
