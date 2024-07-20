from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Showtable") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
df = spark.read.format("delta").load("hdfs://192.168.80.41:9000/kt/deltable")
df.show()