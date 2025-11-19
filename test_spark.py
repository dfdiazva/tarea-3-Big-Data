from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestSpark") \
    .getOrCreate()

data = [(1, "Hola"), (2, "Mundo")]
df = spark.createDataFrame(data, ["id", "texto"])

df.show()

spark.stop()
