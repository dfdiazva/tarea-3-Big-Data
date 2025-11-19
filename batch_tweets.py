from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, split, explode,
    count as spark_count, date_format, to_timestamp
)

# 1. Crear sesión de Spark
spark = SparkSession.builder.appName("BatchTweets").getOrCreate()

# 2. Cargar el CSV de tweets
df = spark.read.csv(
    "data/tweets.csv",
    header=True,
    inferSchema=True
)

print("\n=== Datos originales ===")
df.show(truncate=False)

# 3. Limpiar texto (minúsculas, quitar signos de puntuación)
df_clean = df.withColumn(
    "clean_text",
    lower(regexp_replace(col("text"), "[^a-zA-ZáéíóúÁÉÍÓÚñÑ ]", ""))
)

# 4. Conteo de tweets por sentimiento
tweets_por_sentimiento = df_clean.groupBy("sentiment").agg(
    spark_count("*").alias("num_tweets")
)

print("\n=== Tweets por sentimiento ===")
tweets_por_sentimiento.show()

# 5. Palabras más frecuentes (muy simple)
words = df_clean.select(
    explode(split(col("clean_text"), r"\s+")).alias("word")
).where(col("word") != "")

palabras_frecuentes = (
    words.groupBy("word")
    .agg(spark_count("*").alias("count"))
    .orderBy(col("count").desc())
)

print("\n=== Palabras más frecuentes ===")
palabras_frecuentes.show(20)

# 6. Distribución de tweets por hora
df_time = df_clean.withColumn(
    "ts",
    to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss")
)

tweets_por_hora = df_time.groupBy(
    date_format(col("ts"), "yyyy-MM-dd HH:00").alias("hour")
).agg(
    spark_count("*").alias("num_tweets")
).orderBy("hour")

print("\n=== Tweets por hora ===")
tweets_por_hora.show(truncate=False)

print("\n✅ Procesamiento batch completado.")
spark.stop()

