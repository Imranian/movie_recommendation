from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

spark = SparkSession.builder \
    .appName("KafkaMovieStream") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user-events") \
    .load()

# Define schema based on TMDb structure
schema = StructType() \
    .add("id", IntegerType()) \
    .add("title", StringType()) \
    .add("overview", StringType()) \
    .add("rating", FloatType()) \
    .add("popularity", FloatType()) \
    .add("release_date", StringType())

# Parse Kafka values
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Display parsed movie data
query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "/user/imran/movies_data") \
    .option("checkpointLocation", "/user/imran/checkpoints") \
    .start()

query.awaitTermination()
