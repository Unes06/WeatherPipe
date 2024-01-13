from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaStreamProcessing").getOrCreate()


# Define the schema to match the JSON structure
schema = StructType([
    StructField("full_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("location", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("postcode", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("email", StringType(), True)
])

# Read data from Kafka as a streaming DataFrame
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092") \
    .option("subscribe", "weather_data") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data
result_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")


# Wait for the streaming query to terminate
