from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Define Spark session
spark = SparkSession.builder.appName("Top Bike by Day Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Define CSV schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("duration", FloatType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("bike_id", StringType(), True),
    StructField("subscription_type", StringType(), True),
    StructField("zip_code", StringType(), True)
])

# Read CSV files using structured streaming
df = spark.readStream.format("csv") \
    .option("header", True) \
    .option("timestampFormat", "M/d/y H:m") \
    .schema(schema) \
    .load("../resources")

# Prepare the result DataFrame to send to Kafka
kafka_df = df.selectExpr("to_json(struct(*)) AS value")

# Send the result to a Kafka topic
kafka_df.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("topic", "bike_topic") \
    .option("checkpointLocation", "checkpoints") \
    .start() \
    .awaitTermination()
