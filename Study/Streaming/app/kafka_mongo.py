from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

spark = SparkSession.builder \
    .appName("KafkaToMongoDB") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,org.mongodb.spark:mongo-spark-connector:10.0.0") \
    .config("spark.mongodb.output.uri", "mongodb://root:rootpassword@localhost:27017/bike_database.bike_collection") \
    .config("spark.mongodb.output.database", "bike_database") \
    .config("spark.mongodb.output.collection", "bike_collection") \
    .getOrCreate()

# Define schema for the Kafka message
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

# Read input data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "bike_topic") \
    .option("startingOffsets", "earliest") \
    .load()

clean_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

def write_row(batch_df , batch_id):
    batch_df.write.format("mongodb").mode("append").save()
    pass

# Write to MongoDB
query = clean_df \
    .writeStream \
    .foreachBatch(write_row)\
    .start()\
    .awaitTermination()


