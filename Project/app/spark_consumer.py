from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, LongType

# Define Spark session
spark = SparkSession.builder.appName("Top Bike by Day from Kafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,"
                                   "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Define schema for the Kafka message
schema = StructType([
    StructField("max_date", DateType(), True),
    StructField("bike_id", IntegerType(), True),
    StructField("max_duration", LongType(), True)
])

# Read input data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "bike_topic") \
    .option("startingOffsets", "latest") \
    .load()

clean_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Write output to the console
console_output = clean_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "checkpoint") \
    .option("keyspace", "bikes") \
    .option("table", "max_duration_bike") \
    .start()

console_output.awaitTermination(60)
