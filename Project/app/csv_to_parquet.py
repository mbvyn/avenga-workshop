from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType

# Define Spark session
spark = SparkSession.builder.master('local').appName("Top Bike by Day").getOrCreate()

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
    StructField("bike_id", IntegerType(), True),
    StructField("subscription_type", StringType(), True),
    StructField("zip_code", StringType(), True)
])

# Read CSV file
df = spark.read.format("csv") \
             .option("header", True) \
             .option("timestampFormat", "M/d/y H:m") \
             .schema(schema) \
             .load("../resources/input/trip.csv")

# Save result as parquet file
df.repartition(1).write.parquet("../resources/output")
