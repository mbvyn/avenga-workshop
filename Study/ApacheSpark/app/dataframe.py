from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max
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
    StructField("bike_id", StringType(), True),
    StructField("subscription_type", StringType(), True),
    StructField("zip_code", StringType(), True)
])

# Read CSV file
df = spark.read.format("csv") \
             .option("header", True) \
             .option("timestampFormat", "M/d/y H:m") \
             .schema(schema) \
             .load("resources/input/trip.csv")

# Group by date and bike id, and find the sum of duration
grouped_df = df.groupBy(col("start_date").cast("date").alias("date"), col("bike_id").alias("bike_id_grouped")) \
               .agg(sum("duration").alias("total_duration"))

# Find the max duration for each date
max_duration_df = grouped_df.groupBy(col("date")) \
                            .agg(max(col("total_duration")).alias("max_duration")) \
                            .withColumnRenamed("date", "max_date")

# Join with the original DataFrame to get the corresponding bike id
result_df = max_duration_df.join(grouped_df, (max_duration_df.max_date == grouped_df.date) & (max_duration_df.max_duration == grouped_df.total_duration)) \
                           .select(max_duration_df.max_date, grouped_df.bike_id_grouped, max_duration_df.max_duration)

# Convert duration to integer
result_df = result_df.withColumn("max_duration", result_df.max_duration.cast(IntegerType()))\
                     .withColumnRenamed("bike_id_grouped", "bike_id")


# Save result as CSV file
result_df.repartition(10).write.format("csv") \
             .option("header", True) \
             .mode("overwrite")\
             .save("resources/output")
