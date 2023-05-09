from pyspark.sql import SparkSession

# Define Spark session
spark = SparkSession.builder.appName("Top Bike by Day").getOrCreate()

# Define CSV schema
schema = "id STRING, duration FLOAT, start_date TIMESTAMP, start_station_name STRING, \
          start_station_id STRING, end_date TIMESTAMP, end_station_name STRING, \
          end_station_id STRING, bike_id STRING, subscription_type STRING, zip_code STRING"

# Read CSV file
df = spark.read.format("csv") \
    .option("header", True) \
    .option("timestampFormat", "M/d/y H:m") \
    .schema(schema) \
    .load("../resources/input/trip.csv")

df.createOrReplaceTempView("trips")

result_df = spark.sql("""
    SELECT md.date AS max_date, td.bike_id, md.max_duration
    FROM (
        SELECT date, MAX(total_duration) AS max_duration
        FROM (
            SELECT DATE(start_date) AS date, SUM(duration) AS total_duration
            FROM trips
            GROUP BY date, bike_id
        )
        GROUP BY date
    ) AS md
    JOIN (
        SELECT DATE(start_date) AS date, bike_id, SUM(duration) AS total_duration
        FROM trips
        GROUP BY date, bike_id
    ) AS td
    ON md.date = td.date AND md.max_duration = td.total_duration
""")

# Convert duration to integer
result_df = result_df.withColumn("max_duration", result_df.max_duration.cast("integer"))

# Save result as CSV file
result_df.write.format("csv") \
    .option("header", True) \
    .mode("overwrite") \
    .save("../resources/output")
