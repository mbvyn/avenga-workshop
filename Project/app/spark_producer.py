from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType

spark = SparkSession.builder \
    .master('local') \
    .appName("Top Bike by Day") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2") \
    .getOrCreate()

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

df = spark.read.format("parquet") \
    .schema(schema) \
    .load("../resources/input/trip.parquet")

grouped_df = df.groupBy(col("start_date").cast("date").alias("date"), col("bike_id").alias("bike_id_grouped")) \
    .agg(sum("duration").alias("total_duration"))

max_duration_df = grouped_df.groupBy(col("date")) \
    .agg(max(col("total_duration")).alias("max_duration")) \
    .withColumnRenamed("date", "max_date")

result_df = max_duration_df.join(grouped_df, (max_duration_df.max_date == grouped_df.date) & (
        max_duration_df.max_duration == grouped_df.total_duration)) \
    .select(max_duration_df.max_date, grouped_df.bike_id_grouped, max_duration_df.max_duration)

result_df = result_df.withColumn("max_duration", result_df.max_duration.cast(IntegerType())) \
    .withColumnRenamed("bike_id_grouped", "bike_id")

kafka_df = result_df.selectExpr("to_json(struct(*)) AS value")

kafka_df.write.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", "bike_topic") \
    .save()
