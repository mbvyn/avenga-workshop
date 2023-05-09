from pyspark import SparkConf, SparkContext
from datetime import datetime

# Define Spark configuration
conf = SparkConf().setAppName("Top Bike by Day").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

# Read CSV file
original_rdd = sc.textFile("../resources/input/trip.csv")

# Filter header row
header = original_rdd.first()
clear_rdd = original_rdd.filter(lambda row: row != header)

# Split rows into fields
splited_rdd = clear_rdd.map(lambda row: row.split(","))

# Convert start date to date object
converted_rdd = splited_rdd.map(lambda row: (
    row[0],                                       # id
    float(row[1]),                                 # duration
    datetime.strptime(row[2], "%m/%d/%Y %H:%M"),  # start_date
    row[3],                                       # start_station_name
    row[4],                                       # start_station_id
    datetime.strptime(row[5], "%m/%d/%Y %H:%M"),  # end_date
    row[6],                                       # end_station_name
    row[7],                                       # end_station_id
    row[8],                                       # bike_id
    row[9],                                       # subscription_type
    row[10]))                                     # zip_code

# Group by date and bike id, and find the sum of duration
grouped_rdd = converted_rdd.map(lambda row: ((row[2].date(), row[8]), row[1])) \
    .reduceByKey(lambda a, b: a + b)

# Find the max duration for each date
max_duration_rdd = grouped_rdd.map(lambda row: (row[0][0], (row[0][1], row[1]))) \
    .reduceByKey(lambda a, b: a if a[1] > b[1] else b)

# Flatten the result and convert duration to integer
result_rdd = max_duration_rdd.map(lambda row: (row[0], row[1][0], int(row[1][1])))

# Save result as CSV file
result_rdd.map(lambda row: ",".join(map(str, row))) \
   .saveAsTextFile("../resources/output/")
