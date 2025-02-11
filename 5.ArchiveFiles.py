from pyspark.sql import SparkSession
from datetime import datetime
import os


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("s3_remove_and_agg_daily") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()


# Define MinIO paths
MINIO_BUCKET = "s3a://root/"
PARQUET_PATH = "s3a://root/data/"  # Root folder with all Parquet files
AGGREGATED_PATH = "s3a://history/"


# Generate a timestamped Parquet file name
current_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
agg_filename = f"{AGGREGATED_PATH}{current_date}.parquet"


# Read all Parquet files into a single DataFrame
df = spark.read.parquet(PARQUET_PATH)


# Save the aggregated data as a single Parquet file
df.write.mode("overwrite").parquet(agg_filename)




# Ensure small files are deleted after aggregation
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
path = spark._jvm.org.apache.hadoop.fs.Path(PARQUET_PATH)


# Check if the directory exists before deleting
if fs.exists(path):
    for file_status in fs.listStatus(path):  # Iterate over files
        fs.delete(file_status.getPath(), True)  # Delete each file
    print(f"Deleted small files from {PARQUET_PATH}")
else:
    print(f"No files found to delete in {PARQUET_PATH}")


print(f"Daily aggregated data saved: {agg_filename}")


# Stop Spark Session
spark.stop()
