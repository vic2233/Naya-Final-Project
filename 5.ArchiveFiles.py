from pyspark.sql import SparkSession
from datetime import datetime
import configuration as c

# Set Hadoop configurations
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('ROUTES_TO_S3') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
        .config("spark.hadoop.fs.s3a.endpoint", c.s3_url) \
    .config("spark.hadoop.fs.s3a.access.key", c.s3_key) \
    .config("spark.hadoop.fs.s3a.secret.key", c.s3_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Define MinIO paths
PARQUET_PATH = "s3a://finalproject/app_source_data/"
AGGREGATED_PATH = "s3a://history/"

# Generate a timestamped Parquet file name
current_date = datetime.now().strftime("%Y-%m-%d_%H-%M")

try:
    # Read Parquet files without _spark_metadata issues
    df = spark.read.option("mergeSchema", "true").parquet(PARQUET_PATH)

    if df.count() > 0:
        df.coalesce(1).write.mode("overwrite").parquet(f"{AGGREGATED_PATH}/agg_{current_date}")
        print(f"Aggregated data saved to {AGGREGATED_PATH}/agg_{current_date}")
    else:
        print("No data to aggregate.")

except Exception as e:
    print(f"Error: {str(e)}")

finally:
    spark.stop()
