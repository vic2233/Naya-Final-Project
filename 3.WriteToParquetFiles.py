
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, current_date
from pyspark.sql.types import StructType, StringType
import configuration as c

# Initialize Spark Session--------LOCAL------
spark = spark = SparkSession.builder \
    .appName("CS_Data_to_Purquet") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0')\
 .getOrCreate()

# Kafka Configuration
kafka_broker = c.KAFKA_BROKER
topic = c.KAFKA_TOPIC

#s3_bucket = "s3://naya-college-student-victoria/FinalProject/AppSourceData/" --AWS
#s3_checkpoints="s3://naya-college-student-victoria/FinalProject/checkpoints/"--AWS
s3_bucket =c.s3_bucket
s3_checkpoints=c.s3_checkpoints


# Define Schema for JSON Data
schema = StructType() \
    .add("Unique id", StringType()) \
    .add("channel_name", StringType()) \
    .add("category", StringType()) \
    .add("Sub-category", StringType()) \
    .add("Customer Remarks", StringType()) \
    .add("Order_id", StringType()) \
    .add("order_date_time", StringType()) \
    .add("Issue_reported at", StringType()) \
    .add("issue_responded", StringType()) \
    .add("Survey_response_Date", StringType()) \
    .add("Customer_City", StringType()) \
    .add("Product_category", StringType()) \
    .add("Item_price", StringType()) \
    .add("connected_handling_time", StringType()) \
    .add("Agent_name", StringType()) \
    .add("Supervisor", StringType()) \
    .add("Manager", StringType()) \
    .add("Tenure Bucket", StringType()) \
    .add("Agent Shift", StringType()) \
    .add("CSAT Score", StringType())

# Read from Kafka as Streaming Data
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .load()

   #.option("startingOffsets", "earliest")\
   #.option("startingOffsets", "latest") \

# Parse JSON messages 
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .selectExpr("data.`Unique id` as unique_id", 
                "data.channel_name as channel_name", 
                "data.category as category", 
                "data.`Sub-category` as sub_category", 
                "data.`Customer Remarks` as customer_remarks", 
                "data.Order_id as order_id", 
                "data.order_date_time as order_date_time", 
                "data.`Issue_reported at` as issue_reported_at", 
                "data.issue_responded as issue_responded", 
                "data.Survey_response_Date as survey_response_Date", 
                "data.Customer_City as customer_City", 
                "data.Product_category as product_category", 
                "data.Item_price as item_price", 
                "data.connected_handling_time as connected_handling_time", 
                "data.Agent_name as agent_name", 
                "data.Supervisor as supervisor", 
                "data.Manager as manager", 
                "data.`Tenure Bucket` as tenure_bucket", 
                "data.`Agent Shift` as agent_shift", 
                "data.`CSAT Score` as csat_score") \
    .withColumn("partition_date", current_date())

# Write to S3
query = parsed_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", s3_checkpoints) \
    .option("path", s3_bucket) \
    .trigger(processingTime="5 seconds") \
    .start()

 #   .partitionBy("partition_date") \

query.awaitTermination()
