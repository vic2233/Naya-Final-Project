from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from textblob import TextBlob
from pyspark.sql.functions import unix_timestamp
import time
import configuration as c

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkSentiment") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.2.27') \
    .getOrCreate()

# Define JSON schema
schema = StructType([
    StructField("Unique id", StringType(), True),
    StructField("Customer Remarks", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("Sub-category", StringType(), True),    
    StructField("Order_id", StringType(), True),
    StructField("Issue_reported at", StringType(), True),
    StructField("issue_responded", StringType(), True),
    StructField("Agent_name", StringType(), True),
    StructField("Supervisor", StringType(), True),
    StructField("Manager", StringType(), True),
    StructField("CSAT Score", StringType(), True)
])

# Function to perform sentiment analysis using TextBlob
def sentiment_analysis(text):
    try:
        if text:
            analysis = TextBlob(text)
            polarity = analysis.sentiment.polarity
            if polarity > 0:
                return "positive", polarity
            elif polarity < 0:
                return "negative", polarity
            else:
                return "neutral", polarity
        else:
            return "no response", 0.0
    except Exception as e:
        print(f"Error in sentiment analysis: {e}")
        return "error", 0.0

# Define UDF for sentiment analysis
sentiment_udf = F.udf(sentiment_analysis, StructType([
    StructField("sentiment_label", StringType(), True),
    StructField("sentiment_score", FloatType(), True)
]))

# Read data from Kafka
df_parsed = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", c.KAFKA_BROKER) \
    .option("subscribe", c.KAFKA_TOPIC) \
    .load()
#  .option("startingOffsets", "earliest") \

# Convert message column to JSON with schema
df_parsed = df_parsed.withColumn("json_data", F.from_json(F.col("value").cast("string"), schema))

# Extract columns from JSON
df_parsed = df_parsed.select(
    F.col("json_data.Unique id").alias("unique_id"),
    F.col("json_data.Customer Remarks").alias("customer_remarks"),
    F.col("json_data.channel_name").alias("channel_name"),
    F.col("json_data.category").alias("category"),
    F.col("json_data.Sub-category").alias("sub_category"),
    F.col("json_data.Order_id").alias("order_id"),
    F.col("json_data.Issue_reported at").alias("issue_datetime"),
    F.col("json_data.issue_responded").alias("response_datetime"),
    F.col("json_data.Agent_name").alias("agent_name"),
    F.col("json_data.Supervisor").alias("supervisor_name"),
    F.col("json_data.Manager").alias("manager_name"),
    F.col("json_data.CSAT Score").alias("csat_score")
)

# Convert string to timestamp for 'issue_datetime' and 'response_datetime'
df_parsed = df_parsed.withColumn(
    "issue_datetime",
    unix_timestamp("issue_datetime", "MM/dd/yyyy HH:mm").cast("timestamp")
)
df_parsed = df_parsed.withColumn(
    "response_datetime",
    unix_timestamp("response_datetime", "MM/dd/yyyy HH:mm").cast("timestamp")
)

# Apply UDF for sentiment analysis on Customer Remarks
df_analyzed = df_parsed.withColumn("sentiment", sentiment_udf(F.col("customer_remarks"))) \
                       .withColumn("update_datetime", F.current_timestamp())

# Flatten the nested structure and drop intermediate column
df_analyzed = df_analyzed.select(
    "*",
    F.col("sentiment.sentiment_label").alias("sentiment_label"),
    F.col("sentiment.sentiment_score").alias("sentiment_score")
).drop("sentiment")

# PostgreSQL connection options
postgres_options = {
    "url": c.pg_url,
    "dbtable": c.pg_dbtable,
    "user": c.pg_user,
    "password": c.pg_password,
    "driver": "org.postgresql.Driver"
}

# Write the processed stream 
query = df_analyzed.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, epoch_id: df.write \
                      .format("jdbc") \
                      .options(**postgres_options) \
                      .mode("append") \
                      .save()) \
    .start()

# Await termination
query.awaitTermination()
