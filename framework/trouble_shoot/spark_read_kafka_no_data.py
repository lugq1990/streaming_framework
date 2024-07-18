

from utils import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType

from pyspark.sql.functions import col, from_json

topic_name = 'test'
group_id = 'test'
bootstrap_servers = ['localhost:9092']

# Create a Spark session
spark = get_spark_session()
# Create a streaming DataFrame that reads from Kafka
# Replace <kafka_bootstrap_servers> with your Kafka server address
# Replace <topic_name> with the Kafka topic name
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",bootstrap_servers[0]) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Define the schema for the data being read from Kafka
schema = StructType() \
    .add("value", StringType()) \
    .add("timestamp", StringType())

# Select only the value column from the Kafka message
# The value column contains the actual message payload
df = df.selectExpr("CAST(value AS STRING)")
schema = DataUtil._get_spark_schema(topic_name, bootstrap_servers=bootstrap_servers)
print(schema)
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df = df.withColumn("value", from_json(col("value"), schema)).select("value.*")

# Start the streaming query
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()