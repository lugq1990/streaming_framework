
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create or retrieve a SparkSession
spark = SparkSession.builder \
    .appName("KafkaStreamReader") \
    .getOrCreate()

# Kafka configuration (replace with your details)
bootstrap_servers = "localhost:9092"
subscribe_topic = "transaction"

# Read data stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", subscribe_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Print the data to the console for demonstration (replace with your processing logic)
query = df.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value") \
    .writeStream \
    .format("console") \
    .option("checkpointLocation", "/tmp/spark-checkpoint")  \
    .start()

# Wait for query termination (optional)
query.awaitTermination()
