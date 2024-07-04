
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from uuid import uuid4





class SparkSessionSingleton(object):
    _instance = None
    
    @staticmethod
    def get_instance(app_name='sparksession', master='local[*]'):
        if not SparkSessionSingleton._instance:
            SparkSessionSingleton._instance = SparkSession \
               .builder \
               .appName(app_name) \
               .master(master) \
               .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
               .getOrCreate()
        return SparkSessionSingleton._instance
    

spark = SparkSessionSingleton.get_instance()

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
