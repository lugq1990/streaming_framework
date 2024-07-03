import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

class TransformationEngine:
    def __init__(self, config):
        self.config = config

    def _filter(self, df, transformation):
        return df.filter(f"{transformation['column']} {transformation['condition']}")

    def _select(self, df, transformation):
        return df.select(*transformation['columns'])

    def _withColumn(self, df, transformation):
        return df.withColumn(transformation['column'], expr(transformation['expression']))

    def _drop(self, df, transformation):
        return df.drop(*transformation['columns'])

    def _groupBy(self, df, transformation):
        group_cols = transformation['columns']
        agg_exprs = {key: expr(value) for key, value in transformation['aggregations'].items()}
        return df.groupBy(*group_cols).agg(agg_exprs)

    def _orderBy(self, df, transformation):
        return df.orderBy(*transformation['columns'], ascending=transformation.get('ascending', True))

    def _limit(self, df, transformation):
        return df.limit(transformation['num'])

    def apply_transformation(self, df, transformation):
        transformation_type = transformation['type']
        method = getattr(self, f"_{transformation_type}", None)
        if method is not None:
            return method(df, transformation)
        else:
            raise ValueError(f"Unknown transformation type: {transformation_type}")

    def apply_transformations(self, df):
        for transformation in self.config['transformations']:
            df = self.apply_transformation(df, transformation)
        return df

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Load the transformation configuration
with open('transformations.json', 'r') as file:
    config = json.load(file)

# Define the schema for the transaction data
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("customer_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("timestamp", StringType(), True),  # We'll convert this to TimestampType later
    StructField("description", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("merchant", StringType(), True)
])

# Read the Kafka stream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transaction") \
    .option("startingOffsets", "latest") \
    .load()

# Select the value column and cast it to string
transactions_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse the JSON data and apply the schema
parsed_df = transactions_df \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert the timestamp string to TimestampType
parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Create an instance of the TransformationEngine and apply transformations
transformation_engine = TransformationEngine(config)
transformed_df = transformation_engine.apply_transformations(parsed_df)

# Display the processed data to the console
query = transformed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the streaming to finish
query.awaitTermination()
