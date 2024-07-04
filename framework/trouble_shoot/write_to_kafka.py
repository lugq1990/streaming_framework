import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import expr, struct, to_json
import json
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType;
import tempfile


def write_to_kafka(df, params, default_split_key=','):
    """Write data to kafka, supported with selected cols to dump

    Args:
        df (_type_): _description_
        params (_type_): _description_
        default_split_key (str, optional): _description_. Defaults to ','.

    Returns:
        _type_: _description_
    """
    selected_cols = params.get('selected_cols', None)
    
    if selected_cols and not isinstance(selected_cols, list) and isinstance(selected_cols, str):
        # Try to convert to list
        selected_cols = selected_cols.split(default_split_key)
    
    if not selected_cols:
        # Then just dump full cols
        selected_cols = df.columns
    
    print("Get selected: ", selected_cols)

    # Convert selected columns to JSON
    value_expr = to_json(struct([col(c) for c in selected_cols])).alias("value")

    kafka_df = df.select(value_expr, col("transaction_id").cast("string").alias("key"))

    return kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", params['bootstrapServers']) \
        .option("topic", params['topic']) \
        .option("checkpointLocation", tempfile.mkdtemp()) \
        .start()

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkKafkaStreaming") \
        .config("spark.sql.streaming.checkpointLocation", tempfile.mkdtemp()) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transaction") \
        .option("startingOffsets", "earliest") \
        .load()

    # Select the value column and cast it to string
    transactions_df = kafka_df.selectExpr("CAST(value AS STRING)")
    
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


   # Parse the JSON data and apply the schema
    parsed_df = transactions_df \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Convert the timestamp string to TimestampType
    parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))


    params = {
        "bootstrapServers": "localhost:9092",
        "topic": "transaction_output",
        "selected_cols": "transaction_id,amount,customer_id,timestamp"
    }

    query = write_to_kafka(parsed_df, params)
    query.awaitTermination()
