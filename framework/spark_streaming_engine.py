import yaml
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json


def read_data_stream(config):
    # Kafka configuration (replace with your details)
    bootstrap_servers = config.get('bootstrap_servers', "localhost:9092")
    subscribe_topic = config.get('subscribe_topic', "transaction")

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



def _convert_tranformation(df, config):
    """Based on the config, convert the stream to a dataframe based on config"""
    pass


# def _convert_transformation_str_to_action():


def process_stream(ssc, config):
    # todo: change here to implement a common logic to process the config
    kafka_params = {
        "bootstrap.servers": config['input']['kafka']['bootstrap_servers'],
        "group.id": config['input']['kafka']['group_id']
    }
    kafka_stream = KafkaUtils.createDirectStream(ssc, config['input']['kafka']['topics'], kafka_params)
    
    transaction = kafka_stream.map(lambda record: json.loads(record[1]))
    
    for kpi in config['kpis']:
        kpi_name = kpi['name']
        transformations = kpi['transformations']
        
        kpi_stream = transaction
        for transformation in transformations:
            if transformation['type'] == 'window':
                kpi_stream = kpi_stream.window(transformation['duration'])
            elif transformation['type'] == 'sum':
                kpi_stream = kpi_stream.map(lambda t: t[transformation['field']]).reduce(lambda a, b: a + b)
            elif transformation['type'] == 'distinct_count':
                kpi_stream = kpi_stream.map(lambda t: t[transformation['field']]).transform(lambda rdd: rdd.distinct().count())
            elif transformation['type'] == 'filter':
                kpi_stream = kpi_stream.filter(lambda t: eval(transformation['criteria'].replace('amount', 't["amount"]')))
            elif transformation['type'] == 'count':
                kpi_stream = kpi_stream.count()

        kpi_stream.pprint()  # Replace with code to send to Kafka


if __name__ == "__main__":
    config = load_config("config.yaml")
    spark = SparkSession.builder.appName(config['application']['name']).getOrCreate()
    ssc = StreamingContext(spark.sparkContext, 10)
    
    process_stream(ssc, config)
    
    ssc.start()
    ssc.awaitTermination()
