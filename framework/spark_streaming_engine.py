import yaml
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid4
import json


global spark


def load_config(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


def load_spark(app_name='streaming_kafka'):
    if not app_name:
        app_name = "Kafka-Spark" + uuid4().hex[-4:]
    spark = SparkSession \
       .builder \
       .appName(app_name) \
       .getOrCreate()
    return spark


def process_stream(ssc, config):
    # todo: change here to implement a common logic to process the config
    kafka_params = {
        "bootstrap.servers": config['input']['kafka']['bootstrap_servers'],
        "group.id": config['input']['kafka']['group_id']
    }
    kafka_stream = KafkaUtils.createDirectStream(ssc, config['input']['kafka']['topics'], kafka_params)
    
    transactions = kafka_stream.map(lambda record: json.loads(record[1]))
    
    for kpi in config['kpis']:
        kpi_name = kpi['name']
        transformations = kpi['transformations']
        
        kpi_stream = transactions
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
