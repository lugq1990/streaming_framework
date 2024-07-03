import yaml
import json
from uuid import uuid4
from pyspark.sql import SparkSession


spark = None


def load_yaml_config(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


def load_json_config(config_path):
    with open(config_path, 'r') as file:
        config = json.loads(file.read())
    return config


def load_spark(app_name='streaming_kafka'):
    global spark
    
    if not spark:
        if not app_name:
            app_name = "Kafka-Spark" + uuid4().hex[-4:]
        spark = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()
    return spark


if __name__ == '__main__':
    spark = load_spark(app_name='streaming_kafka')