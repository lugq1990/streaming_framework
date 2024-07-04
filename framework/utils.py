import yaml
import json
from uuid import uuid4
from pyspark.sql import SparkSession
import os



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
    
    

def load_yaml_config(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config



def convert_str_to_bool(obj):
    if isinstance(obj, str):
        if obj.lower() == "true":
            return True
        elif obj.lower() == "false":
            return False
    elif isinstance(obj, dict):
        for key, value in obj.items():
            obj[key] = convert_str_to_bool(value)
    elif isinstance(obj, list):
        for index, value in enumerate(obj):
            obj[index] = convert_str_to_bool(value)
    return obj


def load_config(file_name):
    config_path = os.path.join(os.path.dirname(__file__), file_name)
    
    with open(config_path, 'r') as file:
        config = json.load(file)
    return convert_str_to_bool(config)


def _get_supported_transforms(config_file_name):
    config = load_config(config_file_name)
    supported_list = []
    for k, v in config.items():
        supported_list.extend(v)
    return supported_list


def check_config_transform(config):
    """Ensures that the config is valid and contains all necessary fields for the transformer to run

    Args:
        config (json): readed config file object

    Raises:
        Exception: Not Supported Transform
    """
    spark_streaming_supported_transformers = _get_supported_transforms('spark_streaming_supported_trans.json')
    print('*' * 10)
    print(spark_streaming_supported_transformers)
    for key, value in config.items():
        if isinstance(value, dict):
            for k, v in value.items():
                if k not in spark_streaming_supported_transformers:
                    raise Exception(f"Transformer {k} is not supported in Streaming")
        elif isinstance(value, list):
            for v in value:
                tran_type = v.get('type', '')
                if tran_type not in spark_streaming_supported_transformers:
                    raise Exception(f"Transformer {tran_type} is not supported in Streaming")
    print("Config file checking pass!")
    

if __name__ == '__main__':
    # spark = SparkSessionSingleton.get_instance()
    config = load_config('sample_transform.json')
    print(config)
    check_config_transform(config)