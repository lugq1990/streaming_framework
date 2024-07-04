import yaml
import json
from uuid import uuid4
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import os
import tempfile


def get_spark_session(app_name=None):
    return SparkSessionSingleton.get_spark_instance(app_name=app_name)


def get_streaming_context(batchDuration=10):
    return SparkSessionSingleton.get_spark_streaming_instance(batchDuration=batchDuration)


class SparkSessionSingleton(object):
    _spark_instance = None
    _streaming_instance = None
    
    @staticmethod
    def get_spark_instance(app_name='sparksession', master='local[*]', enable_checkpoint=False, checkpoint_dir=None):
        if not app_name:
            app_name = 'SparkSessionSingleton' + uuid4().hex
            
        if not SparkSessionSingleton._spark_instance:
            SparkSessionSingleton._spark_instance = SparkSession \
               .builder \
               .appName(app_name) \
               .master(master) \
               .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
               .getOrCreate()
        
        if enable_checkpoint and not checkpoint_dir:
            # todo: checkpoint don't need to be in config, just in code for resistency, but should be in hdfs.
            tmp_dir = tempfile.mkdtemp()
            SparkSessionSingleton._spark_instance.option("checkpointLocation", tmp_dir)
        return SparkSessionSingleton._spark_instance
    
    @staticmethod
    def get_spark_streaming_instance(app_name='sparksession', master='local[*]', batchDuration=10):
        if not SparkSessionSingleton._spark_instance:
            _spark_instance = SparkSessionSingleton.get_spark_instance(app_name=app_name, master=master)
        else:
            _spark_instance = SparkSessionSingleton._spark_instance
            
        if not SparkSessionSingleton._streaming_instance:
            SparkSessionSingleton._streaming_instance = StreamingContext(_spark_instance.sparkContext, batchDuration=batchDuration)
        return SparkSessionSingleton._streaming_instance
    
    

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