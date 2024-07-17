import yaml
import json
from uuid import uuid4
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import os
import tempfile



def get_spark_session():
    return SparkSessionSingleton.get_spark_instance()


# def get_streaming_context(batchDuration=10):
#     return SparkSessionSingleton.get_spark_streaming_instance(batchDuration=batchDuration)


class SparkSessionSingleton(object):
    _spark_instance = None
    _streaming_instance = None
    
    @staticmethod
    def get_spark_instance(user_config=None):
        """Init spark session based on default config and user provide config, 
            user config if provided,
            then should be just key-value for streaming settings.

        Args:
            user_config (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        if not SparkSessionSingleton._spark_instance:
            # based on default config file to init spark session, here could also do some overwrite from the user provide config.
            builder =  SparkSession \
                .builder \
                       
            # first based on the default config, read it
            default_config = load_config('spark_streaming_config.json')

            for stream_key, value in default_config.items():
                print("Set config for : {}".format(stream_key))
                for k, v in value.items():
                    builder = builder.config(k, v)
                
            if user_config:
                app_name = user_config.get('app_name', None)
                # where to run the spark
                master = user_config.get('master', 'local[*]')
                for k, v in user_config.items():
                    builder = builder.config(k, v)
            else:
                app_name = 'SparkSessionSingleton' + uuid4().hex  
                master =   'local[*]'
                       
            # TODO: here for the jars could be provided by user.
            SparkSessionSingleton._spark_instance = builder.appName(app_name) \
                .master(master) \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .getOrCreate()
               
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


def load_config(file_name, config_folder='config'):
    base_path = os.path.dirname(__file__)
    config_path = os.path.join(base_path, config_folder, file_name)
    
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
    spark = get_spark_session()
