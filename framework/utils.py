import yaml
import json
from uuid import uuid4
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import os
import tempfile
from kafka import KafkaConsumer
from pyspark.sql.types import *
import pandas as pd

class DataUtil:
    def __init__(self) -> None:
        pass
    
    @staticmethod
    def _get_one_kafka_record(topic_name, bootstrap_servers, group_id=None):
        if not group_id:
            group_id = 'read_one_record'
            
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest', 
            enable_auto_commit=False )
        try:
            for i, c in enumerate(consumer):
                if c is not None:
                    # this is real string in one record
                    return c.value.decode('utf-8')
                if i == 10:
                    # not sure here needed?
                    break
            print("Not get")
        finally:
            consumer.close()
            
    @staticmethod       
    def _get_value_schema(input_topic, bootstrap_servers, group_id=None):
        kafka_record = DataUtil._get_one_kafka_record(input_topic, bootstrap_servers, group_id=group_id)
        # based on record to get value, and it's schema
        record_json = json.loads(kafka_record)
        if 'value' not in record_json:
            schema_list = list(record_json.keys())
        else:
            value_json = record_json['value']
            schema_list = list(value_json.keys())
        return schema_list
    
    @staticmethod
    def _get_spark_schema(input_topic, bootstrap_servers):
        schema_list  = DataUtil._get_value_schema(input_topic, bootstrap_servers)
        spark_schema = StructType([StructField(field_name, StringType(), True) for field_name in schema_list])
        return spark_schema
    

    @staticmethod
    def _infer_kafka_data_schema(input_topic, bootstrap_servers, group_id=None, return_engine='flink'):
        # todo: for spark and pyflink schema is different, change it.
        kafka_record = DataUtil._get_one_kafka_record(input_topic, bootstrap_servers, group_id=group_id)
        if not kafka_record:
            print("Couldn't get one record from kafka topic: {}".format(input_topic))
            return None

        # based on record to get value, and it's schema
        record_json = json.loads(kafka_record)
        value_json = record_json['value']
        
        df = pd.json_normalize(value_json)
        
        if return_engine == 'flink':
            schema = {}
            for col, dtype in zip(df.columns, df.dtypes):
                if dtype == 'int64':
                    schema[col] = "INT"
                elif dtype == 'float64':
                    schema[col] = "DOUBLE"
                elif dtype == 'bool':
                    schema[col] = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    schema[col] = "TIMESTAMP"
                else:
                    schema[col] = "STRING"
            return schema
        else:
            # convert to structure type for spark
            schema = {}
            for col, dtype in zip(df.columns, df.dtypes):
                if dtype == 'int64':
                    schema[col] = IntegerType()
                elif dtype == 'float64':
                    schema[col] = DoubleType()
                elif dtype == 'bool':
                    schema[col] = BooleanType()
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    schema[col] = TimestampType()
                else:
                    schema[col] = StringType()
                    
            field_list = []
            for c, t in schema.items():
                field = StructField(c, t, True)
                field_list.append(field)
            schema = StructType(field_list) 
            return schema
        

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
