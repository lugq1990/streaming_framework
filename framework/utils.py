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
from pyflink.table.types import DataTypes as FlinkDataTypes 
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

base_config_path = os.path.join(os.path.dirname(__file__), 'config')

def add_pyflink_jar_files(t_env):
    # Assuming the JARs are in a 'lib' folder in the current directory
    current_dir = os.path.dirname(os.path.realpath(__file__))
    jar_files = [
        f"file://{current_dir}/lib/flink-sql-connector-kafka-3.0.1-1.18.jar",
        f"file://{current_dir}/lib/kafka-clients-3.4.1.jar",
        f"file://{current_dir}/lib/flink-json-1.18.0.jar"
    ]
    t_env.get_config().get_configuration().set_string(
            "pipeline.jars", ';'.join(jar_files)
        )
    
    
def apply_config(t_env, config):
    configuration = t_env.get_config().get_configuration()
    
    for section in ['streaming', 'resources', 'high_availability']:
        if section not in config: continue
        for key, value in config[section].items():
            if isinstance(value, bool):
                configuration.set_boolean(key, value)
            elif isinstance(value, int):
                configuration.set_integer(key, value)
            elif isinstance(value, float):
                configuration.set_float(key, value)
            else:
                configuration.set_string(key, str(value))

def get_flink_t_env():
    """Flink config setting should happen here!

    Returns:
        _type_: _description_
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    flink_config = load_config('flink_config')
    
    # add with jars and apply the default config for HA
    add_pyflink_jar_files(t_env=t_env)
    apply_config(t_env=t_env, config=flink_config)
    
    print(t_env.get_config().get_configuration().to_dict())
    return t_env


class DataUtil:
    def __init__(self) -> None:
        pass
    
    @staticmethod
    def _get_one_kafka_record(topic_name, bootstrap_servers, group_id=None, auto_offset_reset='earliest'):
        if not group_id:
            group_id = 'read_one_record'
            
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset, 
            enable_auto_commit=False )
        # todo: should change for better solution.
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
    def escape_sql_keywords(columns):
        """flink sql has some key words that should be escaped with ``

        Args:
            columns (_type_): _description_

        Returns:
            _type_: _description_
        """
        sql_keywords = {
            "add", "all", "alter", "and", "any", "as", "asc", "between", "by", "case", 
            "cast", "check", "column", "constraint", "create", "current_date", 
            "current_time", "current_timestamp", "date", "delete", "desc", "distinct", 
            "drop", "else", "end", "exists", "false", "for", "from", "group", "having", 
            "in", "inner", "insert", "interval", "is", "join", "left", "like", "limit", 
            "not", "null", "on", "or", "order", "outer", "primary", "references", 
            "right", "select", "set", "table", "then", "to", "true", "union", "unique", 
            "update", "using", "values", "when", "where", "timestamp"
        }

        escaped_columns = []
        for col in columns:
            col_name, col_type = col.split()
            if col_name.lower() in sql_keywords:
                col_name = f'`{col_name}`'
            escaped_columns.append(f"{col_name} {col_type}")
        
        return escaped_columns


    @staticmethod
    def _infer_kafka_data_schema(input_topic, bootstrap_servers, group_id=None, return_engine='flink', auto_offset_reset='earliest'):
        # todo: for spark and pyflink schema is different, change it.
        kafka_record = DataUtil._get_one_kafka_record(input_topic, bootstrap_servers, group_id=group_id, auto_offset_reset=auto_offset_reset)
        if not kafka_record:
            print("Couldn't get one record from kafka topic: {}".format(input_topic))
            return None
        else:
            print("Good: get one record from kafka, now to infer schema.")

        # based on record to get value, and it's schema
        record_json = json.loads(kafka_record)
        # sometimes the record will contain kv as value is the key for json.
        if 'value' in record_json:
            value_json = record_json['value']
        else:
            value_json = record_json
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
            
            column_list = ['{} {}'.format(col, col_type) for col,col_type in schema.items()]
            column_list = DataUtil.escape_sql_keywords(columns=column_list)
            schema = ', '.join(column_list)
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
        

def get_spark_session(config):
    return SparkSessionSingleton.get_spark_instance(user_config=config)


# def get_streaming_context(batchDuration=10):
#     return SparkSessionSingleton.get_spark_streaming_instance(batchDuration=batchDuration)


class SparkSessionSingleton(object):
    _spark_instance = None
    _streaming_instance = None
    
    @staticmethod
    def _get_checkpoint_path(app_name):
        spark_config_key = "spark.streaming.checkpointLocation"
        spark_config = load_config('spark_config')
        # hdfs or local path
        default_checkpoiont_path = spark_config.get(spark_config_key)
        if not default_checkpoiont_path:
            default_checkpoiont_path = '/tmp/checkpoint'
            os.makedirs(default_checkpoiont_path, exist_ok=True)
            
        print("checkpoint path: {}".format(default_checkpoiont_path))
        print('&' * 100)
        return os.path.join(default_checkpoiont_path, app_name)
    
    @staticmethod
    def get_spark_instance(user_config):
        """Init spark session based on default config and user provide config, 
            user config if provided,
            then should be just key-value for streaming settings.

        Args:
            user_config (_type_, optional): _description_. Should be provided,
            as for each application should have it's own checkpoint folder path, if user need to re-start app, 
            then should re-load checkpoint folder and re-init spark session.

        Returns:
            _type_: _description_
        """
        if not SparkSessionSingleton._spark_instance:
            # based on default config file to init spark session, here could also do some overwrite from the user provide config.
            builder =  SparkSession \
                .builder 
                       
            # first based on the default config, read it
            default_config = load_config('spark_config.json')

            for stream_key, value in default_config.items():
                print("Set config for : {}".format(stream_key))
                for k, v in value.items():
                    builder = builder.config(k, v)
               
            # HERE means that user could overwrite some predefined config, 
            # todo: but should check first that only some of them are supported 
            if user_config:
                app_name = user_config.get('app_name', 'Sparkstreaming')
                app_config = user_config.get('app_config')
                # user provide app config that to setup the cluster running status.
                if app_config:
                    for k, v in app_config.items():
                        print("Set config for: {}".format(stream_key))
                        builder = builder.config(k, v)
                else:
                    print('No app config provided')
                    master =  'local[*]'
            else:
                # user should provide a app_name config, otherwise how to create checkpoint?
                app_name = 'SparkSessionSingleton'
                master =   'local[*]'
                       
            # TODO: here for the jars could be provided by user.
            # enable checkpoint, if not exist, then just create, or will re-load from checkpoint folder.
            checkpoint_dir = SparkSessionSingleton._get_checkpoint_path(app_name=app_name)
            SparkSessionSingleton._spark_instance = builder.appName(app_name) \
                .master(master) \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
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


def load_config(file_name, config_folder='framework_config'):
    if not file_name.endswith('.json'):
        file_name = f'{file_name}.json'
    config_path = os.path.join(base_config_path, config_folder, file_name)
    
    with open(config_path, 'r') as file:
        config = json.load(file)
    return convert_str_to_bool(config)


def load_user_config(file_name, config_folder='user_config'):
    if not file_name.endswith('.json'):
        file_name += '.json'
    config_path = os.path.join(base_config_path, config_folder, file_name)
    
    with open(config_path, 'r') as file:
        config  = json.load(file)
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
    
    
    

def convert_flink_table_data_type_to_sql_type(data_type):
    """Convert flink table schmea to real string for query side that dynamic inference.

    Args:
        data_type (_type_): _description_

    Returns:
        _type_: _description_
    """
    if data_type == 'VARCHAR':
        return "STRING"  
    else:
        return data_type
    if isinstance(data_type, FlinkDataTypes.CHAR):
        return f"CHAR({data_type.length})"
    elif isinstance(data_type, FlinkDataTypes.VARCHAR):
        return "STRING"  
    elif isinstance(data_type, FlinkDataTypes.BOOLEAN):
        return "BOOLEAN"
    elif isinstance(data_type, FlinkDataTypes.TINYINT):
        return "TINYINT"
    elif isinstance(data_type, FlinkDataTypes.SMALLINT):
        return "SMALLINT"
    elif isinstance(data_type, FlinkDataTypes.INT):
        return "INT"
    elif isinstance(data_type, FlinkDataTypes.BIGINT):
        return "BIGINT"
    elif isinstance(data_type, FlinkDataTypes.FLOAT):
        return "FLOAT"
    elif isinstance(data_type, FlinkDataTypes.DOUBLE):
        return "DOUBLE"
    elif isinstance(data_type, FlinkDataTypes.DECIMAL):
        return f"DECIMAL({data_type.precision}, {data_type.scale})"
    elif isinstance(data_type, FlinkDataTypes.DATE):
        return "DATE"
    elif isinstance(data_type, FlinkDataTypes.TIME):
        return "TIME"
    elif isinstance(data_type, FlinkDataTypes.TIMESTAMP):
        return "TIMESTAMP"
    elif isinstance(data_type, FlinkDataTypes.ARRAY):
        element_type = convert_flink_table_data_type_to_sql_type(data_type.element_type)
        return f"ARRAY<{element_type}>"
    elif isinstance(data_type, FlinkDataTypes.MAP):
        key_type = convert_flink_table_data_type_to_sql_type(data_type.key_type)
        value_type = convert_flink_table_data_type_to_sql_type(data_type.value_type)
        return f"MAP<{key_type}, {value_type}>"
    elif isinstance(data_type, FlinkDataTypes.ROW):
        field_types = [convert_flink_table_data_type_to_sql_type(ft) for ft in data_type.field_types]
        return f"ROW<{', '.join(field_types)}>"
    else:
        return data_type


if __name__ == '__main__':
    spark = get_spark_session()
