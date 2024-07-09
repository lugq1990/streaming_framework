"""The full functionality should based on pyflink table API, for datastream should be created with new class.
- init env
- get config for source kafka, sink kafka info, init source and sink table objec
- based on user provide config, run the user provide query
- support with some user defined function, with a new class for UDF, UDTF, UDAF etc.
- should provide user table schema info and table exection plan.
"""
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema
from pyflink.table.expressions import col
from typing import Dict
from uuid import uuid4
from datetime import datetime
import logging
from kafka import KafkaConsumer
import pandas as pd



class FlinkStreamingApp:
    """Core of the flink streaming app, core step:
    - based on the config to init env
    - infer schema from a sample data based on the records
    - setup the input data
    - transform data
    - output data
    """
    def _init_env(self, config):
        self.env = StreamTableEnvironment.get_execution_environment()
        # todo: here could set some more config based on the config string.
        self.env.set_parallelism(1)
        self.table_env = StreamTableEnvironment.create(self.env)
        
    def _init_log(self):
        self._logger = logging.Logger()
        
    def __init__(self, config: Dict):
        self.config = config
        self._init_env(config=config)
        self._init_log()
        self.sample_json = self.sample_kafka_data()
        self.schema = self.infer_schema(self.sample_json)
        print('*' * 100)
        print(self.schema)
        self.setup_input()
        self.apply_transformations()
        self.setup_output()

    def sample_kafka_data(self):
        # Sample some data from Kafka topic to infer schema
        # Here, for simplicity, we assume you can fetch a sample message from Kafka.
        # This part should be implemented to fetch a real sample from Kafka.
        sample_json = '{"transaction_id": "12345", "amount": 150.0, "customer_id": "cust_001", "timestamp": "2023-01-01T00:00:00Z", "description": "test transaction", "account_number": "acc_001"}'
        return sample_json

    def infer_schema(self, sample_json: str):
        # Infer schema from JSON sample
        sample_data = json.loads(sample_json)
        fields = []
        for key, value in sample_data.items():
            if isinstance(value, int):
                data_type = DataTypes.INT()
            elif isinstance(value, float):
                data_type = DataTypes.FLOAT()
            elif isinstance(value, bool):
                data_type = DataTypes.BOOLEAN()
            else:
                data_type = DataTypes.STRING()
            fields.append(DataTypes.FIELD(key, data_type))
        return DataTypes.ROW(fields)
    
    @staticmethod
    def _generate_random_str():
        uuid4 = uuid4()
        return str(uuid4)
    
    def _init_kafka_source(self, schema=None) -> str:
        """
        schema could be None, then just infer from data."""
        # todo: confirm.
        input_config = self.config.get('input_config')
        schema_ddl = ', '.join([f'`{col}` {dtype}' for col, dtype in schema.items()])
        
        # these config must be provided.
        topic = input_config['topic_name']
        bootstrap_servers = input_config['bootstrap_servers']
        
        schema = DataUtil._infer_kafka_data_schema(topic_name=topic, bootstrap_servers=bootstrap_servers)
        # these config don't need to be provided.
        # key_deserializer = input_config['key_deserializer']
        # value_deserializer = input_config['value_deserializer']
        table_name = input_config.get('table_name')
        group_id = input_config.get('group_id')
        if not table_name:
            table_name = self._generate_random_str()
        if not group_id:
            group_id = self._generate_random_str()
        
        
        create_table_ddl = f"""
                CREATE TABLE {table_name} (
                    {schema}
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = '{topic}',
                    'properties.bootstrap.servers' = '{bootstrap_servers}',
                    'properties.group.id' = '{group_id}',
                    'format' = 'json',
                    'scan.startup.mode' = 'earliest-offset'
                )
                """
        self._logger.info(f"Creating Kafka Source Table: {table_name}")
        self._logger.debug(f"DDL: {create_table_ddl}")

        return table_name

    def _init_kafka_sink(self, schema: Dict):
        output_config = self.config['output_config']
         # these config must be provided.
        topic = output_config['topic_name']
        bootstrap_servers = output_config['bootstrap_servers']
        
        schema_ddl = ', '.join([f'`{col}` {dtype}' for col, dtype in schema.items()])
        table_name = self._generate_random_str()
        
        create_table_ddl = f"""
            CREATE TABLE {table_name} (
                {schema_ddl}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{bootstrap_servers}',
                'format' = 'json',
                'sink.partitioner' = 'fixed'
            )
            """
        self.table_env.execute_sql(create_table_ddl)    
        return table_name  
        
    def _execute_table_insert(self, table):
        insert_table = self._init_kafka_source(table)
        self.table_env.execute_sql(f"INSERT INTO {insert_table} SELECT * FROM {table}")
        
    

    def apply_transformations(self):
        transformations = self.config['transformations']
        self.data_table = self.table_env.from_path("kafka_input")

        for transform in transformations:
            if transform['type'] == 'filter':
                column = transform['params']['column']
                condition = transform['params']['condition']
                self.data_table = self.data_table.filter(col(column) > int(condition.split()[1]))

            elif transform['type'] == 'select':
                columns = transform['params']['columns']
                self.data_table = self.data_table.select(*[col(c) for c in columns])

            elif transform['type'] == 'withColumn':
                column = transform['params']['column']
                expression = transform['params']['expression']
                self.data_table = self.data_table.add_columns((col("amount") * 0.15).alias(column))

            elif transform['type'] == 'drop':
                columns = transform['params']['columns']
                for column in columns:
                    self.data_table = self.data_table.drop_columns(column)

            elif transform['type'] == 'limit':
                num = transform['params']['num']
                self.data_table = self.data_table.limit(num)

    
    def run(self):
        self.env.execute(self.config['appName'])


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
                    print(c)
                    return c.value.decode('utf-8')
                if i == 10:
                    # not sure here needed?
                    break
            print("Not get")
        finally:
            consumer.close()

    @staticmethod
    def _infer_kafka_data_schema(topic_name, bootstrap_servers, group_id=None):
        kafka_record = DataUtil._get_one_kafka_record(topic_name, bootstrap_servers, group_id=group_id)
        if not kafka_record:
            print("Couldn't get one record from kafka topic: {}".format(topic_name))
            return None

        # otherwise try to get the data
        df = pd.json_normalize(json.loads(kafka_record))
        schema = {}
        for col, dtype in zip(df.columns, df.dtypes):
            if dtype == 'int64':
                schema[col] = DataTypes.INT()
            elif dtype == 'float64':
                schema[col] = DataTypes.DOUBLE()
            elif dtype == 'bool':
                schema[col] = DataTypes.BOOLEAN()
            else:
                schema[col] = DataTypes.STRING()
        return schema
        
        


# Example usage
if __name__ == "__main__":
    from pyflink.table import EnvironmentSettings, TableEnvironment

    # todo: here should be fixed, as couldn't get the kafka source.
    # Step 1: Set Up the Flink Environment
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(env_settings)

    kafka_connector_jar = "/Users/guangqianglu/Downloads/flink-sql-connector-kafka_2.11-1.13.6.jar"
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars", f"file://{kafka_connector_jar}")

    # Print the configurations to ensure the connector is added
    print(table_env.get_config().get_configuration().to_dict())

    config_json = '''
    {
        "appName": "MySparkStreamingApp",
        "batchInterval": 5,
        "input": {
            "type": "kafka",
            "kafkaParams": {
                "metadata.broker.list": "localhost:9092"
            },
            "topic": "input_topic"
        },
        "transformations": [
            {
                "type": "filter",
                "params": {
                    "column": "amount",
                    "condition": "> 100"
                }
            },
            {
                "type": "select",
                "params": {
                    "columns": [
                        "transaction_id",
                        "amount",
                        "customer_id",
                        "timestamp"
                    ]
                }
            },
            {
                "type": "withColumn",
                "params": {
                    "column": "amount_usd",
                    "expression": "amount * 0.15"
                }
            },
            {
                "type": "drop",
                "params": {
                    "columns": [
                        "description",
                        "account_number"
                    ]
                }
            },
            {
                "type": "limit",
                "params": {
                    "num": 100
                }
            }
        ],
        "output": {
            "type": "write_to_kafka",
            "params": {
                "bootstrapServers": "localhost:9092",
                "topic": "transaction_output",
                "key_col": "transaction_id"
            }
        }
    }
    '''
    config = json.loads(config_json)
    app = FlinkStreamingApp(config)
    app.run()
