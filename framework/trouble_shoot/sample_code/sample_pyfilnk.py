from pyflink.table import EnvironmentSettings, TableEnvironment

# Step 1: Set Up the Flink Environment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)

kafka_connector_jar = "/Users/guangqianglu/Downloads/flink-sql-connector-kafka_2.12-1.13.6.jar"
table_env.get_config().get_configuration().set_string(
    "pipeline.jars", f"file://{kafka_connector_jar}")

# Print the configurations to ensure the connector is added
print(table_env.get_config().get_configuration().to_dict())

from kafka import KafkaConsumer
import pandas as pd
import json

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
                schema[col] = "INT"
            elif dtype == 'float64':
                schema[col] = "DOUBLE"
            elif dtype == 'bool':
                schema[col] = "BOOLEAN"
            else:
                schema[col] = "STRING"
        return schema
        

# Step 2: Define Kafka Source with Table API
def create_kafka_table_source(table_env, table_name, topic, bootstrap_servers, group_id):
    schema = DataUtil._infer_kafka_data_schema(topic, bootstrap_servers)
    
    schema_ddl = ', '.join([f'`{col}` {dtype}' for col, dtype in schema.items()])

    print(schema_ddl)
    create_table_ddl = f"""
    CREATE TABLE {table_name} (
        {schema_ddl}
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id' = '{group_id}',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
    """
    print('*' * 10)
    print(create_table_ddl)
    table_env.execute_sql(create_table_ddl)

create_kafka_table_source(
    table_env=table_env,
    table_name='kafka_source',
    topic='test',
    bootstrap_servers='localhost:9092',
    group_id='testGroup'
)

# Step 3: Convert Kafka Source to Table Object
source_table = table_env.from_path('kafka_source')

source_table.execute().print()
# Step 4: Perform Transformations and Processing on the Table Object
# Example transformation: filter and add a new column
# transformed_table = source_table.filter("id > 1") \
#                                 .select("id, name, timestamp, name || '_transformed' AS transformed_name")

# # Convert the Table object to a DataStream if needed
# data_stream = table_env.to_append_stream(transformed_table)

# Print the DataStream to verify the output (for debugging purposes)
# |