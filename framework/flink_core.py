import json
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.udf import udf
from pyflink.common.typeinfo import Types
from utils import load_user_config, DataUtil



config = load_user_config('spark_trans.json')
source_config = config['source']['read_config']
input_topic = source_config['input_topic']
bootstrap_servers = source_config['bootstrap_servers']

schema = DataUtil._infer_kafka_data_schema(input_topic=input_topic, bootstrap_servers=bootstrap_servers)

def create_kafka_source_table(t_env, config):
    source_config = config['source']['read_config']
    input_topic = source_config['input_topic']
    bootstrap_servers = source_config['bootstrap_servers']
    
    schema = DataUtil._infer_kafka_data_schema(input_topic=input_topic, bootstrap_servers=bootstrap_servers)
    print("Fink schema from kafka: ", schema)
    
    flink_sql = f"""
        CREATE TABLE source_table (
           {schema}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{source_config['input_topic']}',
            'properties.bootstrap.servers' = '{source_config['bootstrap_servers']}',
            'properties.group.id' = '{source_config['group_id']}',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'json.ignore-parse-errors' = 'true'
        )
    """
    print("Source SQL: ", flink_sql)
    
    t_env.execute_sql(flink_sql)
    

def create_console_sink_table(t_env, config):
    sink_config = config['sink']['sink_config']
    console_query = f"""
        CREATE TABLE sink_table (
            {schema}
        ) WITH (
            'connector' = 'print'
        )
    """
    print('Sink SQL:', console_query)
    t_env.execute_sql(console_query)
    

def create_kafka_sink_table(t_env, config):
    sink_config = config['sink']['sink_config']
    # todo: should infer schema from table obj with dynamic schema
    t_env.execute_sql(f"""
        CREATE TABLE sink_table (
            {schema}
        ) WITH (
            'connector' = 'print',
            'topic' = '{sink_config['sink_topic']}',
            'properties.bootstrap.servers' = '{sink_config['bootstrap_servers']}',
            'format' = 'json',
            'sink.partitioner' = 'round-robin'
        )
    """)

def apply_transformations(table, transformations):
    for transform in transformations:
        if transform['type'] == 'filter':
            table = table.filter(f"{transform['params']['column']} {transform['params']['condition']}")
        elif transform['type'] == 'select':
            table = table.select(','.join(transform['params']['columns']))
        elif transform['type'] == 'withColumn':
            table = table.add_columns(f"{transform['params']['expression']} as {transform['params']['column']}")
        elif transform['type'] == 'drop':
            table = table.drop_columns(*transform['params']['columns'])
        elif transform['type'] == 'limit':
            table = table.limit(transform['params']['num'])
    return table


def add_jar_files(t_env):
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

def main(config):

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    add_jar_files(t_env=t_env)
    # kafka_connector_jar = "/Users/guangqianglu/Downloads/flink-connector-kafka-3.0.1-1.18.jar"
    # t_env.get_config().get_configuration().set_string(
    #     "pipeline.jars", f"file://{kafka_connector_jar}")
    print(t_env.get_config().get_configuration().to_dict())


    # Create source table
    create_kafka_source_table(t_env, config)

    # Create sink table
    # create_kafka_sink_table(t_env, config)
    create_console_sink_table(t_env, config)

    # Read from source
    source_table = t_env.from_path('source_table')

    # Apply transformations
    # result_table = apply_transformations(source_table, config['transformations'])

    # Execute queries
    print('[FLINK SQL START]:')
    for query_config in config['queries']:
        query = query_config['query']
        table_name = query_config['table_name']
        
        print('[Query]: {}'.format(query))
        t_env.create_temporary_view(table_name, source_table)
        result = t_env.sql_query(query=query)

        print("[Schema]:")
        result.print_schema()
       

    # Write to sink
    # result_table.execute_insert('sink_table').wait()
    result.execute_insert('sink_table').wait()

    # Execute the job
    t_env.execute(config['app_name'])

if __name__ == "__main__":
    
    main(config)