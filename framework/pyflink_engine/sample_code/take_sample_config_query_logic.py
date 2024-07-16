import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Kafka, Json, Schema

# Load configuration
with open('config.json', 'r') as f:
    config = json.load(f)

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Set checkpointing
if 'checkpointInterval' in config:
    env.enable_checkpointing(config['checkpointInterval'])

# Create table environment
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Set up the configuration for the local cluster
kafka_params = config['input']['kafkaParams']
input_topic = kafka_params['topics'][0]

# Fetch schema dynamically
def get_kafka_schema(topic, kafka_params):
    # Placeholder for dynamic schema fetching logic
    # For simplicity, assuming schema is known
    # Implement actual logic to fetch schema from Kafka if needed
    return [
        ("transaction_id", "STRING"),
        ("amount", "DOUBLE"),
        ("customer_id", "STRING"),
        ("timestamp", "TIMESTAMP(3)")
    ]

schema_fields = get_kafka_schema(input_topic, kafka_params)

# Register Kafka source table
kafka_descriptor = (
    Kafka()
    .version("universal")
    .topic(input_topic)
    .property("bootstrap.servers", kafka_params['bootstrap.servers'])
    .property("group.id", kafka_params['group.id'])
)

json_format = Json().fail_on_missing_field(False)

schema_descriptor = Schema()
for field_name, field_type in schema_fields:
    schema_descriptor = schema_descriptor.field(field_name, field_type)

table_env.connect(kafka_descriptor).with_format(json_format).with_schema(schema_descriptor).in_append_mode().create_temporary_table("kafka_table")

# Define watermark strategy if needed
if 'watermark' in config:
    watermark_config = config['watermark']
    table_env.execute_sql(f"""
        CREATE TABLE kafka_table (
            transaction_id STRING,
            amount DOUBLE,
            customer_id STRING,
            `timestamp` TIMESTAMP(3),
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '{watermark_config['maxOutOfOrderness'] // 1000} SECOND'
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{input_topic}',
            'properties.bootstrap.servers' = '{kafka_params['bootstrap.servers']}',
            'properties.group.id' = '{kafka_params['group.id']}',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

# Execute SQL queries in sequence
for query in config['queries']:
    query_name = query['name']
    query_sql = query['sql']
    table_env.execute_sql(f"CREATE TEMPORARY VIEW {query_name} AS {query_sql}")

# Output the final result
output_config = config['output']
if output_config['type'] == 'log_to_console':
    result_table = table_env.sql_query(config['queries'][-1]['sql'])
    result_table.execute().print()

# Execute the job
env.execute(config['appName'])
