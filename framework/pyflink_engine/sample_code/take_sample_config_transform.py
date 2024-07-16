import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Time

# Load configuration
with open('config.json', 'r') as f:
    config = json.load(f)

# Create execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Set checkpointing
if 'checkpointInterval' in config:
    env.enable_checkpointing(config['checkpointInterval'])

# Add Kafka source
input_config = config['input']
kafka_params = input_config['kafkaParams']
consumer = FlinkKafkaConsumer(
    topics=input_config['kafkaParams']['topics'],
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_params
)
consumer.set_start_from_latest()
data_stream = env.add_source(consumer)

# Apply watermark strategy
if 'watermark' in config:
    watermark_config = config['watermark']
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Time.milliseconds(watermark_config['maxOutOfOrderness']))
    data_stream = data_stream.assign_timestamps_and_watermarks(watermark_strategy.with_timestamp_assigner(lambda x: x[watermark_config['timeAttribute']]))

# Transformations
for transformation in config['transformations']:
    t_type = transformation['type']
    params = transformation['params']

    if t_type == 'filter':
        column = params['column']
        condition = params['condition']
        data_stream = data_stream.filter(lambda x: eval(f"x['{column}'] {condition}"))

    elif t_type == 'select':
        columns = params['columns']
        data_stream = data_stream.map(lambda x: {col: x[col] for col in columns}, output_type=Types.PICKLED_BYTE_ARRAY())

    elif t_type == 'withColumn':
        column = params['column']
        expression = params['expression']
        data_stream = data_stream.map(lambda x: {**x, column: eval(expression)}, output_type=Types.PICKLED_BYTE_ARRAY())

    elif t_type == 'drop':
        columns = params['columns']
        data_stream = data_stream.map(lambda x: {k: v for k, v in x.items() if k not in columns}, output_type=Types.PICKLED_BYTE_ARRAY())

    elif t_type == 'limit':
        num = params['num']
        data_stream = data_stream.filter(lambda x, counter=[0]: (counter.append(counter.pop() + 1) or True) and counter[0] <= num)

# Output
output_config = config['output']
if output_config['type'] == 'log_to_console':
    data_stream.print()

# Execute the job
env.execute(config['appName'])
