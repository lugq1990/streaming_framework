from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json

# Configuration
KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'input_topic'
OUTPUT_TOPIC = 'output_topic'
GROUP_ID = 'flink_consumer_group'

def process_stream(input_topic, output_topic, kafka_broker, group_id):
    env = StreamExecutionEnvironment.get_execution_environment()

    # Set Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        input_topic,
        SimpleStringSchema(),
        {'bootstrap.servers': kafka_broker, 'group.id': group_id}
    )

    # Set Kafka producer
    kafka_producer = FlinkKafkaProducer(
        output_topic,
        SimpleStringSchema(),
        {'bootstrap.servers': kafka_broker}
    )

    # Define the data processing pipeline
    input_stream = env.add_source(kafka_consumer)

    def process_data(value):
        data = json.loads(value)
        # Example transformation: incrementing a count field
        if 'count' in data:
            data['count'] += 1
        return json.dumps(data)

    processed_stream = input_stream.map(process_data, output_type=Types.STRING())

    processed_stream.add_sink(kafka_producer)

    env.execute("Kafka Streaming Job")

if __name__ == "__main__":
    process_stream(INPUT_TOPIC, OUTPUT_TOPIC, KAFKA_BROKER, GROUP_ID)
