from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
from datetime import datetime, timedelta

# Kafka configuration
bootstrap_servers = ['localhost:9092']  # Replace with your Kafka broker address
topic_name = 'test'  # Your Kafka topic name

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Create Faker instance
fake = Faker()

def generate_fake_record():
    """Generate a fake record."""
    key = fake.uuid4()
    # with data with late arriving later
    current_time = datetime.now()
    event_time = current_time - timedelta(minutes=random.randint(0, 5))
    
    value = {
        "id": key,
        "name": fake.name(),
        "email": fake.email(),
        "date_time": fake.date_time_this_year().isoformat(),
        "country": fake.country(),
        "company": fake.company(),
        "job": fake.job(),
        "phone": fake.phone_number(),
        "sentence": fake.sentence(),
        "number": random.randint(1, 100),
        'event_timestamp': int(event_time.timestamp() * 1000)  # Unix timestamp in milliseconds
    }
    record = {"key": key, "value": value}
    return value


def send_fake_records(num_records, delay=1):
    """Send fake records to Kafka topic."""
    for _ in range(num_records):
        fake_record = generate_fake_record()
        
        # Send the record to Kafka
        future = producer.send(topic_name, value=fake_record)
        
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
            print(f"Sent record to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
        except Exception as e:
            print(f"Error sending record: {e}")
        
        # time.sleep(delay)  # Wait for specified delay

# Main execution
if __name__ == "__main__":
    num_records = 10  # Number of fake records to generate and send
    delay_between_sends = 1  # Delay in seconds between each send
    
    print(f"Sending {num_records} fake records to topic '{topic_name}'...")
    send_fake_records(num_records, delay_between_sends)
    
    print("Finished sending fake records.")
    producer.close()