"""This is used to generate some sample transaction data and send the data to kafka"""
import json
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# Initialize Faker
fake = Faker()

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Change this to your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    """Generate a single bank transaction record."""
    transaction = {
        'transaction_id': fake.uuid4(),
        'amount': round(random.uniform(10.0, 10000.0), 2),
        'customer_id': fake.uuid4(),
        'transaction_type': random.choice(['debit', 'credit']),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'description': fake.sentence(nb_words=5),
        'account_number': fake.bban(),
        'merchant': fake.company()
    }
    return transaction


def send_transactions_to_kafka(topic, num_transactions):
    """Generate and send multiple transactions to a Kafka topic."""
    for _ in range(num_transactions):
        transaction = generate_transaction()
        producer.send(topic, transaction)
        print(f"Sent transaction: {transaction}")

    # Block until all messages are sent
    producer.flush()


if __name__ == "__main__":
    # Configuration
    kafka_topic = 'transaction'
    number_of_transactions = 100  # Number of transactions to generate

    # Generate and send transactions
    send_transactions_to_kafka(kafka_topic, number_of_transactions)
