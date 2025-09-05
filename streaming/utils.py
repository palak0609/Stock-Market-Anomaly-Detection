import logging
import socket
import sys
import os
from confluent_kafka import Producer, Consumer

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import KAFKA_BROKER

# ------------------------------
# Logging Configuration
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def create_producer():
    try:
        producer = Producer({
            "bootstrap.servers": KAFKA_BROKER,
            "client.id": socket.gethostname(),
            "enable.idempotence": True,
            "compression.type": "lz4",
            "batch.size": 64000,
            "linger.ms": 10,
            "acks": "all",
            "retries": 5,
            "delivery.timeout.ms": 1000
        })
        logging.info("Kafka producer created.")
        return producer
    except Exception as e:
        logging.exception("❌ Couldn't create Kafka producer")
        return None


def create_consumer(topic, group_id):
    try:
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": group_id,
            "client.id": socket.gethostname(),
            "isolation.level": "read_committed",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False
        })
        consumer.subscribe([topic])
        logging.info(f"Kafka consumer created for topic: {topic}, group: {group_id}")
        return consumer
    except Exception as e:
        logging.exception("❌ Couldn't create Kafka consumer")
        return None
