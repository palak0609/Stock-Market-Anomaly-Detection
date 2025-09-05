import json
import os
import logging
from joblib import load
import numpy as np
from multiprocessing import Process

from utils import create_producer, create_consumer
from config.settings import (
    TRANSACTIONS_TOPIC,
    TRANSACTIONS_CONSUMER_GROUP,
    ANOMALIES_TOPIC,
    NUM_PARTITIONS
)

# Set logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/anomaly_detector.log"),
        logging.StreamHandler()
    ]
)

# Load the model
model_path = os.path.abspath("model/isolation_forest.joblib")

def detect():
    consumer = create_consumer(
        topic=TRANSACTIONS_TOPIC,
        group_id=TRANSACTIONS_CONSUMER_GROUP
    )
    producer = create_producer()

    if not os.path.exists(model_path):
        logging.critical(f"Model file not found at {model_path}. Please run train.py first.")
        return

    try:
        clf = load(model_path)
        logging.info("✅ Model loaded successfully.")
    except Exception as e:
        logging.critical(f"❌ Failed to load model: {e}")
        return

    try:
        while True:
            try:
                messages = consumer.consume(num_messages=1, timeout=5.0)
                if not messages:
                    continue

                for message in messages:
                    if message.error():
                        logging.error(f"Consumer error: {message.error()}")
                        continue

                    try:
                        # Decode Kafka message
                        record = json.loads(message.value().decode('utf-8'))

                        # Extract the data (Open, Close)
                        input_data = np.array(record["data"]).reshape(1, -1)

                        # Predict if the transaction is an anomaly
                        prediction = clf.predict(input_data)

                        if prediction[0] == -1:
                            # Anomaly detected
                            score = clf.score_samples(input_data)[0]
                            record["score"] = round(score, 3)

                            logging.warning(f"Anomaly detected: {record}")

                            producer.produce(
                                topic=ANOMALIES_TOPIC,
                                value=json.dumps(record).encode("utf-8")
                            )
                            producer.flush()

                    except Exception as e:
                        logging.error(f"Error processing message: {e}")

            except Exception as e:
                logging.error(f"Error during detection loop: {e}")
    finally:
        consumer.close()

# Launch one process per partition
if __name__ == "__main__":
    logging.info("Starting anomaly detector processes...")
    for _ in range(NUM_PARTITIONS):
        p = Process(target=detect)
        p.start()
