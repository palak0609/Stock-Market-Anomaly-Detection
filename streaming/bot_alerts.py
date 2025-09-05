import json
import logging
import sys
import os
from time import sleep
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    SLACK_API_TOKEN,
    SLACK_CHANNEL,
    ANOMALIES_TOPIC,
    ANOMALIES_CONSUMER_GROUP
)
from utils import create_consumer, create_producer

# ------------------------------
# Logging Configuration
# ------------------------------
if not os.path.exists("logs"):
    os.makedirs("logs")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/bot_alerts.log"),
        logging.StreamHandler()
    ]
)

# Initialize Slack client
client = WebClient(token=SLACK_API_TOKEN)

# Create Kafka consumer to listen for anomaly messages
consumer = create_consumer(
    topic=ANOMALIES_TOPIC,
    group_id=ANOMALIES_CONSUMER_GROUP
)

logging.info("Bot is now listening for anomalies...")

try:
    while True:
        messages = consumer.consume(num_messages=1, timeout=5.0)
        if not messages:
            continue

        for message in messages:
            if message.error():
                logging.error(f"Consumer error: {message.error()}")
                continue

            try:
                record = json.loads(message.value().decode("utf-8"))

                # Validate message format
                if "score" not in record:
                    logging.warning(f"⚠️ Skipping malformed message (missing 'score'): {record}")
                    continue

                msg_text = (
                    f":rotating_light: *Anomaly Detected!*\n"
                    f"> *Stock:* {record['stock']}\n"
                    f"> *Time:* {record['timestamp']}\n"
                    f"> *Open, Close:* {record['data']}\n"
                    f"> *Score:* {record['score']}\n"
                    f"> *Volume:* {record.get('volume', 'N/A')}"
                )

                # Retry logic for Slack delivery
                for attempt in range(3):
                    try:
                        response = client.chat_postMessage(
                            channel=SLACK_CHANNEL,
                            text=msg_text
                        )
                        logging.info(f"✅ Anomaly alert sent to Slack for ID: {record['id']}")
                        break
                    except SlackApiError as e:
                        logging.error(f"Slack attempt {attempt + 1} failed: {e.response['error']}")
                        sleep(2)

            except Exception as e:
                logging.error(f"Error processing message: {e}")

            consumer.commit()

finally:
    consumer.close()
    logging.info("Bot shut down.")
