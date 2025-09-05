import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yfinance as yf
import json
import time
from datetime import datetime
import logging

from config.settings import TRANSACTIONS_TOPIC, DELAY, STOCK_SYMBOL
from utils import create_producer

# ------------------------------
# Logging Configuration
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/producer.log"),
        logging.StreamHandler()
    ]
)

producer = create_producer()

def fetch_stock_data():
    try:
        stock = yf.Ticker(STOCK_SYMBOL)
        data = stock.history(period="1d", interval="1m")
        return data.iloc[-1]  # Get the latest 1-minute candle
    except Exception as e:
        logging.error(f"Error fetching stock data: {e}")
        return None

if producer is not None:
    _id = 0
    while True:
        latest_data = fetch_stock_data()

        if latest_data is not None:
            try:
                record = {
                    "id": _id,
                    "stock": STOCK_SYMBOL,
                    "timestamp": latest_data.name.strftime("%Y-%m-%d %H:%M:%S"),
                    "data": [
                        round(latest_data["Open"], 3),
                        round(latest_data["Close"], 3)
                    ],
                    "volume": int(latest_data["Volume"])
                }

                logging.info(f"Sending: {record}")

                producer.produce(
                    topic=TRANSACTIONS_TOPIC,
                    value=json.dumps(record).encode("utf-8")
                )
                producer.flush()
                _id += 1

            except Exception as e:
                logging.error(f"Error sending data to Kafka: {e}")

        time.sleep(DELAY)
else:
    logging.critical("Kafka producer could not be created.")
