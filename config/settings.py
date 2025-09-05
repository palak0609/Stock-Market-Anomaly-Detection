import os
from os.path import join, dirname
from dotenv import load_dotenv

# Loading .env
dotenv_path = join(dirname(__file__), '..', '.env')
if not os.path.exists(dotenv_path):
    raise FileNotFoundError(f"⚠️ .env file not found at {dotenv_path}")

load_dotenv(dotenv_path)

# Fetching environment variables
SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#anomalies-alerts")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
STOCK_SYMBOL = os.environ.get("STOCK_SYMBOL", "AAPL")


# calling yfinance once every minute. Hence the delay is set to 60s to match interval.
DELAY = 60
NUM_PARTITIONS = 1
TRANSACTIONS_TOPIC = "transactions"
TRANSACTIONS_CONSUMER_GROUP = "transactions"
ANOMALIES_TOPIC = "anomalies"
ANOMALIES_CONSUMER_GROUP = "anomalies"


# Validations
if not SLACK_API_TOKEN:
    raise EnvironmentError("❌ SLACK_API_TOKEN is missing from .env")

if not SLACK_CHANNEL.startswith("#"):
    raise ValueError("❌ SLACK_CHANNEL must begin with '#'")

if not KAFKA_BROKER:
    raise EnvironmentError("❌ KAFKA_BROKER is not set in .env")