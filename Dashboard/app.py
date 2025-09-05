import os
import json
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import datetime
import time
from confluent_kafka import Consumer
from joblib import load
from config.settings import KAFKA_BROKER, TRANSACTIONS_TOPIC, ANOMALIES_TOPIC


# -----------------------------
# Configuration
# -----------------------------
MODEL_PATH = os.path.abspath("model/isolation_forest.joblib")

# -----------------------------
# Kafka Consumer Helpers
# -----------------------------
def get_consumer(topic, group_id="streamlit-dashboard"):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([topic])
    return consumer

@st.cache_data(show_spinner=False)
def fetch_transactions(max_records=100):
    consumer = get_consumer(TRANSACTIONS_TOPIC)
    messages = []
    start_time = time.time()
    timeout = 5  # seconds

    while time.time() - start_time < timeout and len(messages) < max_records:
        msg = consumer.poll(timeout=0.5)
        if msg is None or msg.error():
            continue
        try:
            messages.append(json.loads(msg.value().decode("utf-8")))
        except:
            continue

    consumer.close()
    return messages

def fetch_messages(consumer, max_records=100):
    messages = []
    for _ in range(max_records):
        msg = consumer.poll(timeout=0.5)
        if msg is None or msg.error():
            continue
        try:
            messages.append(json.loads(msg.value().decode("utf-8")))
        except:
            continue
    return messages


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="📊 Stock Anomaly Dashboard", layout="wide")

st.markdown("<h1 style='text-align: center;'>📉 AAPL Real-Time Anomaly Detection</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; font-size: 18px;'>Powered by Kafka, Isolation Forest, and Slack Alerts</p>", unsafe_allow_html=True)
st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs(["📈 Live Transactions", "🚨 Anomalies", "📊 Insights", "🧠 Model"])

# -----------------------------
# Tab 1: Live Transactions
# -----------------------------
with tab1:
    st.subheader("Recent Transactions")

    # Manual refresh for new data
    if st.button("🔁 Fetch New Transactions"):
        st.cache_data.clear()
        st.experimental_rerun()


    new_records = fetch_transactions(max_records=200)


    # Ensure session state persists across refreshes
    if "all_transactions" not in st.session_state:
        st.session_state.all_transactions = []


    existing_ids = {r["id"] for r in st.session_state.all_transactions}
    for r in new_records:
        if r["id"] not in existing_ids:
            st.session_state.all_transactions.append(r)

    records = st.session_state.all_transactions

    if records:
        df = pd.DataFrame(records)
        df["time"] = pd.to_datetime(df["timestamp"])
        df["open"] = df["data"].apply(lambda x: x[0])
        df["close"] = df["data"].apply(lambda x: x[1])

        with st.expander("📋 Full Transaction Table", expanded=True):
            st.dataframe(
                df[["id", "stock", "time", "open", "close", "volume"]].sort_values("time", ascending=False),
                use_container_width=True,
                height=400
            )

        st.plotly_chart(
            px.line(df, x="time", y=["open", "close"], title="Stock Price (Open vs Close)", markers=True),
            use_container_width=True
        )
    else:
        st.info("⏳ Waiting for transaction data...")

# -----------------------------
# Tab 2: Anomalies
# -----------------------------
with tab2:
    st.subheader("🚨 Detected Anomalies")

    anomaly_consumer = get_consumer(ANOMALIES_TOPIC)
    anomalies = fetch_messages(anomaly_consumer, max_records=100)

    if anomalies:
        adf = pd.DataFrame(anomalies)
        adf["time"] = pd.to_datetime(adf["timestamp"])
        adf["open"] = adf["data"].apply(lambda x: x[0])
        adf["close"] = adf["data"].apply(lambda x: x[1])

        with st.expander("📋 Anomaly Records", expanded=True):
            st.dataframe(
                adf[["id", "stock", "time", "open", "close", "volume", "score"]].sort_values("time", ascending=False),
                use_container_width=True,
                height=400
            )

        st.plotly_chart(
            px.line(adf, x="time", y="score", title="Anomaly Score Over Time", markers=True, color_discrete_sequence=["crimson"]),
            use_container_width=True
        )

        st.metric("Total Anomalies", len(adf))
        st.metric("Worst Anomaly Score", round(adf["score"].min(), 3))
        st.metric("Latest Anomaly Time", adf["time"].max().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        st.success("No anomalies detected yet.")

# -----------------------------
# Tab 3: Insights & KPIs
# -----------------------------
with tab3:
    st.subheader("📊 Insights")

    if not records:
        st.warning("No data to compute insights.")
    else:
        df["volatility"] = (df["open"] - df["close"]).abs()

        latest = df.sort_values("time").iloc[-1]
        st.metric("📈 Latest Price", f"${latest['close']:.2f}")
        st.metric("📦 Latest Volume", f"{latest['volume']:,}")

        st.markdown("### 🔍 Volume Analysis")
        volume_stats = {
            "Mean Volume": int(df["volume"].mean()),
            "Max Volume": int(df["volume"].max()),
            "Min Volume": int(df["volume"].min()),
            "Std Dev": int(df["volume"].std())
        }
        st.json(volume_stats)

        st.plotly_chart(
            px.histogram(df, x="volume", nbins=30, title="Volume Distribution", color_discrete_sequence=["teal"]),
            use_container_width=True
        )

        st.markdown("### 📊 Price Volatility (Open - Close)")
        st.plotly_chart(
            px.line(df, x="time", y="volatility", title="Price Volatility Over Time", color_discrete_sequence=["orange"]),
            use_container_width=True
        )

# -----------------------------
# Tab 4: Model Info
# -----------------------------
with tab4:
    st.subheader("🧠 Isolation Forest Model Metadata")

    if not os.path.exists(MODEL_PATH):
        st.error(f"❌ Model file not found at {MODEL_PATH}. Please run train.py to generate it.")
    else:
        try:
            model = load(MODEL_PATH)
            st.success("Model loaded successfully.")

            model_info = {
                "Contamination": model.contamination,
                "n_estimators": model.n_estimators,
                "max_samples": model.max_samples,
                "features": ["Open", "Close"],
                "Random State": model.random_state
            }

            st.json(model_info)

            st.markdown("#### 💡 Detection Logic")
            st.write("Anomaly is flagged when Isolation Forest predicts `-1` based on Open & Close price deviation from trained norms.")
            st.write("Each anomaly is scored using `clf.score_samples()` — the lower the score, the more anomalous the point.")

        except Exception as e:
            st.error(f"Model loading failed: {e}")
