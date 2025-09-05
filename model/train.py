import os
import logging
import yfinance as yf
import numpy as np
from joblib import dump
from sklearn.ensemble import IsolationForest
import pandas as pd
import matplotlib.pyplot as plt


# ------------------------------
# Logging Configuration
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/model_train.log"),
        logging.StreamHandler()
    ]
)

# Config
STOCK_SYMBOL = "AAPL"
MODEL_PATH = os.path.abspath("model/isolation_forest.joblib")

def fetch_historical_data(symbol="AAPL", period="5d", interval="1m"):
    """
    This function:
        Uses yfinance.download() to pull 1-minute stock data for the last 5 days
        Returns a DataFrame with price and volume info
    """
    logging.info(f"Fetching historical data for {symbol}...")
    try:
        data = yf.download(tickers=symbol, period=period, interval=interval, progress=False)
        return data
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return None

def preprocess_data(data):
    """
    This function:
        Selects only the Open and Close columns (2D input)
        Drops missing rows
        Converts the data into a rounded NumPy array
    """
    if data is None or data.empty:
        logging.critical("No data to train on.")
        return None
    
    clean_data = data[["Open", "Close"]].dropna()
    clean_data = clean_data.values
    clean_data = np.round(clean_data, 3)

    logging.info(f"Training on {clean_data.shape[0]} samples.")
    return clean_data


# ---------------
# Stock Analytics
# ---------------
def stock_analysis(data, symbol="AAPL"):
    logging.info(f"Running analytics for {symbol}...")

    # Returns
    returns = data['Close'].pct_change().dropna()

    # Ensure scalar floats
    mean_return = float(returns.mean()) * 252
    volatility = float(returns.std()) * np.sqrt(252)

    # Sharpe Ratio
    rf = 0.03
    sharpe_ratio = (mean_return - rf) / volatility if volatility > 0 else np.nan

    logging.info(f"{symbol} Annual Return: {mean_return:.4f}")
    logging.info(f"{symbol} Volatility: {volatility:.4f}")
    logging.info(f"{symbol} Sharpe Ratio: {sharpe_ratio:.4f}")

    # Value at Risk (95%)
    var_95 = np.percentile(returns, 5)
    logging.info(f"{symbol} 95% VaR: {var_95:.4f}")

    # ---- Monte Carlo Simulation ----
    num_days = 252
    num_sims = 1000
    simulations = np.random.normal(mean_return / 252, volatility / np.sqrt(252), (num_days, num_sims))
    sim_df = pd.DataFrame(simulations).cumsum()

    # Compute percentiles of paths
    p5 = sim_df.quantile(0.05, axis=1)
    p50 = sim_df.quantile(0.5, axis=1)
    p95 = sim_df.quantile(0.95, axis=1)

    # Plot fan chart of simulated returns
    plt.figure(figsize=(12,6))
    plt.plot(p50, label="Median Path", color="blue")
    plt.fill_between(range(num_days), p5, p95, color="lightblue", alpha=0.5, label="5%-95% range")
    plt.title(f"Monte Carlo Simulation of {symbol} Returns\nSharpe Ratio: {sharpe_ratio:.2f}")
    plt.xlabel("Days")
    plt.ylabel("Cumulative Return")
    plt.legend()
    plt.grid(True)
    plt.show()

    # Final outcomes distribution
    final_returns = sim_df.iloc[-1, :]
    p5_final = np.percentile(final_returns, 5)
    p50_final = np.percentile(final_returns, 50)
    p95_final = np.percentile(final_returns, 95)

    plt.figure(figsize=(10,5))
    plt.hist(final_returns, bins=50, color="skyblue", edgecolor="black")
    plt.axvline(p5_final, color="red", linestyle="--", label=f"5%: {p5_final:.2f}")
    plt.axvline(p50_final, color="blue", linestyle="--", label=f"Median: {p50_final:.2f}")
    plt.axvline(p95_final, color="green", linestyle="--", label=f"95%: {p95_final:.2f}")
    plt.title(f"Distribution of Final Simulated Returns ({symbol})")
    plt.xlabel("Cumulative Return after 1 Year")
    plt.ylabel("Frequency")
    plt.legend()
    plt.grid(True)
    plt.show()

def train_model(X):
    """
    Trains Isolation Forest model, Fits it on the 2D [Open, Close] data and saves it.
    """
    clf = IsolationForest(
        n_estimators=100,
        max_samples='auto',
        contamination=0.01,
        random_state=42
    )

    clf.fit(X)
    dump(clf, MODEL_PATH)
    logging.info(f"Model saved to: {MODEL_PATH}")

if __name__ == "__main__":
    if not os.path.exists("logs"):
        os.makedirs("logs")
    if not os.path.exists("model"):
        os.makedirs("model")

    df = fetch_historical_data(STOCK_SYMBOL)
    X_train = preprocess_data(df)
    if X_train is not None:
        train_model(X_train)
