# Databricks notebook source
# MAGIC %md
# MAGIC # Kalshi Markets API - Databricks
# MAGIC # 
# MAGIC Tests the Kalshi API (REST + WebSocket) for college basketball markets (KXNCAAMBGAME).
# MAGIC Uses **kalshi-secrets** scope for credentials. Run **mount_adls** notebook first if writing to Delta.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Get Unique Title Values


# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup and Imports


# COMMAND ----------
# Databricks: credentials from kalshi-secrets scope (Key Vault)
import json
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC 


# COMMAND ----------
# Get Kalshi credentials from Databricks secret scope (Key Vault)
api_key = dbutils.secrets.get(scope="kalshi-secrets", key="kalshi-api-key")
private_key_pem = dbutils.secrets.get(scope="kalshi-secrets", key="kalshi-private-key")

print(f"✓ API Key: {api_key[:8]}...{api_key[-4:]}")
print("✓ Private key: loaded from kalshi-secrets")

# COMMAND ----------
# API Configuration
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
ENDPOINT = "/markets"
URL = f"{BASE_URL}{ENDPOINT}"

print(f"Base URL: {BASE_URL}")
print(f"Endpoint: {ENDPOINT}")
print(f"Full URL: {URL}")

# COMMAND ----------
import requests
# this is how we see all the categories that we need to loop through below
url = "https://api.elections.kalshi.com/trade-api/v2/search/tags_by_categories"

response = requests.get(url)

response.text

# COMMAND ----------
import requests

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2/series"

all_series = []
cursor = None
# test run using basket ball as the tag
while True:
    params = {
        "category": "Sports",
        "tags": "Basketball"
        

    }
    if cursor:
        params["cursor"] = cursor

    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()

    data = resp.json()
    all_series.extend(data["series"])

    cursor = data.get("cursor")
    if not cursor:
        break

df_series = spark.createDataFrame(all_series)

# COMMAND ----------
display(df_series)
# Filter rows where 'title' contains 'College'
nba_only_rows = df_series.filter(F.col("title").contains("College"))
display(nba_only_rows)

# COMMAND ----------
import requests

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2/events"
# looks at the nba ticket 'NBA Team'


series_id = "KXNCAAMBGAME"  # example
all_events = []
cursor = None

while True:
    params = {
        "status": "open",
        "series_ticker": series_id,
        "limit": 200,
    }
    if cursor:
        params["cursor"] = cursor

    r = requests.get(BASE_URL, params=params)
    r.raise_for_status()
    data = r.json()

    all_events.extend(data["events"])
    cursor = data.get("cursor")
    if not cursor:
        break
event_tickers =[]
for event in all_events:
    event_tickers.append(event["event_ticker"])


all_events

# COMMAND ----------
import requests
import time

BASE = "https://api.elections.kalshi.com/trade-api/v2"

def get_markets_by_event(event_ticker: str, limit: int = 1000):
    markets = []
    cursor = None

    while True:
        
        params = {"limit": limit, "event_ticker": event_ticker}
        if cursor:
            params["cursor"] = cursor

        r = requests.get(f"{BASE}/markets", params=params)
        r.raise_for_status()
        data = r.json()

        markets.extend(data.get("markets", []))
        cursor = data.get("cursor")
        if not cursor:
            break
        

    return markets

all_markets = []
for et in event_tickers:
    all_markets.extend(get_markets_by_event(et))
    time.sleep(0.5)  # Avoid 429 Too Many Requests

df_markets = spark.createDataFrame(all_markets)
print("Total markets:", df_markets.count())

# COMMAND ----------
df_markets

# COMMAND ----------
# MAGIC %md
# MAGIC ## WebSocket (ticker_v2)
# MAGIC
# MAGIC If you get `ModuleNotFoundError: No module named 'websockets'`, run in a new cell: `%pip install websockets`

# COMMAND ----------
"""
Kalshi authenticated WebSocket stream (ticker_v2) - Databricks version.
Uses api_key and private_key_pem from kalshi-secrets scope (run credentials cell first).

Prereqs: %pip install websockets (Databricks runtime includes cryptography)
"""

import json
import time
import base64
import asyncio

import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# Credentials from kalshi-secrets (run credentials cell first)
API_KEY_ID = api_key
WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

# Market tickers from df_markets (run REST cells first)
MARKET_TICKERS = [str(r.ticker) for r in df_markets.select("ticker").na.drop().distinct().collect()]


# -------------------------
# Auth helpers
# -------------------------
def load_private_key_from_pem(pem_str: str):
    return serialization.load_pem_private_key(pem_str.encode(), password=None)


def make_ws_headers(api_key_id: str, private_key) -> dict:
    """
    Kalshi WS signature message format:
      timestamp_ms + "GET" + "/trade-api/ws/v2"
    """
    ts_ms = str(int(time.time() * 1000))
    path = "/trade-api/ws/v2"
    message = f"{ts_ms}GET{path}".encode("utf-8")

    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )

    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
    }


# -------------------------
# WebSocket client
# -------------------------
async def main():
    if MARKET_TICKERS is None or len(MARKET_TICKERS) == 0:
        raise ValueError("MARKET_TICKERS is empty. Add at least one market ticker to subscribe.")

    private_key = load_private_key_from_pem(private_key_pem)
    headers = make_ws_headers(API_KEY_ID, private_key)

    async with websockets.connect(
        WS_URL,
        additional_headers=headers,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_queue=1024,
    ) as ws:
        sub_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker_v2"],
                "market_tickers": MARKET_TICKERS,
            },
        }
        await ws.send(json.dumps(sub_msg))
        print(f"Subscribed to ticker_v2 for {len(MARKET_TICKERS)} markets.")

        while True:
            raw = await ws.recv()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            if data.get("type") == "ticker_v2":
                msg = data.get("msg", {})
                mt = msg.get("market_ticker")
                ts = msg.get("ts")
                yes_bid = msg.get("yes_bid")
                yes_ask = msg.get("yes_ask")
                last_price = msg.get("price")
                print(msg)
            else:
                # Uncomment to see acks/errors:
                # print("NON-TICKER:", data)
                pass


# In Jupyter:
await main()

# In a .py script instead:
# if __name__ == "__main__":
#     asyncio.run(main())

# COMMAND ----------