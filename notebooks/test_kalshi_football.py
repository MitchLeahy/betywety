# Databricks notebook source
# MAGIC %md
# MAGIC # Kalshi CBB Events - Bronze Ingestion (MVP)
# MAGIC
# MAGIC Fetches open events for a series (default: KXNCAAMBGAME) and writes to bronze layer.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
dbutils.widgets.text("series_id", "KXNCAAMBGAME", "Series ticker (e.g. KXNCAAMBGAME)")
dbutils.widgets.text("storage_account", "stkalshiogihujuict7io", "ADLS storage account name")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configure ADLS (KALSHI_DATA_PATH)

# COMMAND ----------
container = "kalshi-data"
STORAGE_ACCOUNT = dbutils.widgets.get("storage_account").strip()
TENANT_ID = "b4b203c1-e6ed-4319-a1aa-80694c9ce7e9"
CLIENT_ID = "5e532278-857e-493b-9185-ea6b714d1e42"

if not STORAGE_ACCOUNT:
    raise ValueError("Set storage_account widget")

client_secret = dbutils.secrets.get(scope="kalshi-secrets", key="sp-client-secret")

spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_ID)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")

KALSHI_DATA_PATH = f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
print(f"KALSHI_DATA_PATH = {KALSHI_DATA_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fetch events (public API, no auth)

# COMMAND ----------
import sys
from pathlib import Path

# Add project root for src imports
project_root = Path.cwd()
if not (project_root / "src" / "kalshi").exists():
    project_root = project_root.parent
sys.path.insert(0, str(project_root))

from src.kalshi.api import fetch_events, fetch_markets
from pyspark.sql.functions import current_timestamp, lit

series_id = dbutils.widgets.get("series_id")
all_events = fetch_events(series_id)

print(f"Fetched {len(all_events)} events for series {series_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to bronze

# COMMAND ----------
df_events = spark.createDataFrame(all_events)

# Add ingestion metadata
df_events = df_events.withColumn("_ingestion_ts", current_timestamp())
df_events = df_events.withColumn("_series_ticker", lit(series_id))

# Bronze path on ADLS
bronze_path = f"{KALSHI_DATA_PATH}/bronze/events"
df_events.write.format("delta").mode("append").save(bronze_path)

print(f"Written to {bronze_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fetch and write markets (public API, no auth)

# COMMAND ----------
all_markets = fetch_markets(series_id)

print(f"Fetched {len(all_markets)} markets for series {series_id}")

# COMMAND ----------
df_markets = spark.createDataFrame(all_markets)

# Add ingestion metadata
df_markets = df_markets.withColumn("_ingestion_ts", current_timestamp())
df_markets = df_markets.withColumn("_series_ticker", lit(series_id))

# Bronze path on ADLS
bronze_markets_path = f"{KALSHI_DATA_PATH}/bronze/markets"
df_markets.write.format("delta").mode("append").save(bronze_markets_path)

print(f"Written to {bronze_markets_path}")

# COMMAND ----------
display(df_events)

# COMMAND ----------
display(df_markets)

# COMMAND ----------
# MAGIC %md
# MAGIC ## WebSocket - Live ticker/trade stream

# COMMAND ----------
import asyncio
import base64
import json
import time
import websockets
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

# Load PEM and API key from Databricks secrets
pem_str = dbutils.secrets.get(scope="kalshi-secrets", key="kalshi-private-key")
api_key_id = dbutils.secrets.get(scope="kalshi-secrets", key="kalshi-api-key")

private_key = serialization.load_pem_private_key(pem_str.encode(), password=None)

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"

def sign_pss_text(private_key, text: str) -> str:
    """Sign message using RSA-PSS."""
    message = text.encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")

def create_ws_headers(private_key, method: str, path: str) -> dict:
    """Create WebSocket authentication headers."""
    timestamp = str(int(time.time() * 1000))
    msg_string = timestamp + method + path.split("?")[0]
    signature = sign_pss_text(private_key, msg_string)
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }

# COMMAND ----------
# Connect and subscribe - run this cell to start streaming (Ctrl+C to stop)
async def stream_ticker_and_trades():
    ws_headers = create_ws_headers(private_key, "GET", WS_PATH)

    async with websockets.connect(WS_URL, additional_headers=ws_headers) as websocket:
        print("Connected to Kalshi WebSocket")

        # Subscribe to ticker (prices) and trade (executed trades)
        subscribe_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker", "trade"],
            },
        }
        await websocket.send(json.dumps(subscribe_msg))
        print("Subscribed to ticker and trade channels")

        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "subscribed":
                print(f"Subscription confirmed: {data}")
            elif msg_type == "ticker":
                msg = data.get("msg", {})
                market = msg.get("market_ticker", "")
                yes_bid = msg.get("yes_bid", "")
                yes_ask = msg.get("yes_ask", "")
                print(f"Ticker {market}: Yes Bid {yes_bid}, Yes Ask {yes_ask}")
            elif msg_type == "trade":
                msg = data.get("msg", {})
                market = msg.get("market_ticker", "")
                price = msg.get("yes_price", msg.get("price", ""))
                count = msg.get("count", "")
                print(f"Trade {market}: price={price} count={count}")
            elif msg_type == "error":
                print(f"Error: {data}")

asyncio.run(stream_ticker_and_trades())
