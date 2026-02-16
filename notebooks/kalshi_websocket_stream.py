# Databricks notebook source
# MAGIC %md
# MAGIC # Kalshi WebSocket Stream
# MAGIC
# MAGIC 1. Pulls active market tickers from silver/markets
# MAGIC 2. Connects to Kalshi WebSocket and subscribes to those tickers
# MAGIC 3. Buffers ticker/trade messages and appends to bronze tables

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
# MAGIC %md
# MAGIC ### Install dependencies (run once, then restart cluster)
# COMMAND ----------
# MAGIC %pip install websockets cryptography nest_asyncio
# COMMAND ----------
dbutils.widgets.text("storage_account", "stkalshiogihujuict7io", "ADLS storage account name")
dbutils.widgets.text("batch_size", "50", "Messages per batch before flush to Delta")
dbutils.widgets.text("flush_interval_sec", "30", "Max seconds between flushes")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configure ADLS

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
# MAGIC ## Load active tickers from silver/markets

# COMMAND ----------
from pyspark.sql.functions import col

silver_markets_path = f"{KALSHI_DATA_PATH}/silver/markets"

df_silver_markets = spark.read.format("delta").load(silver_markets_path)

# Filter for tradeable markets (Kalshi status: active = open for trading)
active_statuses = ["active", "open"]
market_tickers = (
    df_silver_markets
    .filter(col("status").isin(active_statuses))
    .select("ticker")
    .distinct()
    .rdd.flatMap(lambda r: [r.ticker])
    .collect()
)

if not market_tickers:
    raise ValueError("No active tickers found in silver/markets. Run bronze/silver ingestion first.")

print(f"Subscribing to {len(market_tickers)} active markets: {market_tickers[:10]}{'...' if len(market_tickers) > 10 else ''}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## WebSocket: connect, subscribe, and dump to bronze

# COMMAND ----------
import asyncio
import base64
import json
import time
import websockets
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from pyspark.sql.functions import current_timestamp

BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
FLUSH_INTERVAL_SEC = int(dbutils.widgets.get("flush_interval_sec"))

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"

bronze_ticker_path = f"{KALSHI_DATA_PATH}/bronze/ticker_snapshots"
bronze_trades_path = f"{KALSHI_DATA_PATH}/bronze/trades"

# Load PEM and API key
pem_str = dbutils.secrets.get(scope="kalshi-secrets", key="kalshi-private-key")
api_key_id = dbutils.secrets.get(scope="kalshi-secrets", key="kalshi-api-key")
private_key = serialization.load_pem_private_key(pem_str.encode(), password=None)

def sign_pss_text(private_key, text: str) -> str:
    message = text.encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")

def create_ws_headers(private_key, method: str, path: str) -> dict:
    timestamp = str(int(time.time() * 1000))
    msg_string = timestamp + method + path.split("?")[0]
    signature = sign_pss_text(private_key, msg_string)
    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }

def flush_to_bronze(ticker_batch: list, trade_batch: list):
    """Write buffered messages to Delta (run on main thread for Spark)."""
    if ticker_batch:
        df = spark.createDataFrame(ticker_batch)
        df = df.withColumn("_ingestion_ts", current_timestamp())
        df.write.format("delta").mode("append").save(bronze_ticker_path)
        print(f"Flushed {len(ticker_batch)} ticker records")
    if trade_batch:
        df = spark.createDataFrame(trade_batch)
        df = df.withColumn("_ingestion_ts", current_timestamp())
        df.write.format("delta").mode("append").save(bronze_trades_path)
        print(f"Flushed {len(trade_batch)} trade records")

# COMMAND ----------
# Run this cell to start streaming (Ctrl+C to stop)
async def stream_and_dump():
    ticker_batch = []
    trade_batch = []
    last_flush = time.time()

    def maybe_flush(force=False):
        nonlocal ticker_batch, trade_batch, last_flush
        now = time.time()
        should_flush = (
            force
            or len(ticker_batch) + len(trade_batch) >= BATCH_SIZE
            or (now - last_flush >= FLUSH_INTERVAL_SEC and (ticker_batch or trade_batch))
        )
        if should_flush and (ticker_batch or trade_batch):
            to_flush_ticker = list(ticker_batch)
            to_flush_trade = list(trade_batch)
            ticker_batch.clear()
            trade_batch.clear()
            last_flush = now
            if force:
                flush_to_bronze(to_flush_ticker, to_flush_trade)
            else:
                loop = asyncio.get_event_loop()
                loop.run_in_executor(None, lambda t=to_flush_ticker, tr=to_flush_trade: flush_to_bronze(t, tr))

    ws_headers = create_ws_headers(private_key, "GET", WS_PATH)

    try:
        async with websockets.connect(WS_URL, additional_headers=ws_headers) as websocket:
            print("Connected to Kalshi WebSocket")

            # Subscribe to ticker and trade for our markets only
            subscribe_msg = {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["ticker", "trade"],
                    "market_tickers": market_tickers,
                },
            }
            await websocket.send(json.dumps(subscribe_msg))
            print(f"Subscribed to ticker + trade for {len(market_tickers)} markets")

            async for message in websocket:
                data = json.loads(message)
                msg_type = data.get("type")
                msg = data.get("msg", {})

                if msg_type == "subscribed":
                    print(f"Subscription confirmed: {data}")
                elif msg_type == "ticker":
                    ticker_batch.append(msg)
                    maybe_flush()
                elif msg_type == "trade":
                    trade_batch.append(msg)
                    maybe_flush()
                elif msg_type == "error":
                    print(f"Error: {data}")
    finally:
        maybe_flush(force=True)

# nest_asyncio allows asyncio.run() inside Databricks/Jupyter (which already run an event loop)
import nest_asyncio
nest_asyncio.apply()
asyncio.run(stream_and_dump())
