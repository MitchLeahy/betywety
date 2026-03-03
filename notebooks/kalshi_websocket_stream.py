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
dbutils.widgets.text("batch_size", "50", "Messages per batch before flush to Delta")
dbutils.widgets.text("flush_interval_sec", "30", "Max seconds between flushes")
dbutils.widgets.text("refresh_interval_sec", "300", "Seconds between subscription refreshes from silver")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ADLS path (auth via Unity Catalog external location)

# COMMAND ----------
KALSHI_DATA_PATH = "abfss://kalshi-data@stkalshiogihujuict7io.dfs.core.windows.net"
print(f"KALSHI_DATA_PATH = {KALSHI_DATA_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load active tickers from silver/markets

# COMMAND ----------
from pyspark.sql.functions import col

silver_markets_path = f"{KALSHI_DATA_PATH}/silver/markets"

def load_active_tickers() -> set:
    """Re-read silver to get current active market tickers."""
    df = spark.read.format("delta").load(silver_markets_path)
    active_statuses = ["active", "open"]
    rows = (
        df.filter(col("status").isin(active_statuses))
        .select("ticker")
        .distinct()
        .collect()
    )
    return {r.ticker for r in rows}

market_tickers = load_active_tickers()

if not market_tickers:
    raise ValueError("No active tickers found in silver/markets. Run bronze/silver ingestion first.")

print(f"Initial subscription: {len(market_tickers)} active markets")

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
REFRESH_INTERVAL_SEC = int(dbutils.widgets.get("refresh_interval_sec"))

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
# MAGIC %md
# MAGIC The stream periodically re-reads silver to pick up new markets and drop resolved ones.
# MAGIC Kalshi supports dynamic subscribe/unsubscribe without reconnecting.

# COMMAND ----------
async def stream_and_dump():
    global market_tickers
    ticker_batch = []
    trade_batch = []
    last_flush = time.time()
    last_refresh = time.time()
    subscribed = set()
    cmd_id = 1

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

    async def refresh_subscriptions(ws):
        nonlocal subscribed, last_refresh, cmd_id
        global market_tickers
        try:
            new_tickers = await asyncio.get_event_loop().run_in_executor(None, load_active_tickers)
            to_add = new_tickers - subscribed
            to_remove = subscribed - new_tickers

            if to_add:
                cmd_id += 1
                msg = {"id": cmd_id, "cmd": "subscribe", "params": {"channels": ["ticker", "trade"], "market_tickers": list(to_add)}}
                await ws.send(json.dumps(msg))
                print(f"Refresh: subscribed to {len(to_add)} new tickers")

            if to_remove:
                cmd_id += 1
                msg = {"id": cmd_id, "cmd": "unsubscribe", "params": {"channels": ["ticker", "trade"], "market_tickers": list(to_remove)}}
                await ws.send(json.dumps(msg))
                print(f"Refresh: unsubscribed from {len(to_remove)} stale tickers")

            subscribed.clear()
            subscribed.update(new_tickers)
            market_tickers = new_tickers

            if not to_add and not to_remove:
                print(f"Refresh: no changes ({len(subscribed)} tickers)")
        except Exception as e:
            print(f"Refresh failed (will retry): {e}")
        last_refresh = time.time()

    ws_headers = create_ws_headers(private_key, "GET", WS_PATH)

    try:
        async with websockets.connect(WS_URL, additional_headers=ws_headers) as websocket:
            print("Connected to Kalshi WebSocket")

            subscribe_msg = {
                "id": cmd_id,
                "cmd": "subscribe",
                "params": {
                    "channels": ["ticker", "trade"],
                    "market_tickers": list(market_tickers),
                },
            }
            await websocket.send(json.dumps(subscribe_msg))
            subscribed.update(market_tickers)
            print(f"Subscribed to ticker + trade for {len(market_tickers)} markets")

            async for message in websocket:
                now = time.time()

                if now - last_refresh >= REFRESH_INTERVAL_SEC:
                    await refresh_subscriptions(websocket)

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

import nest_asyncio
nest_asyncio.apply()
asyncio.run(stream_and_dump())
