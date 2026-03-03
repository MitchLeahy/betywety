# Databricks notebook source
# MAGIC %md
# MAGIC # Polymarket WebSocket Stream
# MAGIC
# MAGIC 1. Pulls active markets and token IDs from silver/polymarket/markets
# MAGIC 2. Connects to Polymarket CLOB WebSocket (no auth required)
# MAGIC 3. Buffers price messages and appends to bronze/polymarket/price_updates

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
# MAGIC %md
# MAGIC ### Install dependencies
# COMMAND ----------
# MAGIC %pip install websockets nest_asyncio
# COMMAND ----------
dbutils.widgets.text("batch_size", "50", "Messages per batch before flush to Delta")
dbutils.widgets.text("flush_interval_sec", "30", "Max seconds between flushes")
dbutils.widgets.text("refresh_interval_sec", "300", "Seconds between subscription refreshes from silver")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ADLS path (auth via Unity Catalog external location)

# COMMAND ----------
DATA_PATH = "abfss://kalshi-data@stkalshiogihujuict7io.dfs.core.windows.net"
print(f"DATA_PATH = {DATA_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load active markets and extract token IDs from silver

# COMMAND ----------
import json
from pyspark.sql.functions import col

silver_markets_path = f"{DATA_PATH}/silver/polymarket/markets"

def load_active_asset_ids():
    """Re-read silver to get current active token IDs and lookup map."""
    df_silver = spark.read.format("delta").load(silver_markets_path)
    rows = (
        df_silver
        .filter((col("active") == "true") | (col("active") == True))
        .filter((col("closed") == "false") | (col("closed") == False) | col("closed").isNull())
        .select("conditionId", "clobTokenIds")
        .collect()
    )
    lookup = {}
    ids = set()
    for row in rows:
        condition_id = row["conditionId"]
        raw = row["clobTokenIds"]
        if not raw:
            continue
        try:
            token_ids = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            token_ids = [t.strip() for t in str(raw).split(",") if t.strip()]
        for tid in token_ids:
            lookup[tid] = condition_id
            ids.add(tid)
    return ids, lookup

all_asset_ids, token_to_condition = load_active_asset_ids()

if not all_asset_ids:
    raise ValueError("No active markets found in silver/polymarket/markets. Run ingestion first.")

print(f"Initial subscription: {len(all_asset_ids)} asset IDs")

# COMMAND ----------
# MAGIC %md
# MAGIC ## WebSocket: connect, subscribe, refresh, and dump to bronze
# MAGIC
# MAGIC The stream periodically re-reads silver to pick up new markets and drop resolved ones
# MAGIC without reconnecting (Polymarket supports dynamic subscribe/unsubscribe).

# COMMAND ----------
import asyncio
import time
import websockets
from pyspark.sql.functions import current_timestamp

BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
FLUSH_INTERVAL_SEC = int(dbutils.widgets.get("flush_interval_sec"))
REFRESH_INTERVAL_SEC = int(dbutils.widgets.get("refresh_interval_sec"))
PING_INTERVAL_SEC = 10
MAX_SUB_SIZE = 100

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
bronze_price_path = f"{DATA_PATH}/bronze/polymarket/price_updates"

def flush_to_bronze(batch: list):
    """Write buffered price messages to Delta."""
    if not batch:
        return
    df = spark.createDataFrame(batch)
    df = df.withColumn("_ingestion_ts", current_timestamp())
    df.write.format("delta").mode("append").save(bronze_price_path)
    print(f"Flushed {len(batch)} price records")

# COMMAND ----------
async def stream_and_dump():
    global all_asset_ids, token_to_condition
    batch = []
    last_flush = time.time()
    last_ping = time.time()
    last_refresh = time.time()
    subscribed_ids = set()

    def maybe_flush(force=False):
        nonlocal batch, last_flush
        now = time.time()
        should_flush = (
            force
            or len(batch) >= BATCH_SIZE
            or (now - last_flush >= FLUSH_INTERVAL_SEC and batch)
        )
        if should_flush and batch:
            to_flush = list(batch)
            batch.clear()
            last_flush = now
            if force:
                flush_to_bronze(to_flush)
            else:
                loop = asyncio.get_event_loop()
                loop.run_in_executor(None, lambda b=to_flush: flush_to_bronze(b))

    async def subscribe_ids(ws, ids_to_sub: set):
        ids_list = list(ids_to_sub)
        for i in range(0, len(ids_list), MAX_SUB_SIZE):
            chunk = ids_list[i:i + MAX_SUB_SIZE]
            msg = {"assets_ids": chunk, "type": "market", "custom_feature_enabled": True}
            await ws.send(json.dumps(msg))

    async def unsubscribe_ids(ws, ids_to_unsub: set):
        ids_list = list(ids_to_unsub)
        for i in range(0, len(ids_list), MAX_SUB_SIZE):
            chunk = ids_list[i:i + MAX_SUB_SIZE]
            msg = {"assets_ids": chunk, "type": "market", "operation": "unsubscribe"}
            await ws.send(json.dumps(msg))

    async def refresh_subscriptions(ws):
        nonlocal subscribed_ids, last_refresh
        global all_asset_ids, token_to_condition
        try:
            new_ids, new_lookup = await asyncio.get_event_loop().run_in_executor(None, load_active_asset_ids)
            to_add = new_ids - subscribed_ids
            to_remove = subscribed_ids - new_ids
            if to_add:
                await subscribe_ids(ws, to_add)
                print(f"Refresh: subscribed to {len(to_add)} new assets")
            if to_remove:
                await unsubscribe_ids(ws, to_remove)
                print(f"Refresh: unsubscribed from {len(to_remove)} stale assets")
            subscribed_ids.clear()
            subscribed_ids.update(new_ids)
            all_asset_ids = new_ids
            token_to_condition = new_lookup
            if not to_add and not to_remove:
                print(f"Refresh: no changes ({len(subscribed_ids)} assets)")
        except Exception as e:
            print(f"Refresh failed (will retry): {e}")
        last_refresh = time.time()

    try:
        async with websockets.connect(WS_URL) as ws:
            print("Connected to Polymarket WebSocket")

            await subscribe_ids(ws, all_asset_ids)
            subscribed_ids.update(all_asset_ids)
            print(f"Subscribed to {len(subscribed_ids)} assets")

            async for message in ws:
                now = time.time()

                if now - last_ping >= PING_INTERVAL_SEC:
                    await ws.send("PING")
                    last_ping = now

                if now - last_refresh >= REFRESH_INTERVAL_SEC:
                    await refresh_subscriptions(ws)

                if message == "PONG":
                    continue

                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    continue

                event_type = data.get("event_type", "")
                if event_type not in ("price_change", "last_trade_price", "best_bid_ask"):
                    continue

                asset_id = data.get("asset_id", "")
                condition_id = token_to_condition.get(asset_id, "")

                record = {
                    "asset_id": asset_id,
                    "condition_id": condition_id,
                    "event_type": event_type,
                    "price": str(data.get("price", "")),
                    "bestBid": str(data.get("best_bid", data.get("bid", ""))),
                    "bestAsk": str(data.get("best_ask", data.get("ask", ""))),
                    "lastTradePrice": str(data.get("last_trade_price", data.get("price", ""))),
                    "timestamp": str(data.get("timestamp", "")),
                    "raw": json.dumps(data),
                }
                batch.append(record)
                maybe_flush()
    finally:
        maybe_flush(force=True)

import nest_asyncio
nest_asyncio.apply()
asyncio.run(stream_and_dump())
