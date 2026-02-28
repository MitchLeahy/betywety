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

DATA_PATH = f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
print(f"DATA_PATH = {DATA_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load active markets and extract token IDs from silver

# COMMAND ----------
from pyspark.sql.functions import col

silver_markets_path = f"{DATA_PATH}/silver/polymarket/markets"
df_silver = spark.read.format("delta").load(silver_markets_path)

active_markets = (
    df_silver
    .filter(col("active") == "true")
    .select("conditionId", "clobTokenIds")
    .collect()
)

if not active_markets:
    raise ValueError("No active markets found in silver/polymarket/markets. Run ingestion first.")

# Build token-ID to conditionId lookup and collect all asset IDs
token_to_condition = {}
all_asset_ids = []

for row in active_markets:
    condition_id = row["conditionId"]
    token_ids_raw = row["clobTokenIds"]
    if not token_ids_raw:
        continue
    # clobTokenIds is stored as a JSON array string, e.g. '["id1","id2"]'
    import json
    try:
        token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
    except (json.JSONDecodeError, TypeError):
        token_ids = [t.strip() for t in str(token_ids_raw).split(",") if t.strip()]

    for tid in token_ids:
        token_to_condition[tid] = condition_id
        all_asset_ids.append(tid)

print(f"Subscribing to {len(all_asset_ids)} asset IDs across {len(active_markets)} markets")
print(f"First 5 asset IDs: {all_asset_ids[:5]}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## WebSocket: connect, subscribe, and dump to bronze

# COMMAND ----------
import asyncio
import json
import time
import websockets
from pyspark.sql.functions import current_timestamp

BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
FLUSH_INTERVAL_SEC = int(dbutils.widgets.get("flush_interval_sec"))
PING_INTERVAL_SEC = 10

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
bronze_price_path = f"{DATA_PATH}/bronze/polymarket/price_updates"

# Broadcast the lookup so it's available inside flush
token_to_condition_local = dict(token_to_condition)

def flush_to_bronze(batch: list):
    """Write buffered price messages to Delta."""
    if not batch:
        return
    df = spark.createDataFrame(batch)
    df = df.withColumn("_ingestion_ts", current_timestamp())
    df.write.format("delta").mode("append").save(bronze_price_path)
    print(f"Flushed {len(batch)} price records")

# COMMAND ----------
# Subscribe in chunks if >100 token IDs to avoid oversized messages
MAX_SUB_SIZE = 100

async def stream_and_dump():
    batch = []
    last_flush = time.time()
    last_ping = time.time()

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

    try:
        async with websockets.connect(WS_URL) as ws:
            print("Connected to Polymarket WebSocket")

            for i in range(0, len(all_asset_ids), MAX_SUB_SIZE):
                chunk = all_asset_ids[i:i + MAX_SUB_SIZE]
                sub_msg = {
                    "assets_ids": chunk,
                    "type": "market",
                    "custom_feature_enabled": True,
                }
                await ws.send(json.dumps(sub_msg))
                print(f"Subscribed to chunk {i // MAX_SUB_SIZE + 1} ({len(chunk)} assets)")

            async for message in ws:
                now = time.time()

                # Heartbeat
                if now - last_ping >= PING_INTERVAL_SEC:
                    await ws.send("PING")
                    last_ping = now

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
                condition_id = token_to_condition_local.get(asset_id, "")

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
