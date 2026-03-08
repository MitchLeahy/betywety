# Databricks notebook source
# MAGIC %md
# MAGIC # Polymarket Ingestion Pipeline
# MAGIC
# MAGIC Bronze: fetches events and markets via Gamma API. Silver: dedupes by conditionId/id. Live prices are maintained separately by the WebSocket stream (silver/polymarket/live_prices).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
dbutils.widgets.text("tag_id", "102114", "Polymarket tag ID (102114 = NCAAB games)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADLS path (auth via Unity Catalog external location)

# COMMAND ----------

DATA_PATH = "abfss://kalshi-data@stkalshiogihujuict7io.dfs.core.windows.net"
print(f"DATA_PATH = {DATA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch events (public API, no auth)

# COMMAND ----------

# dbutils.fs.rm("abfss://kalshi-data@stkalshiogihujuict7io.dfs.core.windows.net/bronze/polymarket/events", recurse=True)
# dbutils.fs.rm("abfss://kalshi-data@stkalshiogihujuict7io.dfs.core.windows.net/bronze/polymarket/markets", recurse=True)
# dbutils.fs.rm("abfss://kalshi-data@stkalshiogihujuict7io.dfs.core.windows.net/silver/polymarket/events", recurse=True)
# dbutils.fs.rm("abfss://kalshi-data@stkalshiogihujuict7io.dfs.core.windows.net/silver/polymarket/markets", recurse=True)
# print("Cleaned all Polymarket bronze and silver tables")

# COMMAND ----------

import sys
from pathlib import Path

project_root = Path.cwd()
if not (project_root / "src" / "polymarket").exists():
    project_root = project_root.parent
sys.path.insert(0, str(project_root))

from src.polymarket.api import fetch_events, extract_markets_from_events
from pyspark.sql.functions import current_timestamp, lit

tag_id_str = dbutils.widgets.get("tag_id").strip()
tag_id = int(tag_id_str) if tag_id_str else None

all_events = fetch_events(active=True, closed=False, tag_id=tag_id)
print(f"Fetched {len(all_events)} events (tag_id={tag_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write events to bronze

# COMMAND ----------

# Remove nested markets from events before writing (markets written separately)
# Coerce int->float to avoid DoubleType/LongType merge errors across records
SKIP_EVENT_KEYS = {"markets", "series", "categories", "collections", "tags", "chats", "templates", "eventCreators"}
events_flat = []
for e in all_events:
    event_copy = {}
    for k, v in e.items():
        if k in SKIP_EVENT_KEYS:
            continue
        if isinstance(v, int) and not isinstance(v, bool):
            v = float(v)
        event_copy[k] = v
    events_flat.append(event_copy)

non_none_keys = {k for r in events_flat for k, v in r.items() if v is not None}
for r in events_flat:
    for k in list(r.keys()):
        if k not in non_none_keys:
            del r[k]

df_events = spark.createDataFrame(events_flat)
df_events = df_events.withColumn("_ingestion_ts", current_timestamp())
df_events = df_events.withColumn("_source", lit("polymarket"))

bronze_events_path = f"{DATA_PATH}/bronze/polymarket/events"
df_events.write.format("delta").mode("append").save(bronze_events_path)
print(f"Written {df_events.count()} events to {bronze_events_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract and write markets to bronze

# COMMAND ----------

all_markets = extract_markets_from_events(all_events, active_only=True)
print(f"Extracted {len(all_markets)} active markets from {len(all_events)} events")

# COMMAND ----------

# Flatten nested objects and coerce numeric types to float for consistency
markets_flat = []
for m in all_markets:
    market_copy = {}
    for k, v in m.items():
        if k in ("events", "categories", "tags", "imageOptimized", "iconOptimized"):
            continue
        if isinstance(v, int) and not isinstance(v, bool):
            v = float(v)
        market_copy[k] = v
    markets_flat.append(market_copy)

non_none_keys = {k for r in markets_flat for k, v in r.items() if v is not None}
for r in markets_flat:
    for k in list(r.keys()):
        if k not in non_none_keys:
            del r[k]

df_markets = spark.createDataFrame(markets_flat)
df_markets = df_markets.withColumn("_ingestion_ts", current_timestamp())
df_markets = df_markets.withColumn("_source", lit("polymarket"))

bronze_markets_path = f"{DATA_PATH}/bronze/polymarket/markets"
df_markets.write.format("delta").mode("append").save(bronze_markets_path)
print(f"Written {df_markets.count()} markets to {bronze_markets_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze to Silver - Events and Markets

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

# Silver: Events (dedupe by id)
silver_events_path = f"{DATA_PATH}/silver/polymarket/events"
df_bronze_events = spark.read.format("delta").load(bronze_events_path)
w_events = Window.partitionBy("id").orderBy(col("_ingestion_ts").desc())
df_silver_events = (
    df_bronze_events
    .withColumn("_rn", row_number().over(w_events))
    .filter(col("_rn") == 1)
    .drop("_rn")
)
df_silver_events.write.format("delta").mode("overwrite").save(silver_events_path)
print(f"Silver events: {df_silver_events.count()} unique events -> {silver_events_path}")

# Silver: Markets (dedupe by conditionId)
silver_markets_path = f"{DATA_PATH}/silver/polymarket/markets"
df_bronze_markets = spark.read.format("delta").load(bronze_markets_path)
w_markets = Window.partitionBy("conditionId").orderBy(col("_ingestion_ts").desc())
df_silver_markets = (
    df_bronze_markets
    .withColumn("_rn", row_number().over(w_markets))
    .filter(col("_rn") == 1)
    .drop("_rn")
)
df_silver_markets.write.format("delta").mode("overwrite").save(silver_markets_path)
print(f"Silver markets: {df_silver_markets.count()} unique markets -> {silver_markets_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect price updates (bronze/polymarket/price_updates)
# MAGIC
# MAGIC View WebSocket data before joining to silver. Run polymarket_websocket_stream first to populate.

# COMMAND ----------

bronze_price_path = f"{DATA_PATH}/bronze/polymarket/price_updates"

try:
    df_prices = spark.read.format("delta").load(bronze_price_path)
    print("Schema:")
    df_prices.printSchema()
    print(f"\nTotal records: {df_prices.count()}")
    print("\nSample (latest first):")
    display(df_prices.orderBy(col("_ingestion_ts").desc()).limit(50))
except Exception as e:
    if "Path does not exist" in str(e) or "cannot find" in str(e).lower():
        print("bronze/polymarket/price_updates not found. Run polymarket_websocket_stream notebook first.")
    else:
        raise

# COMMAND ----------

display(df_events)

# COMMAND ----------

display(df_markets)

# COMMAND ----------

display(df_silver_markets)
