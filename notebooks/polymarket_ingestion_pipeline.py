# Databricks notebook source
# MAGIC %md
# MAGIC # Polymarket Ingestion Pipeline
# MAGIC
# MAGIC Bronze: fetches events and markets via Gamma API. Silver: dedupes and enriches with live price data from WebSocket stream.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
dbutils.widgets.text("tag_id", "100149", "Polymarket tag ID (100149 = NCAAB)")

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
events_flat = []
for e in all_events:
    event_copy = {k: v for k, v in e.items() if k not in ("markets", "series", "categories", "collections", "tags", "chats", "templates", "eventCreators")}
    events_flat.append(event_copy)

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
# Flatten nested objects from markets before writing
markets_flat = []
for m in all_markets:
    market_copy = {k: v for k, v in m.items() if k not in ("events", "categories", "tags", "imageOptimized", "iconOptimized")}
    markets_flat.append(market_copy)

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
# MAGIC %md
# MAGIC ## Update silver markets with latest prices from WebSocket stream

# COMMAND ----------
from pyspark.sql.functions import coalesce

try:
    df_prices = spark.read.format("delta").load(bronze_price_path)
    w_price = Window.partitionBy("asset_id").orderBy(col("_ingestion_ts").desc())
    df_latest_price = (
        df_prices
        .withColumn("_rn", row_number().over(w_price))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    price_cols = [f.name for f in df_latest_price.schema.fields]
    update_cols = [c for c in ["bestBid", "bestAsk", "lastTradePrice"]
                   if c in df_silver_markets.columns and c in price_cols]

    out_cols = []
    for c in df_silver_markets.columns:
        if c in update_cols:
            out_cols.append(coalesce(col("t." + c), col("s." + c)).alias(c))
        else:
            out_cols.append(col("s." + c))

    df_silver_with_prices = (
        df_silver_markets.alias("s")
        .join(df_latest_price.alias("t"), col("s.conditionId") == col("t.condition_id"), "left")
        .select(out_cols)
    )
    df_silver_with_prices.write.format("delta").mode("overwrite").save(silver_markets_path)
    print(f"Updated silver markets with latest prices from {df_latest_price.count()} price records")
except Exception as e:
    if "Path does not exist" in str(e) or "cannot find" in str(e).lower():
        print("bronze/polymarket/price_updates not found - run WebSocket stream first. Skipping price update.")
    else:
        raise

# COMMAND ----------
display(df_events)

# COMMAND ----------
display(df_markets)

# COMMAND ----------
display(df_silver_markets)
