# Databricks notebook source
# MAGIC %md
# MAGIC # Kalshi Ingestion Pipeline
# MAGIC
# MAGIC Bronze: fetches events and markets via REST API. Silver: dedupes and enriches with live ticker prices from WebSocket stream.

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
# MAGIC %md
# MAGIC ## Bronze to Silver - Events and Markets
# MAGIC
# MAGIC Reads bronze tables, deduplicates by ticker (keeps latest per entity), and writes cleaned data to silver.

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

# Silver: Events (dedupe by event_ticker)
bronze_events_path = f"{KALSHI_DATA_PATH}/bronze/events"
silver_events_path = f"{KALSHI_DATA_PATH}/silver/events"
df_bronze_events = spark.read.format("delta").load(bronze_events_path)
w_events = Window.partitionBy("event_ticker").orderBy(col("_ingestion_ts").desc())
df_silver_events = (
    df_bronze_events
    .withColumn("_rn", row_number().over(w_events))
    .filter(col("_rn") == 1)
    .drop("_rn")
)
df_silver_events.write.format("delta").mode("overwrite").save(silver_events_path)
print(f"Silver events: {df_silver_events.count()} unique events -> {silver_events_path}")

# Silver: Markets (dedupe by market ticker)
bronze_markets_path = f"{KALSHI_DATA_PATH}/bronze/markets"
silver_markets_path = f"{KALSHI_DATA_PATH}/silver/markets"
df_bronze_markets = spark.read.format("delta").load(bronze_markets_path)
w_markets = Window.partitionBy("ticker").orderBy(col("_ingestion_ts").desc())
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
# MAGIC ## Inspect ticker stream (bronze/ticker_snapshots)
# MAGIC
# MAGIC View WebSocket ticker data before joining to silver. Run WebSocket stream notebook first to populate.

# COMMAND ----------
bronze_ticker_path = f"{KALSHI_DATA_PATH}/bronze/ticker_snapshots"

try:
    df_ticker = spark.read.format("delta").load(bronze_ticker_path)
    print(f"Schema:")
    df_ticker.printSchema()
    print(f"\nTotal records: {df_ticker.count()}")
    print(f"\nSample (latest first):")
    display(df_ticker.orderBy(col("_ingestion_ts").desc()).limit(50))

    # Latest per market
    w_ticker = Window.partitionBy("market_ticker").orderBy(col("_ingestion_ts").desc())
    df_latest_ticker = (
        df_ticker
        .withColumn("_rn", row_number().over(w_ticker))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    print(f"\nLatest ticker per market ({df_latest_ticker.count()} markets):")
    display(df_latest_ticker)
except Exception as e:
    if "Path does not exist" in str(e) or "cannot find" in str(e).lower():
        print("bronze/ticker_snapshots not found. Run kalshi_websocket_stream notebook first to populate.")
    else:
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Update silver markets with latest prices from ticker stream
# MAGIC
# MAGIC Joins silver/markets with latest ticker snapshots (from WebSocket) and overwrites price columns when available.

# COMMAND ----------
from pyspark.sql.functions import coalesce

# Get latest ticker per market from stream (skip if table doesn't exist yet)
try:
    df_ticker = spark.read.format("delta").load(bronze_ticker_path)
    w_ticker = Window.partitionBy("market_ticker").orderBy(col("_ingestion_ts").desc())
    df_latest_ticker = (
        df_ticker
        .withColumn("_rn", row_number().over(w_ticker))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    ticker_cols = [f.name for f in df_latest_ticker.schema.fields]
    price_cols = [c for c in ["yes_bid", "yes_ask", "last_price", "volume", "open_interest"]
                  if c in df_silver_markets.columns and c in ticker_cols]

    out_cols = []
    for c in df_silver_markets.columns:
        if c in price_cols:
            out_cols.append(coalesce(col("t." + c), col("s." + c)).alias(c))
        else:
            out_cols.append(col("s." + c))

    df_silver_with_prices = (
        df_silver_markets.alias("s")
        .join(df_latest_ticker.alias("t"), col("s.ticker") == col("t.market_ticker"), "left")
        .select(out_cols)
    )
    df_silver_with_prices.write.format("delta").mode("overwrite").save(silver_markets_path)
    print(f"Updated silver markets with latest prices from {df_latest_ticker.count()} ticker records")
except Exception as e:
    if "Path does not exist" in str(e) or "cannot find" in str(e).lower():
        print("bronze/ticker_snapshots not found - run WebSocket stream first. Skipping price update.")
    else:
        raise

# COMMAND ----------
display(df_events)

# COMMAND ----------
display(df_markets)

# COMMAND ----------
display(df_silver_markets)
