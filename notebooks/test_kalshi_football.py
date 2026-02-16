# Databricks notebook source
# MAGIC %md
# MAGIC # Kalshi CBB Events - Bronze Ingestion (MVP)
# MAGIC
# MAGIC Fetches open events for a series (default: KXNCAAMBGAME) and writes to bronze layer.
# MAGIC **Run mount_adls notebook first** to set `KALSHI_DATA_PATH`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
dbutils.widgets.text("series_id", "KXNCAAMBGAME", "Series ticker (e.g. KXNCAAMBGAME)")

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

from src.kalshi.api import fetch_events
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

# Bronze path: raw events per series
bronze_path = f"{KALSHI_DATA_PATH}/bronze/events"
df_events.write.format("delta").mode("append").save(bronze_path)

print(f"Written to {bronze_path}")

# COMMAND ----------
display(df_events)
