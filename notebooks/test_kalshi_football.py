# Databricks notebook source
# MAGIC %md
# MAGIC # Kalshi CBB Events - Bronze Ingestion (MVP)
# MAGIC
# MAGIC Fetches open events for a series (default: KXNCAAMBGAME) and writes to Unity Catalog bronze table.
# MAGIC
# MAGIC **First run:** Create schema if needed: `CREATE SCHEMA IF NOT EXISTS main.kalshi_bronze;`

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------
dbutils.widgets.text("series_id", "KXNCAAMBGAME", "Series ticker (e.g. KXNCAAMBGAME)")
dbutils.widgets.text("table", "main.kalshi_bronze.events", "Unity Catalog table (catalog.schema.table)")

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
# MAGIC ## Write to bronze (Unity Catalog)

# COMMAND ----------
df_events = spark.createDataFrame(all_events)

# Add ingestion metadata
df_events = df_events.withColumn("_ingestion_ts", current_timestamp())
df_events = df_events.withColumn("_series_ticker", lit(series_id))

table_name = dbutils.widgets.get("table")
df_events.write.mode("append").saveAsTable(table_name)

print(f"Written to {table_name}")

# COMMAND ----------
display(df_events)
