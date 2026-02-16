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

# Bronze path on ADLS
bronze_path = f"{KALSHI_DATA_PATH}/bronze/events"
df_events.write.format("delta").mode("append").save(bronze_path)

print(f"Written to {bronze_path}")

# COMMAND ----------
display(df_events)
