# Databricks notebook source
# COMMAND ----------
# 03_ingest_events_raw.py
# Ingest ONLY `fpl_bronze.events_raw` (Gameweek metadata) from bootstrap-static/

import os
import sys
import yaml
import json
from datetime import datetime
import pandas as pd

sys.path.append(os.path.abspath("../../"))
sys.path.append(os.path.abspath("./"))

from src.fpl_api import FPLApiClient

def sanitize_df_for_delta(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf_clean = pdf.copy()
    for col in pdf_clean.columns:
        pdf_clean[col] = pdf_clean[col].apply(
            lambda val: json.dumps(val) if isinstance(val, (list, dict)) else val
        )
    return pdf_clean

# COMMAND ----------
# Load config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze = config["databases"]["bronze"]
client = FPLApiClient()
ingested_at = datetime.utcnow()

# COMMAND ----------
# Fetch bootstrap-static
data = client.get_bootstrap_static()
assert data is not None, "Failed to fetch bootstrap-static payload from FPL API"

# COMMAND ----------
# Process events (gameweeks) dataframe
events_pdf = pd.DataFrame(data["events"])
events_pdf["_ingested_at"] = ingested_at
events_pdf_clean = sanitize_df_for_delta(events_pdf)

# Convert to Spark DataFrame
events_df = spark.createDataFrame(events_pdf_clean)

# COMMAND ----------
# Save to Delta Table
target_table = f"{db_bronze}.events_raw"
events_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully written {events_df.count()} rows to Delta table: {target_table}")

# COMMAND ----------
# Display sample preview
display(events_df.select("id", "name", "deadline_time", "is_current", "is_next", "finished", "_ingested_at").limit(38))
