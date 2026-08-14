# Databricks notebook source
# COMMAND ----------
# 04_ingest_fixtures_raw.py
# Ingest ONLY `fpl_bronze.fixtures_raw` from fixtures/ endpoint

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
# Fetch fixtures
fixtures_data = client.get_fixtures()
assert fixtures_data is not None, "Failed to fetch fixtures payload from FPL API"

# COMMAND ----------
# Process fixtures dataframe
fixtures_pdf = pd.DataFrame(fixtures_data)
fixtures_pdf["_ingested_at"] = ingested_at
fixtures_pdf_clean = sanitize_df_for_delta(fixtures_pdf)

# Convert to Spark DataFrame
fixtures_df = spark.createDataFrame(fixtures_pdf_clean)

# COMMAND ----------
# Save to Delta Table
target_table = f"{db_bronze}.fixtures_raw"
fixtures_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully written {fixtures_df.count()} rows to Delta table: {target_table}")

# COMMAND ----------
# Display sample preview
display(fixtures_df.select("id", "event", "team_h", "team_a", "team_h_difficulty", "team_a_difficulty", "finished", "_ingested_at").limit(20))
