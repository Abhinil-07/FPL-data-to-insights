# Databricks notebook source
# COMMAND ----------
# 01_ingest_players_raw.py
# Ingest ONLY `fpl_bronze.players_raw` from bootstrap-static/

import os
import sys
import yaml
import json
from datetime import datetime
import pandas as pd

sys.path.append(os.path.abspath("../../"))
sys.path.append(os.path.abspath("./"))

from src.fpl_api import FPLApiClient

# COMMAND ----------
# Helper function to sanitize pandas DataFrame for Delta Lake PySpark compatibility.
# Converts list/dict/nested VOID columns (e.g. scout_risks=[]) into JSON strings.
def sanitize_df_for_delta(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf_clean = pdf.copy()
    for col in pdf_clean.columns:
        # If column contains Python lists or dicts, serialize to JSON string
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
# Process players (elements) dataframe & sanitize nested list/dict fields
players_pdf = pd.DataFrame(data["elements"])
players_pdf["_ingested_at"] = ingested_at
players_pdf_clean = sanitize_df_for_delta(players_pdf)

# Convert to Spark DataFrame
players_df = spark.createDataFrame(players_pdf_clean)

# COMMAND ----------
# Save to Delta Table
target_table = f"{db_bronze}.players_raw"
players_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully written {players_df.count()} rows to Delta table: {target_table}")

# COMMAND ----------
# Display sample preview
display(players_df.select("id", "web_name", "team", "element_type", "now_cost", "total_points", "_ingested_at").limit(10))
