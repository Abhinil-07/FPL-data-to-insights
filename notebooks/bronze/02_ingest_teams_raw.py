# Databricks notebook source
# COMMAND ----------
# 02_ingest_teams_raw.py
# Ingest ONLY `fpl_bronze.teams_raw` from bootstrap-static/

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
# Process teams dataframe
teams_pdf = pd.DataFrame(data["teams"])
teams_pdf["_ingested_at"] = ingested_at
teams_pdf_clean = sanitize_df_for_delta(teams_pdf)

# Convert to Spark DataFrame
teams_df = spark.createDataFrame(teams_pdf_clean)

# COMMAND ----------
# Save to Delta Table
target_table = f"{db_bronze}.teams_raw"
teams_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully written {teams_df.count()} rows to Delta table: {target_table}")

# COMMAND ----------
# Display sample preview
display(teams_df.select("id", "name", "short_name", "strength", "strength_overall_home", "strength_overall_away", "_ingested_at").limit(20))
