# Databricks notebook source
# COMMAND ----------
# 05_ingest_my_team_raw.py
# Ingest ONLY `fpl.bronze.my_team_raw` from entry/{TEAM_ID}/ endpoint (Optional)

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
team_id = config.get("fpl_team_id")

client = FPLApiClient()
ingested_at = datetime.utcnow()

# COMMAND ----------
if team_id:
    team_data = client.get_my_team(team_id)
    if team_data:
        my_team_pdf = pd.DataFrame([team_data])
        my_team_pdf["_ingested_at"] = ingested_at
        my_team_pdf_clean = sanitize_df_for_delta(my_team_pdf)
        my_team_df = spark.createDataFrame(my_team_pdf_clean)
        
        target_table = f"{db_bronze}.my_team_raw"
        my_team_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)
        print(f"✅ Successfully written squad data for Team ID {team_id} to Unity Catalog table: {target_table}")
        display(my_team_df)
    else:
        print(f"⚠️ Could not fetch team data for Team ID: {team_id}")
else:
    print("ℹ️ No FPL Team ID configured in config/config.yaml. Skipping personal squad Bronze table ingestion gracefully.")
