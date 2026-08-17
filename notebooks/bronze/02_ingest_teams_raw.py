# Databricks notebook source
# COMMAND ----------
# 02_ingest_teams_raw.py
# Ingest `fpl.bronze.teams_raw` from bootstrap-static/ teams[]
#
# Column strategy: keep all strength, form, and result columns.
# Drop explicitly:
#   - pulse_id → PL internal team ID, never used in joins

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
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze = config["databases"]["bronze"]
client = FPLApiClient()
ingested_at = datetime.utcnow()

# COMMAND ----------
# Columns intentionally dropped at Bronze.
TEAMS_DROP_COLS = [
    # PL internal ID — never used in any join or analytics
    "pulse_id",
]

# COMMAND ----------
data = client.get_bootstrap_static()
assert data is not None, "Failed to fetch bootstrap-static payload from FPL API"

teams_pdf = pd.DataFrame(data["teams"])
teams_pdf.drop(columns=[c for c in TEAMS_DROP_COLS if c in teams_pdf.columns], inplace=True)
teams_pdf["_ingested_at"] = ingested_at
teams_pdf_clean = sanitize_df_for_delta(teams_pdf)

print(f"Columns kept : {len(teams_pdf_clean.columns)}")

teams_df = spark.createDataFrame(teams_pdf_clean)

# COMMAND ----------
target_table = f"{db_bronze}.teams_raw"
teams_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)
print(f"Written {teams_df.count()} rows to {target_table}")

# COMMAND ----------
display(teams_df.select(
    "id", "name", "short_name", "code",
    "strength_overall_home", "strength_overall_away",
    "strength_attack_home", "strength_attack_away",
    "strength_defence_home", "strength_defence_away",
    "played", "win", "draw", "loss", "position", "_ingested_at"
).limit(20))
