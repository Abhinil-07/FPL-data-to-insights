# Databricks notebook source
# COMMAND ----------
# 03_ingest_events_raw.py
# Ingest `fpl.bronze.events_raw` (GW metadata) from bootstrap-static/ events[]
#
# Column strategy: keep all status, deadline, aggregate, and chip columns.
# Drop explicitly:
#   - deadline_time_game_offset → internal game clock offset, not used
#   - cup_leagues_created       → FPL admin flag, not relevant to analytics
#   - h2h_ko_matches_created    → FPL admin flag, not relevant to analytics

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
EVENTS_DROP_COLS = [
    # Internal game clock offset — no analytical use
    "deadline_time_game_offset",
    # FPL admin flags — not relevant to GW analytics
    "cup_leagues_created",
    "h2h_ko_matches_created",
]

# COMMAND ----------
data = client.get_bootstrap_static()
assert data is not None, "Failed to fetch bootstrap-static payload from FPL API"

events_pdf = pd.DataFrame(data["events"])
events_pdf.drop(columns=[c for c in EVENTS_DROP_COLS if c in events_pdf.columns], inplace=True)
events_pdf["_ingested_at"] = ingested_at
events_pdf_clean = sanitize_df_for_delta(events_pdf)

print(f"Columns kept : {len(events_pdf_clean.columns)}")

events_df = spark.createDataFrame(events_pdf_clean)

# COMMAND ----------
target_table = f"{db_bronze}.events_raw"
events_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)
print(f"Written {events_df.count()} rows to {target_table}")

# COMMAND ----------
display(events_df.select(
    "id", "name", "deadline_time",
    "finished", "data_checked",
    "is_previous", "is_current", "is_next",
    "average_entry_score", "transfers_made", "_ingested_at"
).limit(38))
