# Databricks notebook source
# COMMAND ----------
# 04_ingest_fixtures_raw.py
# Ingest `fpl.bronze.fixtures_raw` from fixtures/ endpoint
#
# Column strategy: keep all fixture context, FDR, scores, and status columns.
# Drop explicitly:
#   - pulse_id  → PL internal fixture ID, never joined
#   - minutes   → live match clock (only meaningful during a live match,
#                 always 0 or 90 after full-time — not useful post-match)
#   - code      → Opta fixture code. Kept for potential future FBref join.
#                 (If FBref integration is confirmed out of scope, drop this too.)

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
FIXTURES_DROP_COLS = [
    # PL internal fixture ID — never used in any join
    "pulse_id",
    # Live match clock — 0 before kickoff, 90 after full-time. No post-match value.
    "minutes",
]

# COMMAND ----------
fixtures_data = client.get_fixtures()
assert fixtures_data is not None, "Failed to fetch fixtures payload from FPL API"

fixtures_pdf = pd.DataFrame(fixtures_data)
fixtures_pdf.drop(columns=[c for c in FIXTURES_DROP_COLS if c in fixtures_pdf.columns], inplace=True)
fixtures_pdf["_ingested_at"] = ingested_at
fixtures_pdf_clean = sanitize_df_for_delta(fixtures_pdf)

print(f"Columns kept : {len(fixtures_pdf_clean.columns)}")

fixtures_df = spark.createDataFrame(fixtures_pdf_clean)

# COMMAND ----------
target_table = f"{db_bronze}.fixtures_raw"
fixtures_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)
print(f"Written {fixtures_df.count()} rows to {target_table}")

# COMMAND ----------
display(fixtures_df.select(
    "id", "event", "kickoff_time",
    "team_h", "team_a", "team_h_score", "team_a_score",
    "team_h_difficulty", "team_a_difficulty",
    "finished", "_ingested_at"
).limit(20))
