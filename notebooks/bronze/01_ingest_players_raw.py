# Databricks notebook source
# COMMAND ----------
# 01_ingest_players_raw.py
# Ingest `fpl.bronze.players_raw` from bootstrap-static/ elements[]
#
# Column strategy: keep every column with analytical value. Drop explicitly:
#   - photo, squad_number, region     → display-only, no analytics use
#   - pulse_id                         → PL internal ID, never joined
#   - special, removed, can_select,
#     can_transact, has_temporary_code → internal FPL admin flags
#   - *_rank_type columns              → position-group rank variants —
#                                        Silver handles position normalisation
#   - direct_freekicks_text,
#     corners_and_indirect_freekicks_text,
#     penalties_text                   → human-readable labels for order cols,
#                                        redundant next to the numeric *_order cols
#   - ep_next, ep_this                 → FPL's own expected-points model,
#                                        not reliable / not used in scoring

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
# Columns intentionally dropped at Bronze.
# Reason documented per column group above.
PLAYERS_DROP_COLS = [
    # Display-only — no analytical value
    "photo", "squad_number", "region",
    # Internal PL / FPL admin IDs never used in joins
    "pulse_id",
    # Internal FPL admin flags
    "special", "removed", "can_select", "can_transact", "has_temporary_code",
    # Position-group rank variants — Silver normalises by position anyway
    "form_rank_type", "creativity_rank_type", "threat_rank_type",
    "influence_rank_type", "ict_index_rank_type", "now_cost_rank_type",
    "points_per_game_rank_type", "selected_rank_type",
    # Human-readable labels for set-piece order — numeric *_order cols kept
    "direct_freekicks_text", "corners_and_indirect_freekicks_text", "penalties_text",
    # FPL's own expected-points model — unreliable, not used in scoring
    "ep_next", "ep_this",
]

# COMMAND ----------
# Fetch bootstrap-static
data = client.get_bootstrap_static()
assert data is not None, "Failed to fetch bootstrap-static payload from FPL API"

# COMMAND ----------
# Build players DataFrame, drop unused columns, sanitize nested types
players_pdf = pd.DataFrame(data["elements"])
players_pdf.drop(columns=[c for c in PLAYERS_DROP_COLS if c in players_pdf.columns], inplace=True)
players_pdf["_ingested_at"] = ingested_at
players_pdf_clean = sanitize_df_for_delta(players_pdf)

print(f"Columns kept : {len(players_pdf_clean.columns)}")
print(f"Columns dropped: {[c for c in PLAYERS_DROP_COLS if c in pd.DataFrame(data['elements']).columns]}")

players_df = spark.createDataFrame(players_pdf_clean)

# COMMAND ----------
# Save to Unity Catalog Delta Table
target_table = f"{db_bronze}.players_raw"
players_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)
print(f"Written {players_df.count()} rows to {target_table}")

# COMMAND ----------
# Preview
display(players_df.select(
    "id", "web_name", "element_type", "team", "now_cost",
    "total_points", "selected_by_percent", "expected_goals", "_ingested_at"
).limit(10))
