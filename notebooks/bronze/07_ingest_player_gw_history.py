# Databricks notebook source
# COMMAND ----------
# 07_ingest_player_gw_history.py
#
# Phase 1.4 — Bronze: Incremental ingestion of current-season per-player
# gameweek history from FPL API element-summary/{player_id}/ endpoint.
#
# Design principles:
#   - APPEND-ONLY: historical GW data never changes once data_checked=True.
#     Never overwrites the table — only adds net-new GW rows.
#   - INCREMENTAL: drives off events_raw.data_checked to know which GWs are
#     final. Only fetches data for GWs not already stored. Zero API calls
#     made if no new GWs have been checked since last run.
#   - GRACEFUL PRE-SEASON: exits cleanly with zero rows if no GWs are
#     data_checked yet (season not started or between GWs).
#   - RATE-LIMITED: uses FPLApiClient which enforces 0.3s delay + 3 retries.
#     ~700 players = ~3-5 minutes total. Only runs when genuinely needed.
#
# Output table: fpl.bronze.player_gw_history_raw
#   One row per player per gameweek for the current season.
#   Unioned with bronze.archive_player_gws in Silver to give full 3+ season history.

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
# Load config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze = config["databases"]["bronze"]
target_table = f"{db_bronze}.player_gw_history_raw"
ingested_at = datetime.utcnow()

client = FPLApiClient()
print(f"Target table : {target_table}")
print(f"Ingested at  : {ingested_at}")

# COMMAND ----------
# STEP 1: Determine which GWs are fully finalised in events_raw.
# data_checked=True means FPL has locked in bonus points — safe to treat as final.
# Using data_checked (not just finished) avoids ingesting provisional scores
# that could still change (bonus point adjustments happen after final whistle).

print("\n--- Step 1: Reading finalised GWs from events_raw ---")

try:
    events_df = spark.read.table(f"{db_bronze}.events_raw")
    finalised_gws = set(
        row["id"]
        for row in events_df
            .filter("data_checked = true")
            .select("id")
            .collect()
    )
    print(f"Finalised (data_checked) GWs in events_raw: {sorted(finalised_gws)}")
except Exception as e:
    print(f"WARNING: Could not read events_raw ({e}). Run 03_ingest_events_raw.py first.")
    finalised_gws = set()

if not finalised_gws:
    print("\nNo finalised GWs found — season not started or events_raw not populated.")
    print("Nothing to ingest. Exiting cleanly.")
    dbutils.notebook.exit("No finalised GWs — skipped cleanly.")

# COMMAND ----------
# STEP 2: Determine which GWs are already stored in player_gw_history_raw.
# On first run (table doesn't exist), stored_gws = empty set.

print("\n--- Step 2: Checking already-stored GWs in player_gw_history_raw ---")

try:
    existing_df = spark.read.table(target_table)
    stored_gws = set(
        row["round"]
        for row in existing_df.select("round").distinct().collect()
    )
    print(f"Already stored GWs: {sorted(stored_gws)}")
except Exception:
    stored_gws = set()
    print("Table does not exist yet — first run, all finalised GWs are new.")

# COMMAND ----------
# STEP 3: Compute net-new GWs to ingest.

new_gws = finalised_gws - stored_gws
print(f"\n--- Step 3: Net-new GWs to ingest: {sorted(new_gws)} ---")

if not new_gws:
    print("All finalised GWs already stored. Nothing to do.")
    dbutils.notebook.exit("Already up to date — no new GWs to ingest.")

# COMMAND ----------
# STEP 4: Get all active player IDs from players_raw.
# This is the source of truth for which player IDs to loop over.
# players_raw is already populated by 01_ingest_players_raw.py.

print("\n--- Step 4: Loading player IDs from players_raw ---")

try:
    players_df = spark.read.table(f"{db_bronze}.players_raw")
    player_ids = [row["id"] for row in players_df.select("id").collect()]
    print(f"Total players to fetch: {len(player_ids)}")
except Exception as e:
    print(f"ERROR: Could not read players_raw ({e}). Run 01_ingest_players_raw.py first.")
    raise

# COMMAND ----------
# STEP 5: Loop through all players and collect GW history rows for new_gws only.
# - Calls element-summary/{player_id}/ for every active player (~700 calls).
# - Filters history rows to only those where round is in new_gws.
# - Appends player_fpl_id to each row (the element field in history also has it,
#   but we add it explicitly for clarity since it's the primary join key).
# - Failed/missing players are logged and skipped — do not crash the pipeline.

print(f"\n--- Step 5: Fetching GW history for {len(player_ids)} players ---")
print(f"           Collecting rows for GWs: {sorted(new_gws)}")
print(f"           Estimated time: {len(player_ids) * 0.3 / 60:.1f}–{len(player_ids) * 0.5 / 60:.1f} minutes\n")

all_rows = []
failed_ids = []

for i, player_id in enumerate(player_ids, start=1):
    if i % 100 == 0 or i == 1:
        print(f"  Progress: {i}/{len(player_ids)} players processed — {len(all_rows)} rows collected so far")

    summary = client.get_element_summary(player_id)

    if summary is None:
        failed_ids.append(player_id)
        continue

    history = summary.get("history", [])

    # Filter to only new GW rows
    new_rows = [row for row in history if row.get("round") in new_gws]

    for row in new_rows:
        row["_player_fpl_id"] = player_id   # explicit denormalised key
        row["_ingested_at"]   = str(ingested_at)
        all_rows.append(row)

print(f"\n  Completed: {len(player_ids)} players processed")
print(f"  Rows collected  : {len(all_rows)}")
print(f"  Failed player IDs ({len(failed_ids)}): {failed_ids[:20]}{'...' if len(failed_ids) > 20 else ''}")

# COMMAND ----------
# STEP 6: Build Spark DataFrame and append to Delta table.
# Uses astype(str) for schema stability — same pattern as archive notebook.
# Silver layer casts columns to correct types.

if not all_rows:
    print("\nNo rows collected (players may have 0 minutes in these GWs). Exiting.")
    dbutils.notebook.exit("No rows to write — zero-minute GWs or all players failed.")

print(f"\n--- Step 6: Writing {len(all_rows)} rows to {target_table} ---")

# Convert nested types to JSON strings for Delta compatibility
def sanitize_for_delta(row: dict) -> dict:
    return {
        k: json.dumps(v) if isinstance(v, (list, dict)) else v
        for k, v in row.items()
    }

pdf = pd.DataFrame([sanitize_for_delta(r) for r in all_rows])
pdf = pdf.astype(str)   # full string cast for cross-GW schema stability

spark_df = spark.createDataFrame(pdf)

spark_df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

print(f"Successfully appended {spark_df.count()} rows to {target_table}")

# COMMAND ----------
# STEP 7: Verify — show row counts per GW after this run.

print("\n--- Step 7: Post-run verification ---")
verification = spark.sql(f"""
    SELECT
        CAST(round AS INT)          AS gw,
        COUNT(*)                    AS total_rows,
        COUNT(DISTINCT _player_fpl_id) AS unique_players
    FROM {target_table}
    GROUP BY CAST(round AS INT)
    ORDER BY gw
""")
display(verification)

print(f"\nDone. GWs now stored in {target_table}: "
      f"{sorted(stored_gws | new_gws)}")
