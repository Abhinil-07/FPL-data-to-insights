# Databricks notebook source
# COMMAND ----------
# 07_ingest_player_gw_history.py
#
# Bronze: Incremental ingestion of current-season per-player GW match stats.
#
# Design:
#   - SOURCE: event/{gw_id}/live/ endpoint — ONE API call per new GW
#     (replaces the old element-summary loop of ~700 calls per GW)
#   - INCREMENTAL: only fetches GWs with data_checked=True not already stored
#   - APPEND-ONLY: finalised GW data never changes — never overwrites
#   - PRE-SEASON SAFE: exits cleanly if no GWs are finalised yet
#
# Output table: fpl.bronze.player_gw_history_raw
#   One row per player per GW — match stats only.
#   Does NOT contain fixture context (was_home, opponent etc.) or economic
#   data (price, transfers, ownership). Those come from:
#     - fixtures_raw + players_raw      → fixture context (joined in Silver)
#     - players_gw_snapshot_raw         → economic data (notebook 08, same run)
#
# Column strategy: keep all stats returned by the live endpoint.
#   No drops needed — the endpoint returns exactly the match stats we need.
#
# NOTE: notebook 08_snapshot_player_economics MUST run in the same pipeline
#   refresh immediately after this notebook. It snapshots players_raw before
#   the next run overwrites it.

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
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze    = config["databases"]["bronze"]
target_table = f"{db_bronze}.player_gw_history_raw"
ingested_at  = datetime.utcnow()

client = FPLApiClient()
print(f"Target table : {target_table}")
print(f"Ingested at  : {ingested_at}")

# COMMAND ----------
# STEP 1: Determine finalised GWs from events_raw (data_checked = True).
# data_checked means FPL has locked in bonus points — scores are truly final.

print("\n--- Step 1: Reading finalised GWs from events_raw ---")

try:
    events_df = spark.read.table(f"{db_bronze}.events_raw")
    finalised_gws = set(
        int(row["id"])
        for row in events_df.filter("data_checked = true").select("id").collect()
    )
    print(f"Finalised GWs : {sorted(finalised_gws)}")
except Exception as e:
    print(f"WARNING: Could not read events_raw ({e}). Run 03_ingest_events_raw.py first.")
    finalised_gws = set()

if not finalised_gws:
    print("\nNo finalised GWs — season not started or events_raw not populated. Exiting cleanly.")
    dbutils.notebook.exit("No finalised GWs — skipped cleanly.")

# COMMAND ----------
# STEP 2: Determine which GWs are already in player_gw_history_raw.

print("\n--- Step 2: Checking already-stored GWs ---")

try:
    existing_df = spark.read.table(target_table)
    stored_gws = set(
        int(row["round"])
        for row in existing_df.select("round").distinct().collect()
    )
    print(f"Already stored GWs : {sorted(stored_gws)}")
except Exception:
    stored_gws = set()
    print("Table does not exist yet — first run.")

# COMMAND ----------
# STEP 3: Compute net-new GWs.

new_gws = finalised_gws - stored_gws
print(f"\n--- Step 3: Net-new GWs to ingest: {sorted(new_gws)} ---")

if not new_gws:
    print("All finalised GWs already stored. Nothing to do.")
    dbutils.notebook.exit("Already up to date — no new GWs to ingest.")

# COMMAND ----------
# STEP 4: Fetch GW live data — ONE API call per new GW.
# event/{gw_id}/live/ returns stats for ALL ~700 players in a single response.

print(f"\n--- Step 4: Fetching live GW data ({len(new_gws)} API call(s)) ---")

all_history_rows = []

for gw in sorted(new_gws):
    print(f"  Calling event/{gw}/live/ ...")
    live_data = client.get_event_live(gw)

    if live_data is None:
        print(f"  WARNING: Could not fetch live data for GW {gw}. Skipping.")
        continue

    elements = live_data.get("elements", [])
    print(f"  GW {gw}: {len(elements)} player entries returned")

    for element in elements:
        stats = element.get("stats", {})
        row = {
            "element": element["id"],
            "round":   gw,
            **stats,
            "_ingested_at": str(ingested_at),
        }
        all_history_rows.append(row)

print(f"\n  Total rows collected: {len(all_history_rows)}")

# COMMAND ----------
# STEP 5: Write to player_gw_history_raw.

if not all_history_rows:
    print("\nNo rows collected. Exiting.")
    dbutils.notebook.exit("No rows to write.")

print(f"\n--- Step 5: Writing {len(all_history_rows)} rows to {target_table} ---")

def sanitize_for_delta(row: dict) -> dict:
    return {k: json.dumps(v) if isinstance(v, (list, dict)) else v for k, v in row.items()}

history_pdf = pd.DataFrame([sanitize_for_delta(r) for r in all_history_rows]).astype(str)
history_spark_df = spark.createDataFrame(history_pdf)

history_spark_df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

print(f"Appended {history_spark_df.count()} rows to {target_table}")

# COMMAND ----------
# STEP 6: Post-run verification.

print("\n--- Step 6: Post-run verification ---")
spark.sql(f"""
    SELECT CAST(round AS INT) AS gw,
           COUNT(*)              AS player_rows,
           COUNT(DISTINCT element) AS unique_players
    FROM {target_table}
    GROUP BY CAST(round AS INT)
    ORDER BY gw
""").display()

print(f"\nDone. GWs now in {target_table}: {sorted(stored_gws | new_gws)}")
print("\nREMINDER: Run 08_snapshot_player_economics next to capture price/ownership data.")
