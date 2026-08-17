# Databricks notebook source
# COMMAND ----------
# 08_snapshot_player_economics.py
#
# Bronze: Incremental economic snapshot of players_raw → players_gw_snapshot_raw.
#
# Purpose:
#   players_raw is overwritten on every pipeline run — it always holds only the
#   current moment's data. This notebook preserves the per-GW economic fields
#   (price, transfers, ownership) BEFORE they get wiped on the next refresh.
#
# Why it must run in the same pipeline refresh as notebook 07:
#   - players_raw is refreshed by notebook 01 (Stage 1)
#   - notebook 07 determines which GWs are new (data_checked but not yet stored)
#   - THIS notebook snapshots players_raw for exactly those same new GWs
#   - On the NEXT run, players_raw will reflect the next GW — GW1's values gone
#   If this notebook is skipped for a GW, that GW's economic data is lost forever.
#   Enforce: always run 07 → 08 together in the same Databricks Job run.
#
# Output table: fpl.bronze.players_gw_snapshot_raw
#   Columns: element, gw, now_cost, transfers_in_event,
#            transfers_out_event, selected_by_percent, _ingested_at
#   One row per player per finalised GW. ~700 rows per GW, growing each week.
#   Joined to player_gw_history_raw in Silver on (element, gw) to add
#   price and ownership history to the unified player_gw_history Silver table.
#
# Column strategy: deliberately narrow — only the 4 economic fields that
#   would otherwise be lost. Everything else is already in players_raw (overwrite)
#   or player_gw_history_raw (append from live endpoint).

import os
import sys
import yaml
from datetime import datetime
import pandas as pd

sys.path.append(os.path.abspath("../../"))
sys.path.append(os.path.abspath("./"))

# COMMAND ----------
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze      = config["databases"]["bronze"]
target_table   = f"{db_bronze}.players_gw_snapshot_raw"
ingested_at    = datetime.utcnow()

print(f"Target table : {target_table}")
print(f"Ingested at  : {ingested_at}")

# COMMAND ----------
# STEP 1: Determine finalised GWs from events_raw (data_checked = True).

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
# STEP 2: Determine which GWs are already in players_gw_snapshot_raw.
# Only snapshot GWs that are not already captured.

print("\n--- Step 2: Checking already-stored GWs in players_gw_snapshot_raw ---")

try:
    existing_df = spark.read.table(target_table)
    stored_gws = set(
        int(row["gw"])
        for row in existing_df.select("gw").distinct().collect()
    )
    print(f"Already stored GWs : {sorted(stored_gws)}")
except Exception:
    stored_gws = set()
    print("Table does not exist yet — first run.")

# COMMAND ----------
# STEP 3: Compute net-new GWs to snapshot.

new_gws = finalised_gws - stored_gws
print(f"\n--- Step 3: Net-new GWs to snapshot: {sorted(new_gws)} ---")

if not new_gws:
    print("All finalised GWs already snapshotted. Nothing to do.")
    dbutils.notebook.exit("Already up to date — no new GWs to snapshot.")

# COMMAND ----------
# STEP 4: Read the 4 economic columns from players_raw.
# players_raw was refreshed by notebook 01 earlier this run — it holds
# the economic data for the current GW. We snapshot it now before the
# next pipeline run overwrites it.

print("\n--- Step 4: Reading economic columns from players_raw ---")

# Only these 4 columns are snapshotted — all other player data lives
# in players_raw (current state) or player_gw_history_raw (match stats).
SNAPSHOT_COLS = [
    "id",                   # renamed to 'element' — join key to player_gw_history_raw
    "now_cost",             # price this GW in tenths (130 = £13.0m)
    "transfers_in_event",   # managers who transferred in this GW
    "transfers_out_event",  # managers who transferred out this GW
    "selected_by_percent",  # % of managers who owned player this GW
]

try:
    players_raw_df = spark.read.table(f"{db_bronze}.players_raw")
    snapshot_base = players_raw_df.select(SNAPSHOT_COLS).toPandas()
    print(f"Loaded {len(snapshot_base)} players from players_raw")
except Exception as e:
    print(f"ERROR: Could not read players_raw ({e}). Run 01_ingest_players_raw.py first.")
    raise

# COMMAND ----------
# STEP 5: Build snapshot rows — one set per new GW — and append.
# In most runs new_gws = {one GW}. DGW catch-up runs may have multiple.

print(f"\n--- Step 5: Writing snapshots for GWs {sorted(new_gws)} ---")

all_snapshot_rows = []
for gw in sorted(new_gws):
    gw_snapshot = snapshot_base.copy()
    gw_snapshot.rename(columns={"id": "element"}, inplace=True)
    gw_snapshot["gw"]          = str(gw)
    gw_snapshot["_ingested_at"] = str(ingested_at)
    all_snapshot_rows.append(gw_snapshot)

snapshot_pdf = pd.concat(all_snapshot_rows, ignore_index=True).astype(str)
snapshot_spark_df = spark.createDataFrame(snapshot_pdf)

snapshot_spark_df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

rows_written = snapshot_spark_df.count()
print(f"Appended {rows_written:,} rows to {target_table}")
print(f"  ({len(new_gws)} GW(s) × {len(snapshot_base)} players = {len(new_gws) * len(snapshot_base):,} expected)")

# COMMAND ----------
# STEP 6: Post-run verification.

print("\n--- Step 6: Post-run verification ---")
spark.sql(f"""
    SELECT CAST(gw AS INT) AS gw,
           COUNT(*)              AS players_snapshotted,
           MIN(CAST(now_cost AS INT)) AS min_price_tenths,
           MAX(CAST(now_cost AS INT)) AS max_price_tenths
    FROM {target_table}
    GROUP BY CAST(gw AS INT)
    ORDER BY gw
""").display()

print(f"\nDone. GWs now in {target_table}: {sorted(stored_gws | new_gws)}")
