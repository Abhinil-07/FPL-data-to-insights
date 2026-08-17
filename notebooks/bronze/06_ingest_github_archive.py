# Databricks notebook source
# COMMAND ----------
# 06_ingest_github_archive.py
# Phase 2: Incremental ingestion of historical season data from vaastav/Fantasy-Premier-League.
#
# Design principles:
#   - INCREMENTAL: checks which seasons are already stored in each table.
#     Only downloads seasons that are genuinely missing.
#   - APPEND-ONLY: completed historical season data never changes.
#     Uses append mode — never overwrites existing seasons.
#   - SAFE TO RUN EVERY REFRESH: once all configured seasons are stored,
#     exits immediately with zero network calls.
#   - ADD A NEW SEASON: just add it to config.yaml seasons list — only
#     that season will be downloaded on the next run.
#
# Column strategy:
#   archive_player_gws  — drop: xP (vaastav expected-points model, unreliable),
#                                round (duplicate of GW column),
#                                modified (FPL internal retroactive flag)
#   archive_players_raw — drop: photo, squad_number, region, pulse_id,
#                                special, removed, can_select, can_transact,
#                                has_temporary_code, *_rank_type columns,
#                                *_text set-piece columns, ep_next, ep_this
#                                (mirrors the drops in 01_ingest_players_raw)
#
# Output tables:
#   fpl.bronze.archive_player_gws    — GW-by-GW stats, one row per player per GW
#   fpl.bronze.archive_players_raw   — Player dimension snapshot per season

import os
import sys
import yaml
import json
from datetime import datetime
import pandas as pd

sys.path.append(os.path.abspath("../../"))
sys.path.append(os.path.abspath("./"))

from src.github_archive import GitHubArchiveDownloader

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
seasons_to_pull = config.get("seasons", ["2023-24", "2024-25", "2025-26"])

downloader = GitHubArchiveDownloader()
ingested_at = datetime.utcnow()

# COMMAND ----------
# Columns intentionally dropped at Bronze — consistent with 01_ingest_players_raw drops.

ARCHIVE_GW_DROP_COLS = [
    # vaastav's own expected-points model — not the FPL official xG/xA
    "xP",
    # Duplicate of GW column (both exist in merged_gw.csv source)
    "round",
    # FPL retroactive adjustment flag — rarely True, no analytical use
    "modified",
]

ARCHIVE_PLAYERS_DROP_COLS = [
    # Display-only
    "photo", "squad_number", "region",
    # Internal PL / FPL admin IDs
    "pulse_id",
    # Internal FPL admin flags
    "special", "removed", "can_select", "can_transact", "has_temporary_code",
    # Position-group rank variants — Silver normalises by position
    "form_rank_type", "creativity_rank_type", "threat_rank_type",
    "influence_rank_type", "ict_index_rank_type", "now_cost_rank_type",
    "points_per_game_rank_type", "selected_rank_type",
    # Human-readable set-piece labels — numeric *_order cols kept
    "direct_freekicks_text", "corners_and_indirect_freekicks_text", "penalties_text",
    # FPL expected-points model — not used in scoring
    "ep_next", "ep_this",
]

print(f"Targeting Unity Catalog schema : {db_bronze}")
print(f"Configured historical seasons  : {seasons_to_pull}")

# COMMAND ----------
# STEP 1: Check which seasons are already stored in each table.
# Historical season data is immutable once a season ends — no need to re-download.
# Only pull seasons that are genuinely missing from the table.

def get_stored_seasons(table_name: str) -> set:
    """Return set of season strings already present in a Delta table."""
    try:
        df = spark.read.table(table_name)
        return set(row["season"] for row in df.select("season").distinct().collect())
    except Exception:
        return set()  # table doesn't exist yet

target_gw      = f"{db_bronze}.archive_player_gws"
target_players = f"{db_bronze}.archive_players_raw"

stored_gw_seasons      = get_stored_seasons(target_gw)
stored_players_seasons = get_stored_seasons(target_players)

new_gw_seasons      = [s for s in seasons_to_pull if s not in stored_gw_seasons]
new_players_seasons = [s for s in seasons_to_pull if s not in stored_players_seasons]

print(f"\nSeasons already in archive_player_gws       : {sorted(stored_gw_seasons)}")
print(f"Seasons already in archive_players_raw      : {sorted(stored_players_seasons)}")
print(f"New seasons to download for GW data         : {new_gw_seasons}")
print(f"New seasons to download for player snapshots: {new_players_seasons}")

if not new_gw_seasons and not new_players_seasons:
    print("\nAll configured seasons already stored. Nothing to download.")
    dbutils.notebook.exit("All seasons already present — skipped cleanly.")

# COMMAND ----------
# 1. Ingest Gameweek-level Historical Data — only for missing seasons

if new_gw_seasons:
    print(f"\n--- Downloading merged_gw.csv for seasons: {new_gw_seasons} ---")
    all_gw_dfs = []

    for season in new_gw_seasons:
        print(f"  Downloading {season}...")
        df_season = downloader.fetch_merged_gw(season)
        if df_season is not None and not df_season.empty:
            all_gw_dfs.append(df_season)
            print(f"  --> {len(df_season):,} rows downloaded for {season}")
        else:
            print(f"  --> WARNING: Could not download merged_gw for {season}")

    if all_gw_dfs:
        combined_gw_pdf = pd.concat(all_gw_dfs, ignore_index=True)
        combined_gw_pdf.drop(columns=[c for c in ARCHIVE_GW_DROP_COLS if c in combined_gw_pdf.columns], inplace=True)
        combined_gw_pdf["_ingested_at"] = ingested_at
        combined_gw_clean = sanitize_df_for_delta(combined_gw_pdf)
        spark_gw_df = spark.createDataFrame(combined_gw_clean.astype(str))

        spark_gw_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .format("delta") \
            .saveAsTable(target_gw)

        print(f"\n  Appended {spark_gw_df.count():,} GW rows to {target_gw}")

        available_cols = spark_gw_df.columns
        preview_cols = [c for c in ["name", "season", "GW", "total_points", "minutes", "goals_scored", "assists", "expected_goals", "expected_assists", "_ingested_at"] if c in available_cols]
        display(spark_gw_df.select(*preview_cols or available_cols[:8]).limit(15))
else:
    print("\narchive_player_gws — all seasons present, skipping.")

# COMMAND ----------
# 2. Ingest Seasonal Player Snapshots — only for missing seasons

if new_players_seasons:
    print(f"\n--- Downloading players_raw.csv for seasons: {new_players_seasons} ---")
    all_player_dfs = []

    for season in new_players_seasons:
        print(f"  Downloading {season}...")
        df_players = downloader.fetch_players_raw(season)
        if df_players is not None and not df_players.empty:
            all_player_dfs.append(df_players)
            print(f"  --> {len(df_players):,} player records downloaded for {season}")
        else:
            print(f"  --> WARNING: Could not download players_raw for {season}")

    if all_player_dfs:
        combined_players_pdf = pd.concat(all_player_dfs, ignore_index=True)
        combined_players_pdf.drop(columns=[c for c in ARCHIVE_PLAYERS_DROP_COLS if c in combined_players_pdf.columns], inplace=True)
        combined_players_pdf["_ingested_at"] = ingested_at
        combined_players_clean = sanitize_df_for_delta(combined_players_pdf)
        spark_players_df = spark.createDataFrame(combined_players_clean.astype(str))

        spark_players_df.write \
            .mode("append") \
            .option("mergeSchema", "true") \
            .format("delta") \
            .saveAsTable(target_players)

        print(f"\n  Appended {spark_players_df.count():,} player rows to {target_players}")

        available_pcols = spark_players_df.columns
        p_preview_cols = [c for c in ["first_name", "second_name", "season", "id", "code", "total_points", "_ingested_at"] if c in available_pcols]
        display(spark_players_df.select(*p_preview_cols or available_pcols[:6]).limit(15))
else:
    print("\narchive_players_raw — all seasons present, skipping.")

# COMMAND ----------
# Final verification — row counts per season across both tables
print("\n--- Post-run verification ---")
spark.sql(f"""
    SELECT 'archive_player_gws' as table_name, season,
           COUNT(*) as rows, COUNT(DISTINCT name) as unique_players
    FROM {target_gw}
    GROUP BY season
    UNION ALL
    SELECT 'archive_players_raw' as table_name, season,
           COUNT(*) as rows, COUNT(DISTINCT web_name) as unique_players
    FROM {target_players}
    GROUP BY season
    ORDER BY table_name, season
""").display()

print("Phase 2 Historical Archive Ingestion complete!")
