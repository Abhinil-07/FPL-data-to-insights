# Databricks notebook source
# COMMAND ----------
# 06_ingest_github_archive.py
# Phase 2: Ingest historical 2-3 season datasets from vaastav/Fantasy-Premier-League repo into Bronze Delta tables.

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

print(f"Targeting Unity Catalog schema: {db_bronze}")
print(f"Configured historical seasons to ingest: {seasons_to_pull}")

# COMMAND ----------
# 1. Ingest Gameweek-level Historical Data (`merged_gw.csv` across configured seasons)
all_gw_dfs = []

for season in seasons_to_pull:
    print(f"Downloading merged gameweek data for season {season}...")
    df_season = downloader.fetch_merged_gw(season)
    if df_season is not None and not df_season.empty:
        all_gw_dfs.append(df_season)
        print(f"  --> Downloaded {len(df_season)} rows for {season}")
    else:
        print(f"  --> Warning: Could not download merged_gw for season {season}")

if all_gw_dfs:
    # Union all historical season dataframes
    combined_gw_pdf = pd.concat(all_gw_dfs, ignore_index=True)
    combined_gw_pdf["_ingested_at"] = ingested_at
    combined_gw_clean = sanitize_df_for_delta(combined_gw_pdf)

    # Convert all columns to string/object compatible types for Spark schema stability across seasons
    spark_gw_df = spark.createDataFrame(combined_gw_clean.astype(str))

    target_table_gw = f"{db_bronze}.archive_player_gws"
    spark_gw_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table_gw)
    print(f"✅ Successfully written {spark_gw_df.count()} total historical GW rows to Unity Catalog table: {target_table_gw}")
    
    # Select available preview columns dynamically to avoid hardcoded column missing errors
    available_cols = spark_gw_df.columns
    preview_cols = [c for c in ["name", "season", "GW", "total_points", "minutes", "goals_scored", "assists", "expected_goals", "expected_assists", "_ingested_at"] if c in available_cols]
    if not preview_cols:
        preview_cols = available_cols[:8]
        
    display(spark_gw_df.select(*preview_cols).limit(15))

# COMMAND ----------
# 2. Ingest Seasonal Player Snapshots (`players_raw.csv` across configured seasons)
all_player_dfs = []

for season in seasons_to_pull:
    print(f"Downloading seasonal players_raw snapshot for season {season}...")
    df_players = downloader.fetch_players_raw(season)
    if df_players is not None and not df_players.empty:
        all_player_dfs.append(df_players)
        print(f"  --> Downloaded {len(df_players)} player records for {season}")
    else:
        print(f"  --> Warning: Could not download players_raw for season {season}")

if all_player_dfs:
    combined_players_pdf = pd.concat(all_player_dfs, ignore_index=True)
    combined_players_pdf["_ingested_at"] = ingested_at
    combined_players_clean = sanitize_df_for_delta(combined_players_pdf)

    spark_players_df = spark.createDataFrame(combined_players_clean.astype(str))

    target_table_players = f"{db_bronze}.archive_players_raw"
    spark_players_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table_players)
    print(f"✅ Successfully written {spark_players_df.count()} total historical player snapshot rows to Unity Catalog table: {target_table_players}")
    
    available_pcols = spark_players_df.columns
    p_preview_cols = [c for c in ["first_name", "second_name", "season", "id", "code", "total_points", "_ingested_at"] if c in available_pcols]
    if not p_preview_cols:
        p_preview_cols = available_pcols[:6]
        
    display(spark_players_df.select(*p_preview_cols).limit(15))

print("🎉 Phase 2 Historical Archive Ingestion complete!")
