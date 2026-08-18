# Databricks notebook source
# COMMAND ----------
# 06_build_silver_gameweeks.py
# Phase 3 — Silver: Gameweek Calendar & Benchmark Dimension (fpl.silver.gameweeks)
#
# Source: fpl.bronze.events_raw (38 gameweek rows from bootstrap-static)
# Output: fpl.silver.gameweeks
#
# Enriches gameweeks with:
#   - Standardized timestamps (deadline_time)
#   - Active status flags (is_previous, is_current, is_next)
#   - Global benchmark averages and highest scores
#   - Crowd data: most captained, most selected, and chip counts
#   - Player names resolved for top performers

import os
import sys
import yaml
from pyspark.sql import functions as F

# COMMAND ----------
# Load config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze = config["databases"]["bronze"]
db_silver = config["databases"]["silver"]
target_table = f"{db_silver}.gameweeks"

print(f"Reading from: {db_bronze}.events_raw")
print(f"Writing to:   {target_table}")

# COMMAND ----------
# Read Bronze raw events and Silver players for name resolution
events_raw = spark.read.table(f"{db_bronze}.events_raw")
silver_players = spark.read.table(f"{db_silver}.players").select(
    F.col("player_id"),
    F.col("web_name")
)

# COMMAND ----------
# Clean & Standardize Gameweeks
gameweeks_prep = events_raw.select(
    F.col("id").cast("int").alias("gameweek"),
    F.trim(F.col("name")).alias("gameweek_name"),
    F.to_timestamp(F.col("deadline_time")).alias("deadline_time"),
    F.coalesce(F.col("finished").cast("boolean"), F.lit(False)).alias("finished"),
    F.coalesce(F.col("data_checked").cast("boolean"), F.lit(False)).alias("data_checked"),
    F.coalesce(F.col("is_previous").cast("boolean"), F.lit(False)).alias("is_previous"),
    F.coalesce(F.col("is_current").cast("boolean"), F.lit(False)).alias("is_current"),
    F.coalesce(F.col("is_next").cast("boolean"), F.lit(False)).alias("is_next"),
    F.col("average_entry_score").cast("double").alias("average_score"),
    F.col("highest_score").cast("int").alias("highest_score"),
    F.col("highest_scoring_entry").cast("int").alias("highest_scoring_entry_id"),
    F.col("transfers_made").cast("int").alias("total_transfers_made"),
    F.col("most_selected").cast("int").alias("most_selected_player_id"),
    F.col("most_transferred_in").cast("int").alias("most_transferred_in_player_id"),
    F.col("most_captained").cast("int").alias("most_captained_player_id"),
    F.col("most_vice_captained").cast("int").alias("most_vice_captained_player_id"),
    F.col("top_element").cast("int").alias("top_scoring_player_id"),
    F.col("chip_plays").alias("chip_plays_json"),
    F.col("_ingested_at")
)

# Resolve player names for most captained and top scoring players
cap_players = silver_players.select(
    F.col("player_id").alias("cap_pid"),
    F.col("web_name").alias("most_captained_player_name")
)

top_players = silver_players.select(
    F.col("player_id").alias("top_pid"),
    F.col("web_name").alias("top_scoring_player_name")
)

gameweeks_silver = gameweeks_prep \
    .join(cap_players, gameweeks_prep.most_captained_player_id == cap_players.cap_pid, "left") \
    .join(top_players, gameweeks_prep.top_scoring_player_id == top_players.top_pid, "left") \
    .select(
        gameweeks_prep.gameweek,
        gameweeks_prep.gameweek_name,
        gameweeks_prep.deadline_time,
        gameweeks_prep.finished,
        gameweeks_prep.data_checked,
        gameweeks_prep.is_previous,
        gameweeks_prep.is_current,
        gameweeks_prep.is_next,
        gameweeks_prep.average_score,
        gameweeks_prep.highest_score,
        gameweeks_prep.highest_scoring_entry_id,
        gameweeks_prep.total_transfers_made,
        gameweeks_prep.most_captained_player_id,
        cap_players.most_captained_player_name,
        gameweeks_prep.most_selected_player_id,
        gameweeks_prep.most_transferred_in_player_id,
        gameweeks_prep.most_vice_captained_player_id,
        gameweeks_prep.top_scoring_player_id,
        top_players.top_scoring_player_name,
        gameweeks_prep.chip_plays_json,
        gameweeks_prep._ingested_at
    ) \
    .orderBy("gameweek")

# COMMAND ----------
# Save to fpl.silver.gameweeks
gameweeks_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

print(f"✅ Successfully created Silver Gameweeks dimension: {target_table} ({gameweeks_silver.count()} rows)")
display(gameweeks_silver.select(
    "gameweek", "gameweek_name", "deadline_time", 
    "is_current", "is_next", "finished", "average_score", 
    "most_captained_player_name", "top_scoring_player_name"
).limit(10))
