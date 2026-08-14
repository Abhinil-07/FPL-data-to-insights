# Databricks notebook source
# COMMAND ----------
# 03_build_silver_players.py
# Phase 3: Build fpl.silver.players dimension table with cleaned prices, position names, and team joins.

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

# COMMAND ----------
# Read Bronze raw players and Silver teams
players_raw = spark.read.table(f"{db_bronze}.players_raw")
teams_silver = spark.read.table(f"{db_silver}.teams")

# COMMAND ----------
# Transform & Clean Players Dimension
players_cleaned = players_raw.join(
    teams_silver.select("team_id", "team_name", "short_name"),
    players_raw.team == teams_silver.team_id,
    "left"
).select(
    F.col("code").cast("int").alias("player_key"),
    F.col("id").cast("int").alias("player_id"),
    F.trim(F.col("first_name")).alias("first_name"),
    F.trim(F.col("second_name")).alias("second_name"),
    F.trim(F.col("web_name")).alias("web_name"),
    F.col("team_id"),
    F.col("team_name"),
    F.col("short_name").alias("team_short_name"),
    F.col("element_type").cast("int").alias("position_id"),
    F.when(F.col("element_type") == 1, "GKP")
     .when(F.col("element_type") == 2, "DEF")
     .when(F.col("element_type") == 3, "MID")
     .when(F.col("element_type") == 4, "FWD")
     .otherwise("UNKNOWN").alias("position_name"),
    (F.col("now_cost").cast("double") / 10.0).alias("price_gbp"),
    F.col("selected_by_percent").cast("double").alias("ownership_percent"),
    F.col("form").cast("double").alias("form"),
    F.col("points_per_game").cast("double").alias("points_per_game"),
    F.col("total_points").cast("int").alias("total_points"),
    F.col("minutes").cast("int").alias("minutes"),
    F.col("goals_scored").cast("int").alias("goals_scored"),
    F.col("assists").cast("int").alias("assists"),
    F.col("clean_sheets").cast("int").alias("clean_sheets"),
    F.col("goals_conceded").cast("int").alias("goals_conceded"),
    F.col("saves").cast("int").alias("saves"),
    F.col("bonus").cast("int").alias("bonus"),
    F.col("bps").cast("int").alias("bps"),
    F.col("influence").cast("double").alias("influence"),
    F.col("creativity").cast("double").alias("creativity"),
    F.col("threat").cast("double").alias("threat"),
    F.col("ict_index").cast("double").alias("ict_index"),
    F.col("transfers_in_event").cast("int").alias("transfers_in_event"),
    F.col("transfers_out_event").cast("int").alias("transfers_out_event"),
    F.col("_ingested_at")
)

# COMMAND ----------
# Save to fpl.silver.players
target_table = f"{db_silver}.players"
players_cleaned.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully written Silver players dimension table: {target_table} ({players_cleaned.count()} rows)")
display(players_cleaned.select("player_key", "web_name", "team_short_name", "position_name", "price_gbp", "total_points", "form", "ownership_percent").limit(15))
