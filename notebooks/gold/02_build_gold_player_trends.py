# Databricks notebook source
# COMMAND ----------
# 02_build_gold_player_trends.py
# Phase 4: Build fpl.gold.player_trends with home/away and opponent strength performance splits over 3 seasons.

import os
import sys
import yaml
from pyspark.sql import functions as F

# COMMAND ----------
# Load config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_silver = config["databases"]["silver"]
db_gold = config["databases"]["gold"]

# COMMAND ----------
# Read Silver tables
player_gw_history = spark.read.table(f"{db_silver}.player_gw_history")
players = spark.read.table(f"{db_silver}.players")

# COMMAND ----------
# Pre-aggregate Home vs. Away performance per player per season
player_trends = player_gw_history.filter(F.col("minutes") > 0) \
    .groupBy("player_key", "season", "is_home") \
    .agg(
        F.count("gameweek").alias("matches_played"),
        F.sum("minutes").alias("total_minutes"),
        F.sum("points").alias("total_points"),
        F.round(F.avg("points"), 2).alias("avg_points_per_game"),
        F.sum("goals_scored").alias("goals_scored"),
        F.sum("assists").alias("assists"),
        F.round(F.sum("xg"), 2).alias("total_xg"),
        F.round(F.sum("xa"), 2).alias("total_xa"),
        F.round(F.sum("ict_index"), 2).alias("total_ict")
    )

# Join with player details
player_trends_enriched = player_trends.join(
    players.select("player_key", "web_name", "team_name", "position_name", "price_gbp"),
    "player_key",
    "left"
).select(
    "player_key",
    "web_name",
    "team_name",
    "position_name",
    "price_gbp",
    "season",
    F.when(F.col("is_home") == True, "Home").otherwise("Away").alias("venue"),
    "matches_played",
    "total_minutes",
    "total_points",
    "avg_points_per_game",
    "goals_scored",
    "assists",
    "total_xg",
    "total_xa",
    "total_ict",
    F.current_timestamp().alias("_updated_at")
)

# COMMAND ----------
# Save to fpl.gold.player_trends
target_table = f"{db_gold}.player_trends"
player_trends_enriched.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Gold Player Trends table: {target_table} ({player_trends_enriched.count()} rows)")
display(player_trends_enriched.filter(F.lower(F.col("web_name")).contains("haaland")).orderBy("season", "venue"))
