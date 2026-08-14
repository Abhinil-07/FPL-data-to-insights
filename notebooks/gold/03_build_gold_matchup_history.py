# Databricks notebook source
# COMMAND ----------
# 03_build_gold_matchup_history.py
# Phase 4: Build fpl.gold.matchup_history for opponent category performance analysis.

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
teams = spark.read.table(f"{db_silver}.teams")

# COMMAND ----------
# Classify Opponents into Categories: Top-6, Promoted, Rest of League
# Top-6 Team Codes / Short Names: ARS, CHE, LIV, MCI, MUN, TOT
top_6_shorts = ["ARS", "CHE", "LIV", "MCI", "MUN", "TOT"]

teams_classified = teams.withColumn(
    "opponent_category",
    F.when(F.col("short_name").isin(top_6_shorts), "Top 6")
     .otherwise("Rest of League")
).select(
    F.col("team_id").alias("opp_team_id"),
    "opponent_category"
)

# COMMAND ----------
# Aggregate matchup performance against opponent categories
matchup_history = player_gw_history.filter(F.col("minutes") > 0) \
    .join(
        teams_classified,
        player_gw_history.opponent_team_id == teams_classified.opp_team_id,
        "left"
    ) \
    .groupBy("player_key", "opponent_category") \
    .agg(
        F.count("gameweek").alias("matches_played"),
        F.sum("points").alias("total_points"),
        F.round(F.avg("points"), 2).alias("avg_points_per_game"),
        F.sum("goals_scored").alias("goals_scored"),
        F.sum("assists").alias("assists"),
        F.round(F.sum("xg"), 2).alias("total_xg"),
        F.round(F.sum("xa"), 2).alias("total_xa")
    )

# Join with player details
matchup_enriched = matchup_history.join(
    players.select("player_key", "web_name", "team_name", "position_name"),
    "player_key",
    "left"
).select(
    "player_key",
    "web_name",
    "team_name",
    "position_name",
    "opponent_category",
    "matches_played",
    "total_points",
    "avg_points_per_game",
    "goals_scored",
    "assists",
    "total_xg",
    "total_xa",
    F.current_timestamp().alias("_updated_at")
)

# COMMAND ----------
# Save to fpl.gold.matchup_history
target_table = f"{db_gold}.matchup_history"
matchup_enriched.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Gold Matchup History table: {target_table} ({matchup_enriched.count()} rows)")
display(matchup_enriched.filter(F.lower(F.col("web_name")).contains("salah")).orderBy("opponent_category"))
