# Databricks notebook source
# COMMAND ----------
# 04_build_gold_team_trends.py
# Phase 4: Build fpl.gold.team_trends for rolling 6-game team attacking & defensive form.
# Automatically falls back to most recent historical season match logs when current season matches = 0 (Pre-Season).

import os
import sys
import yaml
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# Load config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_silver = config["databases"]["silver"]
db_gold = config["databases"]["gold"]

# COMMAND ----------
# Read Silver tables
fixtures = spark.read.table(f"{db_silver}.fixtures")
teams = spark.read.table(f"{db_silver}.teams")
player_gw_history = spark.read.table(f"{db_silver}.player_gw_history")

# COMMAND ----------
# Check if current season has finished fixtures
finished_fixtures_count = fixtures.filter(F.col("finished") == True).count()

if finished_fixtures_count > 0:
    print("Using live current season finished fixtures for team trends...")
    home_games = fixtures.filter(F.col("finished") == True) \
        .select(
            F.col("home_team_id").alias("team_id"),
            F.col("gameweek"),
            F.col("home_score").alias("goals_scored"),
            F.col("away_score").alias("goals_conceded"),
            F.when(F.col("away_score") == 0, 1).otherwise(0).alias("clean_sheet")
        )

    away_games = fixtures.filter(F.col("finished") == True) \
        .select(
            F.col("away_team_id").alias("team_id"),
            F.col("gameweek"),
            F.col("away_score").alias("goals_scored"),
            F.col("home_score").alias("goals_conceded"),
            F.when(F.col("home_score") == 0, 1).otherwise(0).alias("clean_sheet")
        )

    all_team_games = home_games.unionByName(away_games)
else:
    print("Pre-Season detected (0 current season finished fixtures). Computing team trends from most recent historical season match logs...")
    # Derive team match outcomes from player_gw_history for the most recent completed historical season
    latest_season = player_gw_history.agg(F.max("season")).collect()[0][0]
    print(f"Using completed season: {latest_season}")

    hist_games = player_gw_history.filter((F.col("season") == latest_season) & (F.col("minutes") > 0)) \
        .join(teams.select("team_id"), player_gw_history.opponent_team_id == teams.team_id, "inner") \
        .groupBy("opponent_team_id", "gameweek") \
        .agg(
            F.max("goals_conceded").alias("goals_scored"), # Goals conceded by opponent = Goals scored by team
            F.max("goals_scored").alias("goals_conceded"),
            F.when(F.max("goals_scored") == 0, 1).otherwise(0).alias("clean_sheet")
        ).select(
            F.col("opponent_team_id").alias("team_id"),
            F.col("gameweek"),
            F.col("goals_scored"),
            F.col("goals_conceded"),
            F.col("clean_sheet")
        )

    all_team_games = hist_games

# COMMAND ----------
# Compute rolling 6-game window stats per team
window_team_recent = Window.partitionBy("team_id").orderBy(F.col("gameweek").desc())

team_recent = all_team_games.withColumn("row_num", F.row_number().over(window_team_recent)) \
    .filter(F.col("row_num") <= 6) \
    .groupBy("team_id") \
    .agg(
        F.count("gameweek").alias("last_6_games_played"),
        F.sum("goals_scored").alias("goals_scored_last_6"),
        F.sum("goals_conceded").alias("goals_conceded_last_6"),
        F.sum("clean_sheet").alias("clean_sheets_last_6"),
        F.round(F.avg("goals_scored"), 2).alias("avg_goals_scored_last_6"),
        F.round(F.avg("goals_conceded"), 2).alias("avg_goals_conceded_last_6")
    )

# Join with teams dimension
team_trends_enriched = teams.select("team_id", "team_name", "short_name") \
    .join(team_recent, "team_id", "left") \
    .na.fill({
        "last_6_games_played": 0,
        "goals_scored_last_6": 0,
        "goals_conceded_last_6": 0,
        "clean_sheets_last_6": 0,
        "avg_goals_scored_last_6": 0.0,
        "avg_goals_conceded_last_6": 0.0
    }) \
    .select(
        "team_id",
        "team_name",
        "short_name",
        "last_6_games_played",
        "goals_scored_last_6",
        "goals_conceded_last_6",
        "clean_sheets_last_6",
        "avg_goals_scored_last_6",
        "avg_goals_conceded_last_6",
        F.current_timestamp().alias("_updated_at")
    )

# COMMAND ----------
# Save to fpl.gold.team_trends
target_table = f"{db_gold}.team_trends"
team_trends_enriched.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Gold Team Trends table: {target_table} ({team_trends_enriched.count()} rows)")
display(team_trends_enriched.orderBy(F.col("clean_sheets_last_6").desc(), F.col("avg_goals_conceded_last_6").asc()))
