# Databricks notebook source
# COMMAND ----------
# 04_build_gold_team_trends.py
# Phase 4 — Gold: Club-Level Attacking & Defensive Momentum Matrix
#
# Builds: fpl.gold.team_trends
# Delivers:
#   1. Rolling 6-Match Momentum: Identifies recent form streaks (goals scored, clean sheets, goals conceded).
#   2. Full-Season Baseline Context: Full-season clean sheet %, home vs. away defensive records.
#   3. Defensive Double-Up Signals: Flags elite defensive units (e.g. Arsenal, Man City) for double-ups.
#   4. Captaincy Target Detector: Flags weak/leaky defenses (10+ goals conceded) to target with attackers.
#   5. Pre-Season & In-Season Adaptability: Uses most recent completed season during pre-season,
#      automatically transitions to rolling live match telemetry once current season starts.

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
current_season = config.get("current_season", "2026-27")
target_table = f"{db_gold}.team_trends"

print(f"Reading from Silver: {db_silver}")
print(f"Writing to Gold:     {target_table}")
print(f"Current season:      {current_season}")

# COMMAND ----------
# 1. Read Silver tables
fixtures = spark.read.table(f"{db_silver}.fixtures")
teams = spark.read.table(f"{db_silver}.teams")
player_gw_history = spark.read.table(f"{db_silver}.player_gw_history")

# COMMAND ----------
# 2. Extract Match Outcomes per Team (Live Season vs. Pre-Season Fallback)
finished_fixtures_count = fixtures.filter(F.col("finished") == True).count()

if finished_fixtures_count > 0:
    print(f"Live Season Mode: Processing {finished_fixtures_count} finished fixtures from 2026-27...")
    home_games = fixtures.filter(F.col("finished") == True) \
        .select(
            F.col("home_team_id").alias("team_id"),
            F.col("gameweek"),
            F.lit(True).alias("is_home"),
            F.col("home_score").alias("goals_scored"),
            F.col("away_score").alias("goals_conceded"),
            F.when(F.col("away_score") == 0, 1).otherwise(0).alias("clean_sheet")
        )

    away_games = fixtures.filter(F.col("finished") == True) \
        .select(
            F.col("away_team_id").alias("team_id"),
            F.col("gameweek"),
            F.lit(False).alias("is_home"),
            F.col("away_score").alias("goals_scored"),
            F.col("home_score").alias("goals_conceded"),
            F.when(F.col("home_score") == 0, 1).otherwise(0).alias("clean_sheet")
        )

    all_team_games = home_games.unionByName(away_games)
else:
    latest_season = player_gw_history.filter(F.col("season") != current_season) \
        .agg(F.max("season")).collect()[0][0]
    print(f"Pre-Season Mode: Computing team trends from completed benchmark season: {latest_season}...")

    # Extract distinct team matches from historical player logs
    hist_team_games = player_gw_history.filter(
        (F.col("season") == latest_season) & (F.col("minutes") > 0)
    ).groupBy("team_id", "gameweek", "is_home").agg(
        F.max(F.when(F.col("is_home") == True, F.col("team_h_score")).otherwise(F.col("team_a_score"))).alias("goals_scored"),
        F.max(F.when(F.col("is_home") == True, F.col("team_a_score")).otherwise(F.col("team_h_score"))).alias("goals_conceded")
    ).withColumn(
        "clean_sheet",
        F.when(F.col("goals_conceded") == 0, 1).otherwise(0)
    ).select(
        "team_id",
        "gameweek",
        "is_home",
        "goals_scored",
        "goals_conceded",
        "clean_sheet"
    )

    all_team_games = hist_team_games

# COMMAND ----------
# 3. Compute Rolling 6-Match Momentum (Current Form)
window_recent = Window.partitionBy("team_id").orderBy(F.col("gameweek").desc())

rolling_6_stats = all_team_games.withColumn("row_num", F.row_number().over(window_recent)) \
    .filter(F.col("row_num") <= 6) \
    .groupBy("team_id") \
    .agg(
        F.count("gameweek").alias("last_6_matches_played"),
        F.sum("goals_scored").alias("goals_scored_last_6"),
        F.sum("goals_conceded").alias("goals_conceded_last_6"),
        F.sum("clean_sheet").alias("clean_sheets_last_6"),
        F.round(F.avg("goals_scored"), 2).alias("avg_goals_scored_last_6"),
        F.round(F.avg("goals_conceded"), 2).alias("avg_goals_conceded_last_6")
    ).withColumn(
        "clean_sheet_pct_last_6",
        F.when(F.col("last_6_matches_played") > 0,
               F.round((F.col("clean_sheets_last_6") / F.col("last_6_matches_played")) * F.lit(100.0), 1))
         .otherwise(0.0)
    )

# COMMAND ----------
# 4. Compute Full-Season / Benchmark Season Totals (Context & Venue Splits)
full_season_stats = all_team_games.groupBy("team_id") \
    .agg(
        F.count("gameweek").alias("full_season_matches"),
        F.sum("goals_scored").alias("full_season_goals_scored"),
        F.sum("goals_conceded").alias("full_season_goals_conceded"),
        F.sum("clean_sheet").alias("full_season_clean_sheets"),
        F.round(F.avg("goals_scored"), 2).alias("full_season_avg_goals_scored"),
        F.round(F.avg("goals_conceded"), 2).alias("full_season_avg_goals_conceded"),
        F.round((F.sum("clean_sheet") / F.count("gameweek")) * F.lit(100.0), 1).alias("full_season_cs_pct"),
        # Home Clean Sheet Percentage
        F.round(
            (F.sum(F.when(F.col("is_home") == True, F.col("clean_sheet")).otherwise(0)) /
             F.nullif(F.count(F.when(F.col("is_home") == True, 1)), 0)) * F.lit(100.0), 1
        ).alias("home_clean_sheet_pct"),
        # Away Clean Sheet Percentage
        F.round(
            (F.sum(F.when(F.col("is_home") == False, F.col("clean_sheet")).otherwise(0)) /
             F.nullif(F.count(F.when(F.col("is_home") == False, 1)), 0)) * F.lit(100.0), 1
        ).alias("away_clean_sheet_pct")
    )

# COMMAND ----------
# 5. Join Dimensions & Assign Tactical Form Tiers
teams_enriched = teams.select("team_id", "team_name", "short_name", "strength") \
    .join(rolling_6_stats, "team_id", "left") \
    .join(full_season_stats, "team_id", "left") \
    .na.fill({
        "last_6_matches_played": 0,
        "goals_scored_last_6": 0,
        "goals_conceded_last_6": 0,
        "clean_sheets_last_6": 0,
        "clean_sheet_pct_last_6": 0.0,
        "avg_goals_scored_last_6": 0.0,
        "avg_goals_conceded_last_6": 0.0,
        "full_season_matches": 0,
        "full_season_goals_scored": 0,
        "full_season_goals_conceded": 0,
        "full_season_clean_sheets": 0,
        "full_season_avg_goals_scored": 0.0,
        "full_season_avg_goals_conceded": 0.0,
        "full_season_cs_pct": 0.0,
        "home_clean_sheet_pct": 0.0,
        "away_clean_sheet_pct": 0.0
    }).withColumn(
        # Tactical Defensive Tag
        "defensive_tier",
        F.when(
            (F.col("clean_sheets_last_6") >= 3) | (F.col("avg_goals_conceded_last_6") <= 0.8),
            "🛡️ Defensive Fortress (Top CS Form)"
        ).when(
            (F.col("goals_conceded_last_6") >= 10) | (F.col("avg_goals_conceded_last_6") >= 1.8),
            "⚠️ Leaky Defense (Target with Attackers)"
        ).otherwise("⚖️ Neutral Defense")
    ).withColumn(
        # Tactical Attacking Tag
        "attacking_tier",
        F.when(
            (F.col("goals_scored_last_6") >= 12) | (F.col("avg_goals_scored_last_6") >= 2.0),
            "🔥 Hot Attack (High Goal Volume)"
        ).when(
            (F.col("goals_scored_last_6") <= 4) | (F.col("avg_goals_scored_last_6") <= 0.8),
            "❄️ Cold Attack (Low Scoring)"
        ).otherwise("⚖️ Neutral Attack")
    ).select(
        "team_id",
        "team_name",
        "short_name",
        "strength",
        "defensive_tier",
        "attacking_tier",
        "last_6_matches_played",
        "goals_scored_last_6",
        "avg_goals_scored_last_6",
        "goals_conceded_last_6",
        "avg_goals_conceded_last_6",
        "clean_sheets_last_6",
        "clean_sheet_pct_last_6",
        "full_season_matches",
        "full_season_goals_scored",
        "full_season_avg_goals_scored",
        "full_season_goals_conceded",
        "full_season_avg_goals_conceded",
        "full_season_clean_sheets",
        "full_season_cs_pct",
        "home_clean_sheet_pct",
        "away_clean_sheet_pct",
        F.current_timestamp().alias("_updated_at")
    )

# COMMAND ----------
# 6. Save to fpl.gold.team_trends
final_target_table = f"{db_gold}.team_trends"
teams_enriched.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(final_target_table)

print(f"✅ Successfully created Gold Team Trends table: {final_target_table} ({teams_enriched.count()} rows)")
display(teams_enriched.orderBy(F.col("clean_sheets_last_6").desc(), F.col("avg_goals_conceded_last_6").asc()))
