# Databricks notebook source
# COMMAND ----------
# 02_build_gold_player_trends.py
# Phase 4 — Gold: Multi-Season Historical Player Performance & Venue Splits
#
# Builds: fpl.gold.player_trends
# Delivers:
#   1. Home vs. Away Venue Splits: Diagnoses home bias vs away consistency for captaincy and starting decisions.
#   2. Multi-Season Career Progression: 3-4 season trajectory (2023-24 to 2026-27) for sustainable form validation.
#   3. Full-Season "Overall" Rollups: 1-click single-season summary rows alongside venue splits.
#   4. Per-90 True Efficiency Metrics: points_per_90 and xgi_per_90 to evaluate player quality independent of rotation/injuries.
#   5. Defensive & Goalkeeper Profiles: Clean sheet counts, clean sheet %, and goals conceded split by venue.

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
target_table = f"{db_gold}.player_trends"

print(f"Reading from Silver: {db_silver}")
print(f"Writing to Gold:     {target_table}")
print(f"Current season:      {current_season}")

# COMMAND ----------
# 1. Read Silver tables
player_gw_history = spark.read.table(f"{db_silver}.player_gw_history")
players = spark.read.table(f"{db_silver}.players")

# COMMAND ----------
# 2. Filter Active Match Appearances (minutes > 0)
# Excludes bench-only games where player had 0 minutes so averages reflect active match performance.
active_gws = player_gw_history.filter(F.col("minutes") > 0)

# COMMAND ----------
# 3. Aggregate Home vs. Away Performance (Venue Split)
venue_stats = active_gws.groupBy("player_key", "season", "is_home") \
    .agg(
        F.count("gameweek").alias("matches_played"),
        F.coalesce(F.sum("starts"), F.count("gameweek")).alias("starts"),
        F.sum("minutes").alias("total_minutes"),
        F.sum("total_points").alias("total_points"),
        F.sum("goals_scored").alias("goals_scored"),
        F.sum("assists").alias("assists"),
        F.sum("clean_sheets").alias("clean_sheets"),
        F.sum("goals_conceded").alias("goals_conceded"),
        F.sum("saves").alias("saves"),
        F.sum("bonus").alias("bonus_points"),
        F.sum("bps").alias("total_bps"),
        F.round(F.sum("expected_goals"), 2).alias("total_xg"),
        F.round(F.sum("expected_assists"), 2).alias("total_xa"),
        F.round(F.sum("expected_goal_involvements"), 2).alias("total_xgi"),
        F.round(F.sum("ict_index"), 2).alias("total_ict")
    ).withColumn(
        "venue",
        F.when(F.col("is_home") == True, F.lit("Home")).otherwise(F.lit("Away"))
    ).drop("is_home")

# COMMAND ----------
# 4. Aggregate Full-Season Overall Performance (Overall Rollup)
overall_stats = active_gws.groupBy("player_key", "season") \
    .agg(
        F.count("gameweek").alias("matches_played"),
        F.coalesce(F.sum("starts"), F.count("gameweek")).alias("starts"),
        F.sum("minutes").alias("total_minutes"),
        F.sum("total_points").alias("total_points"),
        F.sum("goals_scored").alias("goals_scored"),
        F.sum("assists").alias("assists"),
        F.sum("clean_sheets").alias("clean_sheets"),
        F.sum("goals_conceded").alias("goals_conceded"),
        F.sum("saves").alias("saves"),
        F.sum("bonus").alias("bonus_points"),
        F.sum("bps").alias("total_bps"),
        F.round(F.sum("expected_goals"), 2).alias("total_xg"),
        F.round(F.sum("expected_assists"), 2).alias("total_xa"),
        F.round(F.sum("expected_goal_involvements"), 2).alias("total_xgi"),
        F.round(F.sum("ict_index"), 2).alias("total_ict")
    ).withColumn("venue", F.lit("Overall"))

# COMMAND ----------
# 5. Union Venue Splits and Overall Rollups
combined_trends = venue_stats.unionByName(overall_stats)

# COMMAND ----------
# 6. Compute Per-Game & Per-90 Minute Efficiency Rates
enriched_rates = combined_trends.withColumn(
    "avg_minutes_per_match",
    F.when(F.col("matches_played") > 0,
           F.round(F.col("total_minutes") / F.col("matches_played"), 1))
     .otherwise(0.0)
).withColumn(
    "avg_points_per_game",
    F.when(F.col("matches_played") > 0,
           F.round(F.col("total_points") / F.col("matches_played"), 2))
     .otherwise(0.0)
).withColumn(
    "points_per_90",
    F.when(F.col("total_minutes") > 0,
           F.round(F.col("total_points") / (F.col("total_minutes") / F.lit(90.0)), 2))
     .otherwise(0.0)
).withColumn(
    "clean_sheet_pct",
    F.when(F.col("matches_played") > 0,
           F.round((F.col("clean_sheets") / F.col("matches_played")) * F.lit(100.0), 1))
     .otherwise(0.0)
).withColumn(
    "xgi_per_90",
    F.when(F.col("total_minutes") > 0,
           F.round(F.col("total_xgi") / (F.col("total_minutes") / F.lit(90.0)), 2))
     .otherwise(0.0)
)

# COMMAND ----------
# 7. Join with Player Dimension Metadata
player_meta = players.select(
    "player_key",
    "web_name",
    "team_name",
    "team_short_name",
    "position_name",
    "price_gbp"
).withColumn(
    "player_display_name",
    F.concat(F.col("web_name"), F.lit(" ("), F.col("team_short_name"), F.lit(" - "), F.col("position_name"), F.lit(")"))
)

final_player_trends = enriched_rates.join(player_meta, "player_key", "inner").select(
    "player_key",
    "web_name",
    "player_display_name",
    "team_name",
    "team_short_name",
    "position_name",
    "price_gbp",
    "season",
    "venue",
    "matches_played",
    "starts",
    "total_minutes",
    "avg_minutes_per_match",
    "total_points",
    "avg_points_per_game",
    "points_per_90",
    "goals_scored",
    "assists",
    "clean_sheets",
    "clean_sheet_pct",
    "goals_conceded",
    "saves",
    "bonus_points",
    "total_bps",
    "total_xg",
    "total_xa",
    "total_xgi",
    "xgi_per_90",
    "total_ict",
    F.current_timestamp().alias("_updated_at")
)

# COMMAND ----------
# 8. Save to fpl.gold.player_trends
final_player_trends.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

print(f"✅ Successfully created Gold Player Trends table: {target_table} ({final_player_trends.count()} rows)")
display(final_player_trends.filter(F.lower(F.col("web_name")).contains("haaland")).orderBy("season", "venue"))
