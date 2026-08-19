# Databricks notebook source
# COMMAND ----------
# 03_build_gold_matchup_history.py
# Phase 4 — Gold: Opponent Matchup History & Opponent Strength Diagnostic
#
# Builds: fpl.gold.matchup_history
# Delivers:
#   1. Specific Club Head-to-Head Records: Performance against exact upcoming opponents (e.g. Haaland vs Arsenal).
#   2. Opponent Tier Rollups: Aggregates performance against 'Top 6', 'Mid-Table', and 'Bottom 6 / Promoted' clubs.
#   3. Flat-Track Bully vs. Big-Game Diagnostics: Spots players who only score against weak teams vs. big-game performers.
#   4. Defensive Clean Sheet Probability: Clean sheet % against elite attacks vs. bottom-tier attacks.
#   5. Per-90 & Advanced Telemetry: xgi_per_90, points_per_90, xG, xA, and minutes security per matchup.

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
current_season = config.get("current_season", "2026-27")
target_table = f"{db_gold}.matchup_history"

print(f"Reading from Silver: {db_silver}")
print(f"Writing to Gold:     {target_table}")
print(f"Current season:      {current_season}")

# COMMAND ----------
# 1. Read Silver tables
player_gw_history = spark.read.table(f"{db_silver}.player_gw_history")
players = spark.read.table(f"{db_silver}.players")
teams = spark.read.table(f"{db_silver}.teams")

# COMMAND ----------
# 2. Classify Opponent Clubs into Tiers
# Top 6: Traditional Big 6 clubs
# Bottom 6 / Promoted: Relegation candidates & newly promoted sides across recent seasons
# Mid-Table: Established solid mid-tier Premier League clubs
top_6_shorts = ["ARS", "CHE", "LIV", "MCI", "MUN", "TOT"]
bottom_tier_shorts = ["IPS", "LEI", "SOU", "LUT", "SHU", "BUR", "NFO", "EVE", "WOL", "LEE", "SUN"]

teams_classified = teams.select(
    F.col("team_id").alias("opp_team_id"),
    F.col("team_name").alias("opp_team_name"),
    F.col("short_name").alias("opp_short_name")
).withColumn(
    "opponent_tier",
    F.when(F.col("opp_short_name").isin(top_6_shorts), F.lit("Top 6"))
     .when(F.col("opp_short_name").isin(bottom_tier_shorts), F.lit("Bottom 6 / Promoted"))
     .otherwise(F.lit("Mid-Table"))
)

# COMMAND ----------
# 3. Filter Active Match Appearances (minutes > 0)
# Join with opponent team metadata
active_gws = player_gw_history.filter(F.col("minutes") > 0).join(
    teams_classified,
    player_gw_history.opponent_team_id == teams_classified.opp_team_id,
    "left"
).withColumn(
    "opp_team_name", F.coalesce(F.col("opp_team_name"), F.lit("Unknown Club"))
).withColumn(
    "opp_short_name", F.coalesce(F.col("opp_short_name"), F.lit("UNK"))
).withColumn(
    "opponent_tier", F.coalesce(F.col("opponent_tier"), F.lit("Mid-Table"))
)

# COMMAND ----------
# 4. Stream A: Specific Opponent Club Matchups (e.g. vs Arsenal, vs Chelsea)
club_matchups = active_gws.groupBy(
    "player_key", "opp_team_id", "opp_team_name", "opp_short_name", "opponent_tier"
).agg(
    F.count("gameweek").alias("matches_played"),
    F.sum("minutes").alias("total_minutes"),
    F.sum("total_points").alias("total_points"),
    F.sum("goals_scored").alias("goals_scored"),
    F.sum("assists").alias("assists"),
    F.sum("clean_sheets").alias("clean_sheets"),
    F.sum("goals_conceded").alias("goals_conceded"),
    F.sum("saves").alias("saves"),
    F.sum("bonus").alias("bonus_points"),
    F.round(F.sum("expected_goals"), 2).alias("total_xg"),
    F.round(F.sum("expected_assists"), 2).alias("total_xa"),
    F.round(F.sum("expected_goal_involvements"), 2).alias("total_xgi"),
    F.round(F.sum("ict_index"), 2).alias("total_ict")
).withColumnRenamed("opp_team_name", "opponent_team_name") \
 .withColumnRenamed("opp_short_name", "opponent_short_name") \
 .drop("opp_team_id")

# COMMAND ----------
# 5. Stream B: Opponent Tier Summary Rollups (e.g. vs All Top 6, vs All Mid-Table, vs All Bottom 6)
tier_matchups = active_gws.groupBy(
    "player_key", "opponent_tier"
).agg(
    F.count("gameweek").alias("matches_played"),
    F.sum("minutes").alias("total_minutes"),
    F.sum("total_points").alias("total_points"),
    F.sum("goals_scored").alias("goals_scored"),
    F.sum("assists").alias("assists"),
    F.sum("clean_sheets").alias("clean_sheets"),
    F.sum("goals_conceded").alias("goals_conceded"),
    F.sum("saves").alias("saves"),
    F.sum("bonus").alias("bonus_points"),
    F.round(F.sum("expected_goals"), 2).alias("total_xg"),
    F.round(F.sum("expected_assists"), 2).alias("total_xa"),
    F.round(F.sum("expected_goal_involvements"), 2).alias("total_xgi"),
    F.round(F.sum("ict_index"), 2).alias("total_ict")
).withColumn(
    "opponent_team_name",
    F.concat(F.lit("All "), F.col("opponent_tier"), F.lit(" Clubs"))
).withColumn(
    "opponent_short_name",
    F.when(F.col("opponent_tier") == "Top 6", F.lit("TOP6"))
     .when(F.col("opponent_tier") == "Bottom 6 / Promoted", F.lit("BOT6"))
     .otherwise(F.lit("MID6"))
)

# COMMAND ----------
# 6. Union Specific Club Matchups & Tier Summary Rollups
combined_matchups = club_matchups.unionByName(tier_matchups)

# COMMAND ----------
# 7. Compute Per-Game & Per-90 Minute Rates
enriched_matchups = combined_matchups.withColumn(
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
# 8. Join with Player Metadata
player_meta = players.select(
    "player_key",
    "web_name",
    "team_name",
    "team_short_name",
    "position_name",
    "price_gbp"
)

final_matchup_history = enriched_matchups.join(player_meta, "player_key", "inner").select(
    "player_key",
    "web_name",
    "team_name",
    "team_short_name",
    "position_name",
    "price_gbp",
    "opponent_team_name",
    "opponent_short_name",
    "opponent_tier",
    "matches_played",
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
    "total_xg",
    "total_xa",
    "total_xgi",
    "xgi_per_90",
    "total_ict",
    F.current_timestamp().alias("_updated_at")
)

# COMMAND ----------
# 9. Save to fpl.gold.matchup_history
final_matchup_history.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

print(f"✅ Successfully created Gold Matchup History table: {target_table} ({final_matchup_history.count()} rows)")
display(final_matchup_history.filter(F.lower(F.col("web_name")).contains("haaland")).orderBy("opponent_tier", "opponent_team_name"))
