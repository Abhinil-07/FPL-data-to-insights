# Databricks notebook source
# COMMAND ----------
# 01_build_gold_value_scores.py
# Phase 4 — Gold: Position-Normalized Player Valuation & Strategy Matrix
#
# Builds: fpl.gold.value_scores
# Delivers:
#   1. quality_score & position_quality_rank: True player quality (Haaland, Bruno, Gabriel, Palmer, Raya).
#   2. value_score & position_value_rank: Budget efficiency ROI (Points / Output per £m).
#   3. strategy_tier: 🛡️ Season Anchor (Set & Forget) vs 🔄 Rolling Transfer Target.
#   4. Pre-Season & In-Season Adaptability: Automatic baseline from historical season when live minutes == 0.

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
target_table = f"{db_gold}.value_scores"

print(f"Reading from Silver: {db_silver}")
print(f"Writing to Gold:     {target_table}")

# COMMAND ----------
# 1. Read Silver tables
players = spark.read.table(f"{db_silver}.players")
fixtures = spark.read.table(f"{db_silver}.fixtures")
player_gw_history = spark.read.table(f"{db_silver}.player_gw_history")

# COMMAND ----------
# 2. Upcoming 5-Gameweek Fixture Difficulty Ease per team (Ordered chronologically by Gameweek)
home_upcoming = fixtures.filter(F.col("finished") == False).select(
    F.col("home_team_id").alias("team_id"),
    F.col("gameweek"),
    F.col("home_fdr").alias("fdr")
)

away_upcoming = fixtures.filter(F.col("finished") == False).select(
    F.col("away_team_id").alias("team_id"),
    F.col("gameweek"),
    F.col("away_fdr").alias("fdr")
)

all_upcoming = home_upcoming.unionByName(away_upcoming)

# Take exactly the NEXT 5 upcoming gameweeks chronologically per team
fixture_ease = all_upcoming.withColumn(
    "row_num", 
    F.row_number().over(Window.partitionBy("team_id").orderBy("gameweek"))
).filter(F.col("row_num") <= 5) \
 .groupBy("team_id") \
 .agg(
     F.round(F.avg("fdr"), 2).alias("avg_upcoming_fdr")
 ).withColumn(
     # Invert FDR so higher number = easier fixtures (5.0 - 2.0 = 3.0 Ease Score)
     "fixture_ease_score", 
     F.round(F.lit(5.0) - F.col("avg_upcoming_fdr"), 2)
 )

# COMMAND ----------
# 3. Derive Historical Baseline Performance (Strictly on matches played minutes > 0)
latest_hist_season = player_gw_history.filter(F.col("season") != config.get("current_season", "2026-27")) \
    .agg(F.max("season")).collect()[0][0]

print(f"Historical baseline benchmark season: {latest_hist_season}")

# Aggregate stats on appearances where player actually played
hist_baseline = player_gw_history.filter(
    (F.col("season") == latest_hist_season) & (F.col("minutes") > 0)
).groupBy("player_key") \
 .agg(
     F.count("gameweek").alias("hist_matches_played"),
     F.sum("minutes").alias("hist_minutes"),
     F.sum("total_points").alias("hist_total_points"),
     F.round(F.avg("total_points"), 2).alias("hist_ppg"), # True Points-Per-Appearance
     F.round(F.sum("expected_goals"), 2).alias("hist_xg"),
     F.round(F.sum("expected_assists"), 2).alias("hist_xa"),
     F.round(F.sum("expected_goal_involvements"), 2).alias("hist_xgi")
 )

# COMMAND ----------
# 4. Join Active Players with Fixture Ease & Historical Baseline
active_players = players.filter(F.coalesce(F.col("status"), F.lit("a")) != "u") \
    .join(fixture_ease, "team_id", "left") \
    .join(hist_baseline, "player_key", "left") \
    .na.fill({
        "fixture_ease_score": 2.5, 
        "avg_upcoming_fdr": 2.5,
        "hist_ppg": 0.0,
        "hist_minutes": 0,
        "hist_matches_played": 0,
        "hist_total_points": 0,
        "hist_xg": 0.0,
        "hist_xa": 0.0,
        "hist_xgi": 0.0
    })

# COMMAND ----------
# 5. Adaptive Mode Switch (Pre-Season vs In-Season)
# Check if any official Premier League fixtures have finished in the current season
finished_fixtures_count = fixtures.filter(F.col("finished") == True).count()
in_season = finished_fixtures_count > 0

if in_season:
    print(f"In-Season detected ({finished_fixtures_count} finished matches): Using live rolling form and current season minutes.")
    evaluated_df = active_players \
        .withColumn("effective_form", F.col("form")) \
        .withColumn("effective_minutes", F.col("minutes"))
else:
    print("Pre-Season detected (0 finished matches): Using historical baseline PPG and historical minutes.")
    evaluated_df = active_players \
        .withColumn("effective_form", F.col("hist_ppg")) \
        .withColumn("effective_minutes", F.col("hist_minutes"))

# COMMAND ----------
# 6. Position-Normalized Z-Scores per Position Group (GKP, DEF, MID, FWD)
window_pos = Window.partitionBy("position_name")

z_df = evaluated_df \
    .withColumn("form_mean", F.avg("effective_form").over(window_pos)) \
    .withColumn("form_std", F.stddev("effective_form").over(window_pos)) \
    .withColumn("form_z", F.when(F.col("form_std") > 0, (F.col("effective_form") - F.col("form_mean")) / F.col("form_std")).otherwise(0.0)) \
    .withColumn("ease_mean", F.avg("fixture_ease_score").over(window_pos)) \
    .withColumn("ease_std", F.stddev("fixture_ease_score").over(window_pos)) \
    .withColumn("fixture_ease_z", F.when(F.col("ease_std") > 0, (F.col("fixture_ease_score") - F.col("ease_mean")) / F.col("ease_std")).otherwise(0.0)) \
    .withColumn("min_mean", F.avg("effective_minutes").over(window_pos)) \
    .withColumn("min_std", F.stddev("effective_minutes").over(window_pos)) \
    .withColumn("minutes_reliability_z", F.when(F.col("min_std") > 0, (F.col("effective_minutes") - F.col("min_mean")) / F.col("min_std")).otherwise(0.0))

# COMMAND ----------
# 7. Dual Scoring Engine: Quality Score (Best Players) & Value Score (Best ROI)
# Quality Score = 65% Form/PPG + 20% Minutes Security + 15% Fixture Ease
# Value Score = Quality / Price (£m)
scored_df = z_df.withColumn(
    "quality_score",
    F.round((F.lit(0.65) * F.col("form_z")) + (F.lit(0.20) * F.col("minutes_reliability_z")) + (F.lit(0.15) * F.col("fixture_ease_z")), 2)
).withColumn(
    "value_score",
    F.round((F.col("quality_score") + F.lit(3.0)) / F.col("price_gbp"), 2) # Standardized positive ROI per £m
).withColumn(
    "position_quality_rank",
    F.row_number().over(Window.partitionBy("position_name").orderBy(F.col("quality_score").desc()))
).withColumn(
    "position_value_rank",
    F.row_number().over(Window.partitionBy("position_name").orderBy(F.col("value_score").desc()))
).withColumn(
    "strategy_tier",
    F.when(
        (F.col("position_name") == "DEF") & ((F.col("price_gbp") >= 5.5) | (F.col("ownership_percent") >= 15.0)),
        "🛡️ Season Anchor (Set & Forget)"
    ).when(
        (F.col("position_name") == "GKP") & ((F.col("price_gbp") >= 5.0) | (F.col("ownership_percent") >= 15.0)),
        "🛡️ Season Anchor (Set & Forget)"
    ).when(
        (F.col("position_name").isin("MID", "FWD")) & ((F.col("price_gbp") >= 9.0) | (F.col("ownership_percent") >= 25.0)),
        "🛡️ Season Anchor (Set & Forget)"
    ).otherwise("🔄 Rolling Transfer Target")
).withColumn(
    "is_penalty_taker",
    F.when(F.col("penalties_order") == 1, True).otherwise(False)
).withColumn(
    "is_set_piece_taker",
    F.when((F.col("corners_and_indirect_freekicks_order") == 1) | (F.col("direct_freekicks_order") == 1), True).otherwise(False)
).select(
    "player_key",
    "player_id",
    "web_name",
    "team_name",
    "team_short_name",
    "position_name",
    "price_gbp",
    "ownership_percent",
    "position_quality_rank",
    "quality_score",
    "position_value_rank",
    "value_score",
    "strategy_tier",
    "effective_form",
    "hist_ppg",
    "hist_minutes",
    "hist_matches_played",
    "avg_upcoming_fdr",
    "fixture_ease_score",
    "effective_minutes",
    "is_penalty_taker",
    "is_set_piece_taker",
    "news",
    "chance_of_playing_next_round",
    F.current_timestamp().alias("_updated_at")
)

# COMMAND ----------
# 8. Save to fpl.gold.value_scores
scored_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

print(f"✅ Successfully created Gold Value Scores table: {target_table} ({scored_df.count()} rows)")
display(scored_df.filter(F.col("position_quality_rank") <= 5).orderBy("position_name", "position_quality_rank"))
