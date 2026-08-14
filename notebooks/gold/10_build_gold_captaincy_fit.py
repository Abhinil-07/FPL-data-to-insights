# Databricks notebook source
# COMMAND ----------
# 10_build_gold_captaincy_fit.py
# Phase 4: Build fpl.gold.captaincy_fit for weekly captaincy selection.
# Includes minimum 2 matches / 180 minutes filter to exclude 0-minute fringe players.

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
# Read Silver & Gold tables
players = spark.read.table(f"{db_silver}.players")
player_gw_history = spark.read.table(f"{db_silver}.player_gw_history")
value_scores = spark.read.table(f"{db_gold}.value_scores")

# COMMAND ----------
# Compute Haul Ceiling (Frequency of 10+ point hauls and max haul) with minimum 2 matches requirement
haul_stats = player_gw_history.filter(F.col("minutes") > 0) \
    .groupBy("player_key") \
    .agg(
        F.count("gameweek").alias("matches_played"),
        F.sum("minutes").alias("total_minutes"),
        F.max("points").alias("max_single_match_haul"),
        F.sum(F.when(F.col("points") >= 10, 1).otherwise(0)).alias("hauls_10plus_count"),
        F.round(F.stddev("points"), 2).alias("points_volatility")
    ).filter((F.col("matches_played") >= 2) & (F.col("total_minutes") >= 180)) \
    .withColumn(
        "haul_frequency_percent",
        F.round(100.0 * F.col("hauls_10plus_count") / F.col("matches_played"), 1)
    )

# COMMAND ----------
# Join Value Scores with Haul Ceiling stats for established active players only
captaincy_base = value_scores.join(haul_stats, "player_key", "inner")

# Compute Position-Normalized Captaincy Fit Score
window_pos = Window.partitionBy("position_name")

captaincy_ranked = captaincy_base \
    .withColumn("haul_std", F.stddev("haul_frequency_percent").over(window_pos)) \
    .withColumn("haul_mean", F.avg("haul_frequency_percent").over(window_pos)) \
    .withColumn("haul_z", F.when(F.col("haul_std") > 0, (F.col("haul_frequency_percent") - F.col("haul_mean")) / F.col("haul_std")).otherwise(0.0)) \
    .withColumn(
        "captaincy_fit_score",
        F.round((F.lit(0.4) * F.col("form_z")) + (F.lit(0.35) * F.col("haul_z")) + (F.lit(0.25) * F.col("fixture_ease_z")), 2)
    ).withColumn(
        "captaincy_rank",
        F.row_number().over(Window.partitionBy("position_name").orderBy(F.col("captaincy_fit_score").desc()))
    ).select(
        "captaincy_rank",
        "player_key",
        "web_name",
        "team_name",
        "position_name",
        "price_gbp",
        "ownership_percent",
        "form",
        "avg_upcoming_fdr",
        "hauls_10plus_count",
        "haul_frequency_percent",
        "max_single_match_haul",
        "captaincy_fit_score",
        F.current_timestamp().alias("_updated_at")
    )

# COMMAND ----------
# Save to fpl.gold.captaincy_fit
target_table = f"{db_gold}.captaincy_fit"
captaincy_ranked.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Gold Captaincy Fit table: {target_table} ({captaincy_ranked.count()} rows)")
display(captaincy_ranked.filter(F.col("captaincy_rank") <= 5).orderBy("position_name", "captaincy_rank"))
