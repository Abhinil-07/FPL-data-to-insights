# Databricks notebook source
# COMMAND ----------
# 08_build_gold_underlying_stats.py
# Phase 4: Build fpl.gold.underlying_stats to flag players 'due a return' (actual goals < expected xG).

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
players = spark.read.table(f"{db_silver}.players")
player_gw_history = spark.read.table(f"{db_silver}.player_gw_history")

# COMMAND ----------
# Aggregate 3-year trailing xG vs Actual Goals
underlying_agg = player_gw_history.filter(F.col("minutes") > 0) \
    .groupBy("player_key") \
    .agg(
        F.sum("goals_scored").alias("total_goals"),
        F.round(F.sum("xg"), 2).alias("total_xg"),
        F.round(F.sum("xa"), 2).alias("total_xa"),
        F.round(F.sum("ict_index"), 2).alias("total_ict")
    ).withColumn(
        "xg_delta",
        F.round(F.col("total_xg") - F.col("total_goals"), 2)
    ).withColumn(
        "due_a_return_flag",
        F.when(F.col("xg_delta") >= 2.0, "Due a Goal 🎯 (Underperforming xG)")
         .when(F.col("xg_delta") <= -2.0, "Overperforming xG ⚠️")
         .otherwise("Expected Output ⚖️")
    )

# Join with player dimension
underlying_stats_enriched = players.select("player_key", "web_name", "team_name", "position_name", "price_gbp", "total_points") \
    .join(underlying_agg, "player_key", "left") \
    .na.fill({"total_goals": 0, "total_xg": 0.0, "total_xa": 0.0, "xg_delta": 0.0, "due_a_return_flag": "Expected Output ⚖️"}) \
    .select(
        "player_key",
        "web_name",
        "team_name",
        "position_name",
        "price_gbp",
        "total_points",
        "total_goals",
        "total_xg",
        "total_xa",
        "xg_delta",
        "due_a_return_flag",
        F.current_timestamp().alias("_updated_at")
    )

# COMMAND ----------
# Save to fpl.gold.underlying_stats
target_table = f"{db_gold}.underlying_stats"
underlying_stats_enriched.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Gold Underlying Stats table: {target_table} ({underlying_stats_enriched.count()} rows)")
display(underlying_stats_enriched.filter(F.col("xg_delta") >= 2.0).orderBy(F.col("xg_delta").desc()).limit(15))
