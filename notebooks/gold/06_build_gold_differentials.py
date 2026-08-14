# Databricks notebook source
# COMMAND ----------
# 06_build_gold_differentials.py
# Phase 4: Build fpl.gold.differentials for surfacing low-ownership (<10%) players with high underlying xG/xA stats.

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
players = spark.read.table(f"{db_silver}.players")
player_gw_history = spark.read.table(f"{db_silver}.player_gw_history")

# COMMAND ----------
# Compute 3-year trailing underlying xG + xA per 90 mins for active players
underlying_stats = player_gw_history.filter(F.col("minutes") > 0) \
    .groupBy("player_key") \
    .agg(
        F.sum("minutes").alias("total_minutes"),
        F.round(F.sum("xg"), 2).alias("total_xg"),
        F.round(F.sum("xa"), 2).alias("total_xa"),
        F.round((F.sum("xg") + F.sum("xa")), 2).alias("total_gi"), # Goal Involvements (xG + xA)
        F.round((F.sum("xg") + F.sum("xa")) / (F.sum("minutes") / 90.0), 2).alias("gi_per_90")
    )

# Filter players with <10% ownership and minimum 450 minutes played
differentials_base = players.filter((F.col("ownership_percent") < 10.0) & (F.col("minutes") > 0)) \
    .join(underlying_stats, "player_key", "left") \
    .filter(F.col("total_minutes") >= 450)

# Position-normalized Differential Ranking
window_pos = Window.partitionBy("position_name").orderBy(F.col("gi_per_90").desc())

differentials_ranked = differentials_base.withColumn("differential_rank", F.row_number().over(window_pos)) \
    .select(
        "differential_rank",
        "player_key",
        "web_name",
        "team_name",
        "position_name",
        "price_gbp",
        "ownership_percent",
        "form",
        "total_points",
        "total_minutes",
        "total_xg",
        "total_xa",
        "total_gi",
        "gi_per_90",
        F.current_timestamp().alias("_updated_at")
    )

# COMMAND ----------
# Save to fpl.gold.differentials
target_table = f"{db_gold}.differentials"
differentials_ranked.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Gold Differentials table: {target_table} ({differentials_ranked.count()} rows)")
display(differentials_ranked.filter(F.col("differential_rank") <= 5).orderBy("position_name", "differential_rank"))
