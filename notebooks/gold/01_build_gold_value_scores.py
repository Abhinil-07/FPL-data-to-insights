# Databricks notebook source
# COMMAND ----------
# 01_build_gold_value_scores.py
# Phase 4: Build fpl.gold.value_scores with 2-Tier Strategy (Set & Forget Anchors vs Rolling Transfer Targets).
# Position-aware anchor thresholds: DEF >= £5.5m, GKP >= £5.0m, MID/FWD >= £9.0m or High Ownership.

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
fixtures = spark.read.table(f"{db_silver}.fixtures")

# COMMAND ----------
# Calculate upcoming 5-gameweek Fixture Difficulty Ease per team
upcoming_fixtures = fixtures.filter(F.col("finished") == False) \
    .select(
        F.col("home_team_id").alias("team_id"),
        F.col("home_fdr").alias("fdr")
    ).unionByName(
        fixtures.filter(F.col("finished") == False) \
        .select(
            F.col("away_team_id").alias("team_id"),
            F.col("away_fdr").alias("fdr")
        )
    )

fixture_ease = upcoming_fixtures.withColumn("row_num", F.row_number().over(Window.partitionBy("team_id").orderBy("team_id"))) \
    .filter(F.col("row_num") <= 5) \
    .groupBy("team_id") \
    .agg(F.avg("fdr").alias("avg_upcoming_fdr")) \
    .withColumn("fixture_ease_score", F.lit(5.0) - F.col("avg_upcoming_fdr"))

# COMMAND ----------
# Join players with fixture ease & filter active players (minutes > 0)
players_with_ease = players.filter(F.col("minutes") > 0) \
    .join(fixture_ease, "team_id", "left") \
    .na.fill({"fixture_ease_score": 2.5, "avg_upcoming_fdr": 2.5})

# COMMAND ----------
# Compute Position-Normalized Z-Scores per Position Group (GKP, DEF, MID, FWD)
window_pos = Window.partitionBy("position_name")

z_df = players_with_ease \
    .withColumn("form_mean", F.avg("form").over(window_pos)) \
    .withColumn("form_std", F.stddev("form").over(window_pos)) \
    .withColumn("form_z", F.when(F.col("form_std") > 0, (F.col("form") - F.col("form_mean")) / F.col("form_std")).otherwise(0.0)) \
    .withColumn("ease_mean", F.avg("fixture_ease_score").over(window_pos)) \
    .withColumn("ease_std", F.stddev("fixture_ease_score").over(window_pos)) \
    .withColumn("fixture_ease_z", F.when(F.col("ease_std") > 0, (F.col("fixture_ease_score") - F.col("ease_mean")) / F.col("ease_std")).otherwise(0.0)) \
    .withColumn("min_mean", F.avg("minutes").over(window_pos)) \
    .withColumn("min_std", F.stddev("minutes").over(window_pos)) \
    .withColumn("minutes_reliability_z", F.when(F.col("min_std") > 0, (F.col("minutes") - F.col("min_mean")) / F.col("min_std")).otherwise(0.0))

# Composite Value Score & Position-Aware Strategy Classification
# DEF Anchors: price >= 5.5m or ownership >= 15%
# GKP Anchors: price >= 5.0m or ownership >= 15%
# MID/FWD Anchors: price >= 9.0m or ownership >= 25%
value_scores = z_df.withColumn(
    "value_score",
    F.round((F.lit(0.5) * F.col("form_z")) + (F.lit(0.35) * F.col("fixture_ease_z")) + (F.lit(0.15) * F.col("minutes_reliability_z")), 2)
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
).select(
    "player_key",
    "player_id",
    "web_name",
    "team_name",
    "team_short_name",
    "position_name",
    "price_gbp",
    "ownership_percent",
    "form",
    "form_z",
    "avg_upcoming_fdr",
    "fixture_ease_z",
    "minutes",
    "minutes_reliability_z",
    "value_score",
    "strategy_tier",
    F.current_timestamp().alias("_updated_at")
)

# COMMAND ----------
# Save to fpl.gold.value_scores
target_table = f"{db_gold}.value_scores"
value_scores.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Gold Value Scores table with Position-Aware Strategy: {target_table} ({value_scores.count()} rows)")
display(value_scores.filter(F.col("position_name") == "DEF").orderBy(F.col("value_score").desc()).limit(10))
