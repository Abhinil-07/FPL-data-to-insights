# Databricks notebook source
# COMMAND ----------
# 01_build_team_crosswalk.py
# Phase 3: Clean raw teams and create fpl.silver.teams dimension table with strength fallback handling

import os
import sys
import yaml
from pyspark.sql import functions as F

# COMMAND ----------
# Load config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze = config["databases"]["bronze"]
db_silver = config["databases"]["silver"]

print(f"Reading from: {db_bronze}.teams_raw")
print(f"Writing to: {db_silver}.teams")

# COMMAND ----------
# Read Bronze raw teams
teams_raw = spark.read.table(f"{db_bronze}.teams_raw")

# COMMAND ----------
# Clean & standardize schema for Silver teams with default fallbacks for pre-season null ratings
teams_silver = teams_raw.select(
    F.col("id").cast("int").alias("team_id"),
    F.col("code").cast("int").alias("team_code"),
    F.trim(F.col("name")).alias("team_name"),
    F.trim(F.col("short_name")).alias("short_name"),
    F.coalesce(F.col("strength").cast("int"), F.lit(3)).alias("strength"),
    F.coalesce(F.col("strength_overall_home").cast("int"), F.lit(1100)).alias("strength_overall_home"),
    F.coalesce(F.col("strength_overall_away").cast("int"), F.lit(1100)).alias("strength_overall_away"),
    F.coalesce(F.col("strength_attack_home").cast("int"), F.lit(1100)).alias("strength_attack_home"),
    F.coalesce(F.col("strength_attack_away").cast("int"), F.lit(1100)).alias("strength_attack_away"),
    F.coalesce(F.col("strength_defence_home").cast("int"), F.lit(1100)).alias("strength_defence_home"),
    F.coalesce(F.col("strength_defence_away").cast("int"), F.lit(1100)).alias("strength_defence_away"),
    # Current league standings — refreshed every pipeline run
    F.col("played").cast("int").alias("played"),
    F.col("win").cast("int").alias("win"),
    F.col("draw").cast("int").alias("draw"),
    F.col("loss").cast("int").alias("loss"),
    F.col("points").cast("int").alias("league_points"),
    F.col("position").cast("int").alias("league_position"),
    F.col("form").alias("form"),
    F.col("_ingested_at")
)

# COMMAND ----------
# Write to fpl.silver.teams
target_table = f"{db_silver}.teams"
teams_silver.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Silver teams dimension table: {target_table} ({teams_silver.count()} rows)")
display(teams_silver.select(
    "team_id", "team_name", "short_name",
    "strength_overall_home", "strength_overall_away",
    "played", "win", "draw", "loss", "league_points", "league_position", "form"
))
