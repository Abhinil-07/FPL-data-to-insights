# Databricks notebook source
# COMMAND ----------
# 05_build_silver_fixtures.py
# Phase 3: Build fpl.silver.fixtures table with team joins and FDR difficulty scores.

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

# COMMAND ----------
# Read Bronze raw fixtures and Silver teams
fixtures_raw = spark.read.table(f"{db_bronze}.fixtures_raw")
teams_silver = spark.read.table(f"{db_silver}.teams")

# COMMAND ----------
# Clean and join team names
home_teams = teams_silver.select(
    F.col("team_id").alias("h_id"),
    F.col("team_name").alias("home_team_name"),
    F.col("short_name").alias("home_team_short")
)

away_teams = teams_silver.select(
    F.col("team_id").alias("a_id"),
    F.col("team_name").alias("away_team_name"),
    F.col("short_name").alias("away_team_short")
)

fixtures_silver = fixtures_raw \
    .join(home_teams, fixtures_raw.team_h == home_teams.h_id, "left") \
    .join(away_teams, fixtures_raw.team_a == away_teams.a_id, "left") \
    .select(
        F.col("id").cast("int").alias("fixture_id"),
        F.col("code").cast("int").alias("fixture_code"),
        F.col("event").cast("int").alias("gameweek"),
        F.col("finished").cast("boolean").alias("finished"),
        F.col("kickoff_time"),
        F.col("team_h").cast("int").alias("home_team_id"),
        F.col("home_team_name"),
        F.col("home_team_short"),
        F.col("team_a").cast("int").alias("away_team_id"),
        F.col("away_team_name"),
        F.col("away_team_short"),
        F.col("team_h_score").cast("int").alias("home_score"),
        F.col("team_a_score").cast("int").alias("away_score"),
        F.col("team_h_difficulty").cast("int").alias("home_fdr"),
        F.col("team_a_difficulty").cast("int").alias("away_fdr"),
        F.col("_ingested_at")
    )

# COMMAND ----------
# Save to fpl.silver.fixtures
target_table = f"{db_silver}.fixtures"
fixtures_silver.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully written Silver fixtures table: {target_table} ({fixtures_silver.count()} rows)")
display(fixtures_silver.select("fixture_id", "gameweek", "home_team_short", "away_team_short", "home_fdr", "away_fdr", "finished").limit(15))
