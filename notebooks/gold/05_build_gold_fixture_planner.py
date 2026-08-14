# Databricks notebook source
# COMMAND ----------
# 05_build_gold_fixture_planner.py
# Phase 4: Build fpl.gold.fixture_planner with wide upcoming N-gameweek FDR difficulty matrix per team.

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
fixtures = spark.read.table(f"{db_silver}.fixtures")
teams = spark.read.table(f"{db_silver}.teams")

# COMMAND ----------
# Unpivot fixtures into team perspective (Home and Away)
home_f = fixtures.filter(F.col("finished") == False) \
    .select(
        F.col("home_team_id").alias("team_id"),
        F.col("gameweek"),
        F.col("away_team_short").alias("opponent_short"),
        F.col("home_fdr").alias("fdr"),
        F.lit("H").alias("venue")
    )

away_f = fixtures.filter(F.col("finished") == False) \
    .select(
        F.col("away_team_id").alias("team_id"),
        F.col("gameweek"),
        F.col("home_team_short").alias("opponent_short"),
        F.col("away_fdr").alias("fdr"),
        F.lit("A").alias("venue")
    )

all_upcoming = home_f.unionByName(away_f)

# COMMAND ----------
# Format fixture display e.g. "ARS (H) - 2" (Opponent, Venue, FDR)
formatted_fixtures = all_upcoming.withColumn(
    "fixture_desc",
    F.concat(F.col("opponent_short"), F.lit(" ("), F.col("venue"), F.lit(") FDR:"), F.col("fdr"))
)

# Window to take next 5 upcoming gameweeks
window_team_gw = Window.partitionBy("team_id").orderBy("gameweek")

next_5_fixtures = formatted_fixtures.withColumn("gw_order", F.row_number().over(window_team_gw)) \
    .filter(F.col("gw_order") <= 5)

# Pivot wide by gw_order (gw_1, gw_2, gw_3, gw_4, gw_5)
pivoted_planner = next_5_fixtures.groupBy("team_id") \
    .pivot("gw_order", [1, 2, 3, 4, 5]) \
    .agg(
        F.first("fixture_desc").alias("desc"),
        F.first("fdr").alias("fdr_val")
    )

# Calculate 5-GW average FDR
avg_fdr = next_5_fixtures.groupBy("team_id") \
    .agg(F.round(F.avg("fdr"), 2).alias("avg_5gw_fdr"))

# Join with teams
fixture_planner = teams.select("team_id", "team_name", "short_name") \
    .join(avg_fdr, "team_id", "left") \
    .join(pivoted_planner, "team_id", "left") \
    .select(
        "team_id",
        "team_name",
        "short_name",
        "avg_5gw_fdr",
        F.col("1_desc").alias("next_gw_1"),
        F.col("2_desc").alias("next_gw_2"),
        F.col("3_desc").alias("next_gw_3"),
        F.col("4_desc").alias("next_gw_4"),
        F.col("5_desc").alias("next_gw_5"),
        F.current_timestamp().alias("_updated_at")
    )

# COMMAND ----------
# Save to fpl.gold.fixture_planner
target_table = f"{db_gold}.fixture_planner"
fixture_planner.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Gold Fixture Planner table: {target_table} ({fixture_planner.count()} rows)")
display(fixture_planner.orderBy(F.col("avg_5gw_fdr").asc()))
