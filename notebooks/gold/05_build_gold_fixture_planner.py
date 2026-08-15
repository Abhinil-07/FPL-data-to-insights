# Databricks notebook source
# COMMAND ----------
# 05_build_gold_fixture_planner.py
# Phase 4: Build fpl.gold.fixture_planner with Mid-Season (19-Gameweek) FDR Matrix per Team.
# Generates wide next_gw_1 through next_gw_19 text columns and numeric fdr_gw_1 through fdr_gw_19 for Power BI.

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
# Unpivot fixtures into team perspective (Home and Away) for all upcoming fixtures
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
# Format fixture display e.g. "ARS (H) FDR:2"
formatted_fixtures = all_upcoming.withColumn(
    "fixture_desc",
    F.concat(F.col("opponent_short"), F.lit(" ("), F.col("venue"), F.lit(") FDR:"), F.col("fdr"))
)

# Window to order upcoming gameweeks per team up to 19 (Mid-Season)
window_team_gw = Window.partitionBy("team_id").orderBy("gameweek")

next_19_fixtures = formatted_fixtures.withColumn("gw_order", F.row_number().over(window_team_gw)) \
    .filter(F.col("gw_order") <= 19)

# Pivot wide by gw_order (1 through 19)
pivot_list = list(range(1, 20))

pivoted_desc = next_19_fixtures.groupBy("team_id") \
    .pivot("gw_order", pivot_list) \
    .agg(F.first("fixture_desc"))

pivoted_fdr = next_19_fixtures.groupBy("team_id") \
    .pivot("gw_order", pivot_list) \
    .agg(F.first("fdr"))

# Calculate 5-GW and 19-GW average FDR
avg_5gw = next_19_fixtures.filter(F.col("gw_order") <= 5).groupBy("team_id").agg(F.round(F.avg("fdr"), 2).alias("avg_5gw_fdr"))
avg_19gw = next_19_fixtures.groupBy("team_id").agg(F.round(F.avg("fdr"), 2).alias("avg_midseason_fdr"))

# Build Dynamic Select Expressions
select_exprs = ["team_id", "team_name", "short_name", "avg_5gw_fdr", "avg_midseason_fdr"]

# Add text columns next_gw_1 .. next_gw_19
desc_renamed = pivoted_desc
for i in range(1, 20):
    desc_renamed = desc_renamed.withColumnRenamed(str(i), f"next_gw_{i}")

fdr_renamed = pivoted_fdr
for i in range(1, 20):
    fdr_renamed = fdr_renamed.withColumnRenamed(str(i), f"fdr_gw_{i}")

# Join all components
fixture_planner = teams.select("team_id", "team_name", "short_name") \
    .join(avg_5gw, "team_id", "left") \
    .join(avg_19gw, "team_id", "left") \
    .join(desc_renamed, "team_id", "left") \
    .join(fdr_renamed, "team_id", "left") \
    .withColumn("_updated_at", F.current_timestamp())

# COMMAND ----------
# Save to fpl.gold.fixture_planner
target_table = f"{db_gold}.fixture_planner"
fixture_planner.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully updated Gold Fixture Planner table with 19 Mid-Season Gameweeks: {target_table} ({fixture_planner.count()} rows)")
display(fixture_planner.orderBy(F.col("avg_5gw_fdr").asc()))
