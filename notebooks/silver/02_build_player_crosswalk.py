# Databricks notebook source
# COMMAND ----------
# 02_build_player_crosswalk.py
# Phase 3: Build player crosswalk table with strict priority deduplication per season per source player.

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

db_bronze = config["databases"]["bronze"]
db_silver = config["databases"]["silver"]
current_season = config.get("current_season", "2026-27")

print(f"Building Player Crosswalk in: {db_silver}.player_crosswalk")

# COMMAND ----------
# 1. Live Players Master Dimension (using durable FPL 'code' as primary player_key)
live_players = spark.read.table(f"{db_bronze}.players_raw").select(
    F.col("code").cast("int").alias("player_key"),
    F.col("id").cast("int").alias("live_player_id"),
    F.trim(F.col("first_name")).alias("first_name"),
    F.trim(F.col("second_name")).alias("second_name"),
    F.trim(F.col("web_name")).alias("web_name"),
    F.lower(F.concat(F.trim(F.col("first_name")), F.lit(" "), F.trim(F.col("second_name")))).alias("norm_full_name"),
    F.col("team").cast("int").alias("team_id"),
    F.col("element_type").cast("int").alias("position_id")
).distinct()

# COMMAND ----------
# 2. Historical Players Snapshot (from archive_players_raw)
hist_players = spark.read.table(f"{db_bronze}.archive_players_raw")

hist_prep = hist_players.select(
    F.col("code").cast("double").cast("int").alias("hist_code"),
    F.col("id").cast("double").cast("int").alias("hist_player_id"),
    F.col("season"),
    F.trim(F.col("first_name")).alias("hist_first_name"),
    F.trim(F.col("second_name")).alias("hist_second_name"),
    F.lower(F.concat(F.trim(F.col("first_name")), F.lit(" "), F.trim(F.col("second_name")))).alias("hist_norm_full_name")
).distinct()

# COMMAND ----------
# 3. Crosswalk Matching Strategies

# Strategy 1: Match by durable FPL code (100% unique per player!)
code_matched = hist_prep.join(
    live_players.select("player_key", "norm_full_name"),
    hist_prep.hist_code == live_players.player_key,
    "inner"
).select(
    F.col("player_key"),
    F.col("season"),
    F.lit("archive").alias("source"),
    F.col("hist_player_id").alias("source_player_id"),
    F.col("hist_first_name").alias("first_name"),
    F.col("hist_second_name").alias("second_name"),
    F.lit("matched_by_durable_code").alias("crosswalk_status"),
    F.lit(1).alias("priority")
)

# Strategy 2: Match by exact FULL NAME (first_name + second_name)
full_name_matched = hist_prep.join(
    live_players.select("player_key", "norm_full_name"),
    hist_prep.hist_norm_full_name == live_players.norm_full_name,
    "inner"
).select(
    F.col("player_key"),
    F.col("season"),
    F.lit("archive").alias("source"),
    F.col("hist_player_id").alias("source_player_id"),
    F.col("hist_first_name").alias("first_name"),
    F.col("hist_second_name").alias("second_name"),
    F.lit("matched_by_full_name").alias("crosswalk_status"),
    F.lit(2).alias("priority")
)

# Live API Players Master Entries
live_entries = live_players.select(
    F.col("player_key"),
    F.lit(current_season).alias("season"),
    F.lit("live_api").alias("source"),
    F.col("live_player_id").alias("source_player_id"),
    F.col("first_name"),
    F.col("second_name"),
    F.lit("live_master").alias("crosswalk_status"),
    F.lit(0).alias("priority")
)

# COMMAND ----------
# Union all crosswalk entries and deduplicate by highest matching priority
raw_union = live_entries.unionByName(code_matched).unionByName(full_name_matched)

windowSpec = Window.partitionBy("season", "source_player_id").orderBy("priority")

crosswalk_dedup = raw_union.withColumn("row_num", F.row_number().over(windowSpec)) \
    .filter(F.col("row_num") == 1) \
    .drop("priority", "row_num")

# COMMAND ----------
# Save to fpl.silver.player_crosswalk
target_table = f"{db_silver}.player_crosswalk"
crosswalk_dedup.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully constructed Player Crosswalk table: {target_table} ({crosswalk_dedup.count()} deduplicated rows mapped cleanly!)")
display(crosswalk_dedup.filter(F.lower(F.col("second_name")) == "palmer").select("player_key", "first_name", "second_name", "season", "crosswalk_status"))
