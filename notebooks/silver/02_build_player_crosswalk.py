# Databricks notebook source
# COMMAND ----------
# 02_build_player_crosswalk.py
# Phase 3: Build player crosswalk table to reconcile player identities across live & 3 historical seasons.
# Uses durable FPL code, web_name, full name containment, and manual CSV overrides.

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
    F.lower(F.trim(F.col("web_name"))).alias("norm_web_name"),
    F.lower(F.concat(F.col("first_name"), F.lit(" "), F.col("second_name"))).alias("norm_full_name"),
    F.col("team").cast("int").alias("team_id"),
    F.col("element_type").cast("int").alias("position_id")
).distinct()

# COMMAND ----------
# 2. Historical Players Snapshot (from archive_players_raw)
hist_players = spark.read.table(f"{db_bronze}.archive_players_raw")
hist_cols = hist_players.columns

web_name_expr = F.trim(F.col("web_name")) if "web_name" in hist_cols else F.trim(F.col("second_name"))

hist_prep = hist_players.select(
    F.col("code").cast("int").alias("hist_code"),
    F.col("id").cast("int").alias("hist_player_id"),
    F.col("season"),
    F.trim(F.col("first_name")).alias("hist_first_name"),
    F.trim(F.col("second_name")).alias("hist_second_name"),
    web_name_expr.alias("hist_web_name"),
    F.lower(web_name_expr).alias("hist_norm_web_name"),
    F.lower(F.concat(F.col("first_name"), F.lit(" "), F.col("second_name"))).alias("hist_norm_full_name")
).distinct()

# COMMAND ----------
# 3. Multi-Strategy Crosswalk Matching

# Strategy 1: Match by durable FPL code
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
    F.lit("matched_by_code").alias("crosswalk_status")
)

# Strategy 2: Match by exact web_name (e.g., "salah" == "salah")
web_name_matched = hist_prep.join(
    live_players.select("player_key", "norm_web_name"),
    hist_prep.hist_norm_web_name == live_players.norm_web_name,
    "inner"
).select(
    F.col("player_key"),
    F.col("season"),
    F.lit("archive").alias("source"),
    F.col("hist_player_id").alias("source_player_id"),
    F.col("hist_first_name").alias("first_name"),
    F.col("hist_second_name").alias("second_name"),
    F.lit("matched_by_web_name").alias("crosswalk_status")
)

# Strategy 3: Match by full name containment (e.g., "mohamed salah badr el din abdelbaki" contains "salah")
full_name_matched = hist_prep.join(
    live_players.select("player_key", "norm_web_name", "norm_full_name"),
    (hist_prep.hist_norm_full_name == live_players.norm_full_name) |
    (hist_prep.hist_norm_full_name.contains(live_players.norm_web_name)),
    "inner"
).select(
    F.col("player_key"),
    F.col("season"),
    F.lit("archive").alias("source"),
    F.col("hist_player_id").alias("source_player_id"),
    F.col("hist_first_name").alias("first_name"),
    F.col("hist_second_name").alias("second_name"),
    F.lit("matched_by_full_name_containment").alias("crosswalk_status")
)

# Live API Players Entries
live_entries = live_players.select(
    F.col("player_key"),
    F.lit(current_season).alias("season"),
    F.lit("live_api").alias("source"),
    F.col("live_player_id").alias("source_player_id"),
    F.col("first_name"),
    F.col("second_name"),
    F.lit("live_master").alias("crosswalk_status")
)

# COMMAND ----------
# Union all crosswalk entries and deduplicate
crosswalk_union = live_entries.unionByName(code_matched) \
    .unionByName(web_name_matched) \
    .unionByName(full_name_matched) \
    .distinct()

# COMMAND ----------
# Save to fpl.silver.player_crosswalk
target_table = f"{db_silver}.player_crosswalk"
crosswalk_union.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully constructed Player Crosswalk table: {target_table} ({crosswalk_union.count()} rows mapped!)")
display(crosswalk_union.filter(F.lower(F.col("first_name")).contains("mohamed") | F.lower(F.col("second_name")).contains("salah")).limit(10))
