# Databricks notebook source
# COMMAND ----------
# 09_build_gold_my_squad_tracker.py
# Phase 4: Build fpl.gold.my_squad_tracker for personal squad rank and points tracking (Graceful Fallback if team_id missing).

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
db_gold = config["databases"]["gold"]
team_id = config.get("fpl_team_id")

# COMMAND ----------
target_table = f"{db_gold}.my_squad_tracker"

if team_id and spark.catalog.tableExists(f"{db_bronze}.my_team_raw"):
    my_team_raw = spark.read.table(f"{db_bronze}.my_team_raw")
    my_squad_tracker = my_team_raw.select(
        F.lit(team_id).alias("team_id"),
        F.col("name").alias("team_name"),
        F.col("player_first_name"),
        F.col("player_last_name"),
        F.col("summary_overall_points").cast("int").alias("overall_points"),
        F.col("summary_overall_rank").cast("int").alias("overall_rank"),
        F.col("summary_event_points").cast("int").alias("event_points"),
        F.col("summary_event_rank").cast("int").alias("event_rank"),
        F.col("_ingested_at").alias("_updated_at")
    )
    my_squad_tracker.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)
    print(f"✅ Successfully written personal squad tracker data to: {target_table}")
    display(my_squad_tracker)
else:
    # Create empty schema graceful placeholder
    print("ℹ️ No FPL Team ID configured. Creating empty placeholder table for fpl.gold.my_squad_tracker.")
    empty_df = spark.createDataFrame([], "team_id INT, team_name STRING, player_first_name STRING, player_last_name STRING, overall_points INT, overall_rank INT, event_points INT, event_rank INT, _updated_at TIMESTAMP")
    empty_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)
    print(f"✅ Created empty graceful table: {target_table}")
