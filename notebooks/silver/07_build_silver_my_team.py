# Databricks notebook source
# COMMAND ----------
# 07_build_silver_my_team.py
# Phase 3 — Silver: Personal Squad Tracker (fpl.silver.my_team_history)
#
# Source: fpl.bronze.my_team_raw (populated if fpl_team_id is configured)
# Output: fpl.silver.my_team_history
#
# Powers Requirement #9 (Personal Squad Tracker):
#   - Overall rank progression over time
#   - Weekly gameweek points and season total
#   - Available bank balance & team value
#   - Gracefully creates empty conformed table if team ID is not yet configured.

import os
import sys
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, 
    DoubleType, BooleanType, TimestampType
)

# COMMAND ----------
# Load config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze = config["databases"]["bronze"]
db_silver = config["databases"]["silver"]
target_table = f"{db_silver}.my_team_history"

print(f"Building Personal Squad Tracker: {target_table}")

# COMMAND ----------
# Define standard schema for my_team_history
team_history_schema = StructType([
    StructField("entry_id", IntegerType(), True),
    StructField("entry_name", StringType(), True),
    StructField("player_first_name", StringType(), True),
    StructField("player_last_name", StringType(), True),
    StructField("current_gameweek", IntegerType(), True),
    StructField("overall_points", IntegerType(), True),
    StructField("overall_rank", IntegerType(), True),
    StructField("event_points", IntegerType(), True),
    StructField("event_rank", IntegerType(), True),
    StructField("bank_gbp", DoubleType(), True),
    StructField("team_value_gbp", DoubleType(), True),
    StructField("total_transfers", IntegerType(), True),
    StructField("_ingested_at", TimestampType(), True)
])

# COMMAND ----------
# Check if bronze.my_team_raw exists and has data
table_name = f"{db_bronze}.my_team_raw"
has_data = False

if spark.catalog.tableExists(table_name):
    raw_df = spark.read.table(table_name)
    if raw_df.count() > 0:
        has_data = True

if has_data:
    cols = raw_df.columns
    
    id_col    = F.col("id").cast("int") if "id" in cols else F.lit(None).cast("int")
    name_col  = F.trim(F.col("name")) if "name" in cols else F.lit(None).cast("string")
    fn_col    = F.trim(F.col("player_first_name")) if "player_first_name" in cols else F.lit(None).cast("string")
    ln_col    = F.trim(F.col("player_last_name")) if "player_last_name" in cols else F.lit(None).cast("string")
    gw_col    = F.col("current_event").cast("int") if "current_event" in cols else F.col("summary_event").cast("int") if "summary_event" in cols else F.lit(0)
    pts_col   = F.col("summary_overall_points").cast("int") if "summary_overall_points" in cols else F.col("total_points").cast("int") if "total_points" in cols else F.lit(0)
    rank_col  = F.col("summary_overall_rank").cast("int") if "summary_overall_rank" in cols else F.col("overall_rank").cast("int") if "overall_rank" in cols else F.lit(None).cast("int")
    epts_col  = F.col("summary_event_points").cast("int") if "summary_event_points" in cols else F.col("event_points").cast("int") if "event_points" in cols else F.lit(0)
    erank_col = F.col("summary_event_rank").cast("int") if "summary_event_rank" in cols else F.col("event_rank").cast("int") if "event_rank" in cols else F.lit(None).cast("int")
    bank_col  = (F.col("last_deadline_bank").cast("double") / 10.0) if "last_deadline_bank" in cols else F.lit(0.0)
    val_col   = (F.col("last_deadline_value").cast("double") / 10.0) if "last_deadline_value" in cols else F.lit(0.0)
    tx_col    = F.col("last_deadline_total_transfers").cast("int") if "last_deadline_total_transfers" in cols else F.lit(0)

    silver_my_team = raw_df.select(
        id_col.alias("entry_id"),
        name_col.alias("entry_name"),
        fn_col.alias("player_first_name"),
        ln_col.alias("player_last_name"),
        gw_col.alias("current_gameweek"),
        pts_col.alias("overall_points"),
        rank_col.alias("overall_rank"),
        epts_col.alias("event_points"),
        erank_col.alias("event_rank"),
        bank_col.alias("bank_gbp"),
        val_col.alias("team_value_gbp"),
        tx_col.alias("total_transfers"),
        F.to_timestamp(F.col("_ingested_at")).alias("_ingested_at")
    )
    print(f"  Processed personal squad data ({silver_my_team.count()} rows).")
else:
    silver_my_team = spark.createDataFrame([], team_history_schema)
    print("  Note: No personal squad data available in bronze.my_team_raw (fpl_team_id not set). Created empty conformed table.")

# COMMAND ----------
# Save to fpl.silver.my_team_history
silver_my_team.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

print(f"✅ Successfully created Silver My Team table: {target_table} ({silver_my_team.count()} rows)")
display(silver_my_team)
