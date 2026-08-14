# Databricks notebook source
# COMMAND ----------
# 04_build_silver_player_gw_history.py
# Phase 3: Build fpl.silver.player_gw_history by unioning gameweek logs across 3 seasons with durable player_key & standardized xG/xA.

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
# Read Bronze historical GW data and Player Crosswalk
archive_gws = spark.read.table(f"{db_bronze}.archive_player_gws")
crosswalk = spark.read.table(f"{db_silver}.player_crosswalk")

# COMMAND ----------
# Extract column list to handle dynamic xG/xA column names safely
col_list = archive_gws.columns

xg_expr = F.col("xG") if "xG" in col_list else F.col("expected_goals") if "expected_goals" in col_list else F.lit(0.0)
xa_expr = F.col("xA") if "xA" in col_list else F.col("expected_assists") if "expected_assists" in col_list else F.lit(0.0)
ict_expr = F.col("ict_index") if "ict_index" in col_list else F.lit(0.0)
was_home_expr = F.col("was_home") if "was_home" in col_list else F.lit(None)

# COMMAND ----------
# Standardize historical gameweeks
gws_clean = archive_gws.select(
    F.col("element").cast("int").alias("source_player_id"),
    F.col("season"),
    F.col("GW").cast("int").alias("gameweek"),
    F.trim(F.col("name")).alias("raw_player_name"),
    F.col("total_points").cast("int").alias("points"),
    F.col("minutes").cast("int").alias("minutes"),
    F.col("goals_scored").cast("int").alias("goals_scored"),
    F.col("assists").cast("int").alias("assists"),
    F.col("clean_sheets").cast("int").alias("clean_sheets"),
    F.col("goals_conceded").cast("int").alias("goals_conceded"),
    F.col("saves").cast("int").alias("saves"),
    F.col("bonus").cast("int").alias("bonus"),
    F.col("bps").cast("int").alias("bps"),
    xg_expr.cast("double").alias("xg"),
    xa_expr.cast("double").alias("xa"),
    ict_expr.cast("double").alias("ict_index"),
    was_home_expr.cast("boolean").alias("is_home"),
    F.col("opponent_team").cast("int").alias("opponent_team_id"),
    F.col("_ingested_at")
)

# COMMAND ----------
# Join with Player Crosswalk to attach durable player_key
silver_gw_history = gws_clean.join(
    crosswalk.select("player_key", "season", "source_player_id").distinct(),
    (gws_clean.source_player_id == crosswalk.source_player_id) & (gws_clean.season == crosswalk.season),
    "left"
).select(
    F.coalesce(crosswalk.player_key, gws_clean.source_player_id).alias("player_key"),
    gws_clean.season,
    gws_clean.gameweek,
    gws_clean.raw_player_name,
    gws_clean.points,
    gws_clean.minutes,
    gws_clean.goals_scored,
    gws_clean.assists,
    gws_clean.clean_sheets,
    gws_clean.goals_conceded,
    gws_clean.saves,
    gws_clean.bonus,
    gws_clean.bps,
    gws_clean.xg,
    gws_clean.xa,
    gws_clean.ict_index,
    gws_clean.is_home,
    gws_clean.opponent_team_id,
    gws_clean._ingested_at
)

# COMMAND ----------
# Save to fpl.silver.player_gw_history
target_table = f"{db_silver}.player_gw_history"
silver_gw_history.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Silver Gameweek History table: {target_table} ({silver_gw_history.count()} rows)")
display(silver_gw_history.select("player_key", "raw_player_name", "season", "gameweek", "points", "minutes", "goals_scored", "assists", "xg", "xa", "ict_index").limit(20))
