# Databricks notebook source
# COMMAND ----------
# 03_build_silver_players.py
# Phase 3: Build fpl.silver.players dimension table with cleaned prices, position names, and team joins.

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
# Read Bronze raw players and Silver teams
players_raw = spark.read.table(f"{db_bronze}.players_raw")
teams_silver = spark.read.table(f"{db_silver}.teams")

# COMMAND ----------
# Transform & Clean Players Dimension
players_cleaned = players_raw.join(
    teams_silver.select("team_id", "team_name", "short_name"),
    players_raw.team == teams_silver.team_id,
    "left"
).select(
    # ── 1. Core Identity & Dimensions ──
    F.col("code").cast("int").alias("player_key"),
    F.col("id").cast("int").alias("player_id"),
    F.trim(F.col("first_name")).alias("first_name"),
    F.trim(F.col("second_name")).alias("second_name"),
    F.trim(F.col("web_name")).alias("web_name"),
    F.col("team").cast("int").alias("team_id"),
    F.col("team_name"),
    F.col("short_name").alias("team_short_name"),
    F.col("element_type").cast("int").alias("position_id"),
    F.when(F.col("element_type") == 1, "GKP")
     .when(F.col("element_type") == 2, "DEF")
     .when(F.col("element_type") == 3, "MID")
     .when(F.col("element_type") == 4, "FWD")
     .otherwise("UNKNOWN").alias("position_name"),

    # ── 2. Availability & Injury Status ──
    F.col("status").alias("status"),
    F.col("news").alias("news"),
    F.col("news_added").alias("news_added"),
    F.col("chance_of_playing_next_round").cast("int").alias("chance_of_playing_next_round"),
    F.col("chance_of_playing_this_round").cast("int").alias("chance_of_playing_this_round"),

    # ── 3. Price & Economic Metrics ──
    (F.col("now_cost").cast("double") / 10.0).alias("price_gbp"),
    (F.col("cost_change_event").cast("double") / 10.0).alias("cost_change_event_gbp"),
    (F.col("cost_change_start").cast("double") / 10.0).alias("cost_change_start_gbp"),
    F.col("selected_by_percent").cast("double").alias("ownership_percent"),
    F.col("transfers_in").cast("int").alias("transfers_in_total"),
    F.col("transfers_out").cast("int").alias("transfers_out_total"),
    F.col("transfers_in_event").cast("int").alias("transfers_in_event"),
    F.col("transfers_out_event").cast("int").alias("transfers_out_event"),
    F.col("value_form").cast("double").alias("value_form"),
    F.col("value_season").cast("double").alias("value_season"),

    # ── 4. Current Season Performance ──
    F.col("total_points").cast("int").alias("total_points"),
    F.col("event_points").cast("int").alias("event_points"),
    F.col("points_per_game").cast("double").alias("points_per_game"),
    F.col("form").cast("double").alias("form"),
    F.col("minutes").cast("int").alias("minutes"),
    F.col("starts").cast("int").alias("starts"),
    F.col("goals_scored").cast("int").alias("goals_scored"),
    F.col("assists").cast("int").alias("assists"),
    F.col("clean_sheets").cast("int").alias("clean_sheets"),
    F.col("goals_conceded").cast("int").alias("goals_conceded"),
    F.col("own_goals").cast("int").alias("own_goals"),
    F.col("penalties_saved").cast("int").alias("penalties_saved"),
    F.col("penalties_missed").cast("int").alias("penalties_missed"),
    F.col("yellow_cards").cast("int").alias("yellow_cards"),
    F.col("red_cards").cast("int").alias("red_cards"),
    F.col("saves").cast("int").alias("saves"),
    F.col("bonus").cast("int").alias("bonus"),
    F.col("bps").cast("int").alias("bps"),
    F.col("dreamteam_count").cast("int").alias("dreamteam_count"),
    F.col("in_dreamteam").cast("boolean").alias("in_dreamteam"),

    # ── 5. Underlying Stats (xG, xA, xGI, xGC) ──
    F.col("expected_goals").cast("double").alias("expected_goals"),
    F.col("expected_assists").cast("double").alias("expected_assists"),
    F.col("expected_goal_involvements").cast("double").alias("expected_goal_involvements"),
    F.col("expected_goals_conceded").cast("double").alias("expected_goals_conceded"),
    F.col("expected_goals_per_90").cast("double").alias("expected_goals_per_90"),
    F.col("expected_assists_per_90").cast("double").alias("expected_assists_per_90"),
    F.col("expected_goal_involvements_per_90").cast("double").alias("expected_goal_involvements_per_90"),
    F.col("expected_goals_conceded_per_90").cast("double").alias("expected_goals_conceded_per_90"),
    F.col("clean_sheets_per_90").cast("double").alias("clean_sheets_per_90"),
    F.col("saves_per_90").cast("double").alias("saves_per_90"),
    F.col("goals_conceded_per_90").cast("double").alias("goals_conceded_per_90"),

    # ── 6. ICT Index ──
    F.col("influence").cast("double").alias("influence"),
    F.col("creativity").cast("double").alias("creativity"),
    F.col("threat").cast("double").alias("threat"),
    F.col("ict_index").cast("double").alias("ict_index"),

    # ── 7. Set-Piece Orders ──
    F.col("penalties_order").cast("int").alias("penalties_order"),
    F.col("direct_freekicks_order").cast("int").alias("direct_freekicks_order"),
    F.col("corners_and_indirect_freekicks_order").cast("int").alias("corners_and_indirect_freekicks_order"),

    # ── 8. Overall Ranks ──
    F.col("ict_index_rank").cast("int").alias("ict_index_rank"),
    F.col("form_rank").cast("int").alias("form_rank"),
    F.col("selected_rank").cast("int").alias("selected_rank"),
    F.col("points_per_game_rank").cast("int").alias("points_per_game_rank"),

    # ── 9. Pipeline Metadata ──
    F.col("_ingested_at")
)

# COMMAND ----------
# Save to fpl.silver.players
target_table = f"{db_silver}.players"
players_cleaned.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully written Silver players dimension table: {target_table} ({players_cleaned.count()} rows)")
display(players_cleaned.select(
    "player_key", "web_name", "team_short_name", "position_name", 
    "price_gbp", "status", "chance_of_playing_next_round",
    "total_points", "form", "expected_goals", "expected_assists", 
    "penalties_order", "ownership_percent"
).limit(15))
