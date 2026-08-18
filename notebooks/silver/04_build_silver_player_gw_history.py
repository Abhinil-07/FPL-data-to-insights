# Databricks notebook source
# COMMAND ----------
# 04_build_silver_player_gw_history.py
#
# Phase 3 — Silver: Unified Gameweek Match Log Fact Table (fpl.silver.player_gw_history)
#
# Architecture: Two-Stream Union
#   Stream 1: Historical Archive (2023-24 to 2025-26)
#             Read bronze.archive_player_gws
#             + Join silver.player_crosswalk (maps element -> durable player_key)
#             + Standardize column types & metrics
#
#   Stream 2: Live Current Season (2026-27)
#             Read bronze.player_gw_history_raw (match stats from 1-call live endpoint)
#             + Join silver.players (attach player_key, name, team, position)
#             + Join silver.fixtures (attach is_home, opponent_team_id, match scores)
#             + Join bronze.players_gw_snapshot_raw (attach weekly price & ownership)
#             + Standardize to the exact same schema
#
#   Output:   fpl.silver.player_gw_history (~100,000+ rows across 4 seasons)
#             Overwritten cleanly on every run.

import os
import sys
import yaml
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, 
    DoubleType, BooleanType, TimestampType
)

sys.path.append(os.path.abspath("../../"))
sys.path.append(os.path.abspath("./"))

# COMMAND ----------
# Load config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze = config["databases"]["bronze"]
db_silver = config["databases"]["silver"]
current_season = config.get("current_season", "2026-27")
target_table = f"{db_silver}.player_gw_history"
ingested_at = datetime.utcnow()

print(f"Building unified Gameweek Fact Table: {target_table}")
print(f"Current season: {current_season}")

# COMMAND ----------
# ─────────────────────────────────────────────────────────────────────────────
# STREAM 1: Historical Archive Data (2023-24, 2024-25, 2025-26)
# ─────────────────────────────────────────────────────────────────────────────

print("\n--- Processing Stream 1: Historical Archive Data ---")

archive_gws = spark.read.table(f"{db_bronze}.archive_player_gws")
crosswalk   = spark.read.table(f"{db_silver}.player_crosswalk")

col_list = archive_gws.columns

# Handle possible historical naming variations gracefully
element_col = F.col("element") if "element" in col_list else F.col("element_id") if "element_id" in col_list else F.col("id")
xg_col      = F.col("xG") if "xG" in col_list else F.col("expected_goals") if "expected_goals" in col_list else F.lit(0.0)
xa_col      = F.col("xA") if "xA" in col_list else F.col("expected_assists") if "expected_assists" in col_list else F.lit(0.0)
xgi_col     = F.col("xGI") if "xGI" in col_list else F.col("expected_goal_involvements") if "expected_goal_involvements" in col_list else (xg_col + xa_col)
xgc_col     = F.col("xGC") if "xGC" in col_list else F.col("expected_goals_conceded") if "expected_goals_conceded" in col_list else F.lit(0.0)
starts_col  = F.col("starts") if "starts" in col_list else F.when(F.col("minutes").cast("int") > 0, F.lit(1)).otherwise(F.lit(0))
pos_col     = F.col("position") if "position" in col_list else F.lit("UNKNOWN")
team_col    = F.trim(F.col("team")) if "team" in col_list else F.lit("UNKNOWN")
val_col     = (F.col("value").cast("double") / 10.0) if "value" in col_list else F.lit(None).cast("double")
tin_col     = F.col("transfers_in").cast("int") if "transfers_in" in col_list else F.lit(0)
tout_col    = F.col("transfers_out").cast("int") if "transfers_out" in col_list else F.lit(0)
sel_col     = F.col("selected").cast("double") if "selected" in col_list else F.col("selected_by_percent").cast("double") if "selected_by_percent" in col_list else F.lit(0.0)
fixture_col = F.col("fixture").cast("int") if "fixture" in col_list else F.lit(None).cast("int")
kickoff_col = F.to_timestamp(F.col("kickoff_time")) if "kickoff_time" in col_list else F.lit(None).cast("timestamp")
th_score    = F.col("team_h_score").cast("int") if "team_h_score" in col_list else F.lit(None).cast("int")
ta_score    = F.col("team_a_score").cast("int") if "team_a_score" in col_list else F.lit(None).cast("int")

# Clean Archive rows
archive_prep = archive_gws.select(
    element_col.cast("double").cast("int").alias("source_player_id"),
    F.col("season"),
    F.col("GW").cast("double").cast("int").alias("gameweek"),
    fixture_col.alias("fixture_id"),
    kickoff_col.alias("kickoff_time"),
    F.trim(F.col("name")).alias("player_name"),
    pos_col.alias("position_name"),
    team_col.alias("team_name"),
    F.coalesce(F.col("was_home").cast("boolean"), F.lit(True)).alias("is_home"),
    F.coalesce(F.col("opponent_team").cast("double").cast("int"), F.lit(0)).alias("opponent_team_id"),
    th_score.alias("team_h_score"),
    ta_score.alias("team_a_score"),
    F.coalesce(F.col("total_points").cast("double").cast("int"), F.lit(0)).alias("total_points"),
    F.coalesce(F.col("minutes").cast("double").cast("int"), F.lit(0)).alias("minutes"),
    F.coalesce(starts_col.cast("double").cast("int"), F.lit(0)).alias("starts"),
    F.coalesce(F.col("goals_scored").cast("double").cast("int"), F.lit(0)).alias("goals_scored"),
    F.coalesce(F.col("assists").cast("double").cast("int"), F.lit(0)).alias("assists"),
    F.coalesce(F.col("clean_sheets").cast("double").cast("int"), F.lit(0)).alias("clean_sheets"),
    F.coalesce(F.col("goals_conceded").cast("double").cast("int"), F.lit(0)).alias("goals_conceded"),
    F.coalesce(F.col("own_goals").cast("double").cast("int"), F.lit(0)).alias("own_goals"),
    F.coalesce(F.col("penalties_saved").cast("double").cast("int"), F.lit(0)).alias("penalties_saved"),
    F.coalesce(F.col("penalties_missed").cast("double").cast("int"), F.lit(0)).alias("penalties_missed"),
    F.coalesce(F.col("yellow_cards").cast("double").cast("int"), F.lit(0)).alias("yellow_cards"),
    F.coalesce(F.col("red_cards").cast("double").cast("int"), F.lit(0)).alias("red_cards"),
    F.coalesce(F.col("saves").cast("double").cast("int"), F.lit(0)).alias("saves"),
    F.coalesce(F.col("bonus").cast("double").cast("int"), F.lit(0)).alias("bonus"),
    F.coalesce(F.col("bps").cast("double").cast("int"), F.lit(0)).alias("bps"),
    F.coalesce(xg_col.cast("double"), F.lit(0.0)).alias("expected_goals"),
    F.coalesce(xa_col.cast("double"), F.lit(0.0)).alias("expected_assists"),
    F.coalesce(xgi_col.cast("double"), F.lit(0.0)).alias("expected_goal_involvements"),
    F.coalesce(xgc_col.cast("double"), F.lit(0.0)).alias("expected_goals_conceded"),
    F.coalesce(F.col("ict_index").cast("double"), F.lit(0.0)).alias("ict_index"),
    F.coalesce(F.col("influence").cast("double"), F.lit(0.0)).alias("influence"),
    F.coalesce(F.col("creativity").cast("double"), F.lit(0.0)).alias("creativity"),
    F.coalesce(F.col("threat").cast("double"), F.lit(0.0)).alias("threat"),
    val_col.alias("value_gbp"),
    tin_col.alias("transfers_in_event"),
    tout_col.alias("transfers_out_event"),
    sel_col.alias("selected_by_percent"),
    F.col("_ingested_at")
)

# Read silver teams for team_id lookup
silver_teams = spark.read.table(f"{db_silver}.teams").select("team_id", "team_name")

# Join Crosswalk (INNER JOIN to keep active players only) and Teams (LEFT JOIN for team_id)
stream1_archive = archive_prep.join(
    crosswalk.select("player_key", "season", "source_player_id").distinct(),
    (archive_prep.source_player_id == crosswalk.source_player_id) & (archive_prep.season == crosswalk.season),
    "inner"
).join(
    silver_teams,
    archive_prep.team_name == silver_teams.team_name,
    "left"
).select(
    crosswalk.player_key.cast("int").alias("player_key"),
    archive_prep.player_name,
    archive_prep.position_name,
    F.coalesce(silver_teams.team_id, F.lit(0)).cast("int").alias("team_id"),
    archive_prep.team_name,
    archive_prep.season,
    archive_prep.gameweek,
    archive_prep.fixture_id,
    archive_prep.kickoff_time,
    archive_prep.is_home,
    archive_prep.opponent_team_id,
    archive_prep.team_h_score,
    archive_prep.team_a_score,
    archive_prep.total_points,
    archive_prep.minutes,
    archive_prep.starts,
    archive_prep.goals_scored,
    archive_prep.assists,
    archive_prep.clean_sheets,
    archive_prep.goals_conceded,
    archive_prep.own_goals,
    archive_prep.penalties_saved,
    archive_prep.penalties_missed,
    archive_prep.yellow_cards,
    archive_prep.red_cards,
    archive_prep.saves,
    archive_prep.bonus,
    archive_prep.bps,
    archive_prep.expected_goals,
    archive_prep.expected_assists,
    archive_prep.expected_goal_involvements,
    archive_prep.expected_goals_conceded,
    archive_prep.ict_index,
    archive_prep.influence,
    archive_prep.creativity,
    archive_prep.threat,
    archive_prep.value_gbp,
    archive_prep.transfers_in_event,
    archive_prep.transfers_out_event,
    archive_prep.selected_by_percent,
    archive_prep._ingested_at
)

archive_count = stream1_archive.count()
print(f"  Stream 1 ready: {archive_count:,} historical match rows processed.")

# COMMAND ----------
# ─────────────────────────────────────────────────────────────────────────────
# STREAM 2: Live Current Season Data (2026-27)
# ─────────────────────────────────────────────────────────────────────────────

print("\n--- Processing Stream 2: Live Current Season Data ---")

live_stream_exists = False
live_table_name = f"{db_bronze}.player_gw_history_raw"

if spark.catalog.tableExists(live_table_name):
    live_raw = spark.read.table(live_table_name)
    if live_raw.count() > 0:
        live_stream_exists = True

if live_stream_exists:
    silver_players  = spark.read.table(f"{db_silver}.players")
    silver_fixtures = spark.read.table(f"{db_silver}.fixtures")
    
    # Read snapshot if available
    snapshot_table_name = f"{db_bronze}.players_gw_snapshot_raw"
    if spark.catalog.tableExists(snapshot_table_name):
        snapshot_raw = spark.read.table(snapshot_table_name)
    else:
        snapshot_raw = None

    # Step A: Attach Player metadata (player_key, name, position, team)
    live_with_player = live_raw.join(
        silver_players.select(
            F.col("player_id").alias("p_id"),
            F.col("player_key"),
            F.col("web_name").alias("player_name"),
            F.col("position_name"),
            F.col("team_id"),
            F.col("team_name")
        ),
        live_raw.element.cast("int") == F.col("p_id"),
        "left"
    )

    # Step B: Attach Fixture context (is_home, opponent_team_id, match scores)
    # Join where fixture's gameweek matches round AND player's team is either home or away
    live_with_fixtures = live_with_player.join(
        silver_fixtures.select(
            F.col("gameweek").alias("f_gw"),
            F.col("fixture_id"),
            F.col("kickoff_time"),
            F.col("home_team_id"),
            F.col("away_team_id"),
            F.col("home_score").alias("f_home_score"),
            F.col("away_score").alias("f_away_score")
        ),
        (live_with_player.round.cast("int") == F.col("f_gw")) & 
        ((live_with_player.team_id == F.col("home_team_id")) | (live_with_player.team_id == F.col("away_team_id"))),
        "left"
    )

    # Step C: Attach Economic snapshot (price, transfers, ownership) if snapshot table exists
    if snapshot_raw is not None:
        live_with_snapshot = live_with_fixtures.join(
            snapshot_raw.select(
                F.col("element").cast("int").alias("s_element"),
                F.col("gw").cast("int").alias("s_gw"),
                (F.col("now_cost").cast("double") / 10.0).alias("s_value_gbp"),
                F.col("transfers_in_event").cast("int").alias("s_transfers_in"),
                F.col("transfers_out_event").cast("int").alias("s_transfers_out"),
                F.col("selected_by_percent").cast("double").alias("s_selected_pct")
            ),
            (live_with_fixtures.element.cast("int") == F.col("s_element")) & 
            (live_with_fixtures.round.cast("int") == F.col("s_gw")),
            "left"
        )
    else:
        live_with_snapshot = live_with_fixtures \
            .withColumn("s_value_gbp", F.lit(None).cast("double")) \
            .withColumn("s_transfers_in", F.lit(0)) \
            .withColumn("s_transfers_out", F.lit(0)) \
            .withColumn("s_selected_pct", F.lit(0.0))

    # Step D: Standardize Live stream columns to exact match Stream 1 schema
    is_home_calc = (live_with_snapshot.team_id == live_with_snapshot.home_team_id)
    opp_calc     = F.when(is_home_calc, live_with_snapshot.away_team_id).otherwise(live_with_snapshot.home_team_id)

    stream2_live = live_with_snapshot.select(
        F.coalesce(live_with_snapshot.player_key, live_with_snapshot.element.cast("int")).alias("player_key"),
        live_with_snapshot.player_name,
        live_with_snapshot.position_name,
        live_with_snapshot.team_id,
        live_with_snapshot.team_name,
        F.lit(current_season).alias("season"),
        live_with_snapshot.round.cast("int").alias("gameweek"),
        live_with_snapshot.fixture_id,
        live_with_snapshot.kickoff_time,
        F.coalesce(is_home_calc, F.lit(True)).alias("is_home"),
        F.coalesce(opp_calc, F.lit(0)).alias("opponent_team_id"),
        live_with_snapshot.f_home_score.alias("team_h_score"),
        live_with_snapshot.f_away_score.alias("team_a_score"),
        F.coalesce(live_with_snapshot.total_points.cast("int"), F.lit(0)).alias("total_points"),
        F.coalesce(live_with_snapshot.minutes.cast("int"), F.lit(0)).alias("minutes"),
        F.coalesce(live_with_snapshot.starts.cast("int"), F.lit(0)).alias("starts"),
        F.coalesce(live_with_snapshot.goals_scored.cast("int"), F.lit(0)).alias("goals_scored"),
        F.coalesce(live_with_snapshot.assists.cast("int"), F.lit(0)).alias("assists"),
        F.coalesce(live_with_snapshot.clean_sheets.cast("int"), F.lit(0)).alias("clean_sheets"),
        F.coalesce(live_with_snapshot.goals_conceded.cast("int"), F.lit(0)).alias("goals_conceded"),
        F.coalesce(live_with_snapshot.own_goals.cast("int"), F.lit(0)).alias("own_goals"),
        F.coalesce(live_with_snapshot.penalties_saved.cast("int"), F.lit(0)).alias("penalties_saved"),
        F.coalesce(live_with_snapshot.penalties_missed.cast("int"), F.lit(0)).alias("penalties_missed"),
        F.coalesce(live_with_snapshot.yellow_cards.cast("int"), F.lit(0)).alias("yellow_cards"),
        F.coalesce(live_with_snapshot.red_cards.cast("int"), F.lit(0)).alias("red_cards"),
        F.coalesce(live_with_snapshot.saves.cast("int"), F.lit(0)).alias("saves"),
        F.coalesce(live_with_snapshot.bonus.cast("int"), F.lit(0)).alias("bonus"),
        F.coalesce(live_with_snapshot.bps.cast("int"), F.lit(0)).alias("bps"),
        F.coalesce(live_with_snapshot.expected_goals.cast("double"), F.lit(0.0)).alias("expected_goals"),
        F.coalesce(live_with_snapshot.expected_assists.cast("double"), F.lit(0.0)).alias("expected_assists"),
        F.coalesce(live_with_snapshot.expected_goal_involvements.cast("double"), F.lit(0.0)).alias("expected_goal_involvements"),
        F.coalesce(live_with_snapshot.expected_goals_conceded.cast("double"), F.lit(0.0)).alias("expected_goals_conceded"),
        F.coalesce(live_with_snapshot.ict_index.cast("double"), F.lit(0.0)).alias("ict_index"),
        F.coalesce(live_with_snapshot.influence.cast("double"), F.lit(0.0)).alias("influence"),
        F.coalesce(live_with_snapshot.creativity.cast("double"), F.lit(0.0)).alias("creativity"),
        F.coalesce(live_with_snapshot.threat.cast("double"), F.lit(0.0)).alias("threat"),
        live_with_snapshot.s_value_gbp.alias("value_gbp"),
        F.coalesce(live_with_snapshot.s_transfers_in, F.lit(0)).alias("transfers_in_event"),
        F.coalesce(live_with_snapshot.s_transfers_out, F.lit(0)).alias("transfers_out_event"),
        F.coalesce(live_with_snapshot.s_selected_pct, F.lit(0.0)).alias("selected_by_percent"),
        F.to_timestamp(live_with_snapshot._ingested_at).alias("_ingested_at")
    )
    
    live_count = stream2_live.count()
    print(f"  Stream 2 ready: {live_count:,} live match rows processed for {current_season}.")
else:
    # If no live data yet (pre-season), create an empty matching DataFrame
    stream2_live = spark.createDataFrame([], stream1_archive.schema)
    print(f"  Stream 2: 0 rows (Pre-season mode).")

# COMMAND ----------
# ─────────────────────────────────────────────────────────────────────────────
# UNION & PERSISTENCE
# ─────────────────────────────────────────────────────────────────────────────

print("\n--- Combining Streams and Overwriting Fact Table ---")

unified_fact_df = stream1_archive.unionByName(stream2_live)

unified_fact_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

total_rows = unified_fact_df.count()
print(f"✅ Successfully written unified Gameweek Fact Table: {target_table}")
print(f"   Total Fact Rows: {total_rows:,} across all seasons.")

# COMMAND ----------
# Post-run verification
print("\n--- Row Counts by Season ---")
spark.sql(f"""
    SELECT 
        season,
        COUNT(*) AS total_match_rows,
        COUNT(DISTINCT player_key) AS unique_players,
        MIN(gameweek) AS min_gw,
        MAX(gameweek) AS max_gw
    FROM {target_table}
    GROUP BY season
    ORDER BY season
""").display()

print("\n--- Sample Fact Rows Preview ---")
display(unified_fact_df.select(
    "player_key", "player_name", "position_name", "season", "gameweek",
    "is_home", "opponent_team_id", "total_points", "minutes", "goals_scored", "assists",
    "expected_goals", "expected_assists", "value_gbp"
).limit(20))
