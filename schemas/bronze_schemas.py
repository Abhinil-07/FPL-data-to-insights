"""
PySpark schema definitions for Bronze Delta tables in Databricks.
Bronze layer stores raw JSON payloads / raw attributes along with an `_ingested_at` timestamp.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType, BooleanType

# Bronze Database Name
BRONZE_DB = "fpl_bronze"

# bronze.players_raw
BRONZE_PLAYERS_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("second_name", StringType(), True),
    StructField("web_name", StringType(), True),
    StructField("team", IntegerType(), True),
    StructField("element_type", IntegerType(), True), # 1: GKP, 2: DEF, 3: MID, 4: FWD
    StructField("now_cost", IntegerType(), True),
    StructField("selected_by_percent", StringType(), True),
    StructField("form", StringType(), True),
    StructField("points_per_game", StringType(), True),
    StructField("total_points", IntegerType(), True),
    StructField("minutes", IntegerType(), True),
    StructField("goals_scored", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("clean_sheets", IntegerType(), True),
    StructField("goals_conceded", IntegerType(), True),
    StructField("own_goals", IntegerType(), True),
    StructField("penalties_saved", IntegerType(), True),
    StructField("penalties_missed", IntegerType(), True),
    StructField("yellow_cards", IntegerType(), True),
    StructField("red_cards", IntegerType(), True),
    StructField("saves", IntegerType(), True),
    StructField("bonus", IntegerType(), True),
    StructField("bps", IntegerType(), True),
    StructField("influence", StringType(), True),
    StructField("creativity", StringType(), True),
    StructField("threat", StringType(), True),
    StructField("ict_index", StringType(), True),
    StructField("transfers_in_event", IntegerType(), True),
    StructField("transfers_out_event", IntegerType(), True),
    StructField("_ingested_at", TimestampType(), False)
])

# bronze.teams_raw
BRONZE_TEAMS_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("short_name", StringType(), True),
    StructField("strength", IntegerType(), True),
    StructField("strength_overall_home", IntegerType(), True),
    StructField("strength_overall_away", IntegerType(), True),
    StructField("strength_attack_home", IntegerType(), True),
    StructField("strength_attack_away", IntegerType(), True),
    StructField("strength_defence_home", IntegerType(), True),
    StructField("strength_defence_away", IntegerType(), True),
    StructField("_ingested_at", TimestampType(), False)
])

# bronze.events_raw (Gameweeks)
BRONZE_EVENTS_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("deadline_time", StringType(), True),
    StructField("average_entry_score", IntegerType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("is_next", BooleanType(), True),
    StructField("is_previous", BooleanType(), True),
    StructField("finished", BooleanType(), True),
    StructField("highest_score", IntegerType(), True),
    StructField("_ingested_at", TimestampType(), False)
])

# bronze.fixtures_raw
BRONZE_FIXTURES_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", IntegerType(), True),
    StructField("event", IntegerType(), True),
    StructField("finished", BooleanType(), True),
    StructField("kickoff_time", StringType(), True),
    StructField("team_h", IntegerType(), True),
    StructField("team_a", IntegerType(), True),
    StructField("team_h_score", IntegerType(), True),
    StructField("team_a_score", IntegerType(), True),
    StructField("team_h_difficulty", IntegerType(), True),
    StructField("team_a_difficulty", IntegerType(), True),
    StructField("_ingested_at", TimestampType(), False)
])
