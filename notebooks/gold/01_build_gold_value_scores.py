# Databricks notebook source
# COMMAND ----------
# 01_build_gold_value_scores.py
# Phase 4 — Gold: Position-Normalized Player Valuation & Strategy Matrix
#
# Builds: fpl.gold.value_scores
# Delivers:
#   1. quality_score & position_quality_rank: True player quality (Haaland, Bruno, Gabriel, Palmer, Raya).
#   2. value_score & position_value_rank: Budget efficiency ROI (Points / Output per £m).
#   3. strategy_tier: 🛡️ Season Anchor (Set & Forget) vs 🔄 Rolling Transfer Target.
#   4. Pre-Season & In-Season Adaptability: Automatic baseline from historical season when live minutes == 0.
#   5. Safety-Net Baseline: Last season primary (≥10 matches), career PL fallback (<10 matches),
#      FPL official estimate for brand-new signings with 0 PL history.

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
current_season = config.get("current_season", "2026-27")
target_table = f"{db_gold}.value_scores"

print(f"Reading from Silver: {db_silver}")
print(f"Writing to Gold:     {target_table}")
print(f"Current season:      {current_season}")

# COMMAND ----------
# 1. Read Silver tables
players = spark.read.table(f"{db_silver}.players")
fixtures = spark.read.table(f"{db_silver}.fixtures")
player_gw_history = spark.read.table(f"{db_silver}.player_gw_history")

# COMMAND ----------
# 2. Upcoming 5-Gameweek Fixture Difficulty Ease per team (Ordered chronologically by Gameweek)
#
# HOW THIS WORKS:
# FPL rates every match from 1 (easiest) to 5 (hardest). We take the next 5 unfinished
# matches for each team, average their FDR, and flip it so a HIGHER number = EASIER run.
#
# Example: Arsenal's next 5 games have FDR ratings [2, 2, 3, 2, 4]
#   → avg_upcoming_fdr = 2.6
#   → fixture_ease_score = 5.0 - 2.6 = 2.4 (relatively easy)

home_upcoming = fixtures.filter(F.col("finished") == False).select(
    F.col("home_team_id").alias("team_id"),
    F.col("gameweek"),
    F.col("home_fdr").alias("fdr")
)

away_upcoming = fixtures.filter(F.col("finished") == False).select(
    F.col("away_team_id").alias("team_id"),
    F.col("gameweek"),
    F.col("away_fdr").alias("fdr")
)

all_upcoming = home_upcoming.unionByName(away_upcoming)

# Take exactly the NEXT 5 upcoming gameweeks chronologically per team
fixture_ease = all_upcoming.withColumn(
    "row_num", 
    F.row_number().over(Window.partitionBy("team_id").orderBy("gameweek"))
).filter(F.col("row_num") <= 5) \
 .groupBy("team_id") \
 .agg(
     F.round(F.avg("fdr"), 2).alias("avg_upcoming_fdr")
 ).withColumn(
     "fixture_ease_score", 
     F.round(F.lit(5.0) - F.col("avg_upcoming_fdr"), 2)
 )

# COMMAND ----------
# 3. Derive Historical Baseline Performance with Safety-Net Logic
#
# THREE-TIER BASELINE:
#   Tier 1 (Primary):   Last completed season, if player played ≥ 10 matches there.
#   Tier 2 (Fallback):  Career PL average across ALL archived seasons, for players
#                        who played < 10 matches last season but have older PL data.
#   Tier 3 (New Signing): FPL's official points_per_game estimate from silver.players,
#                          for brand-new PL arrivals with 0 archived match history.
#
# WHY THIS MATTERS:
#   - A star who missed half a season through injury (e.g. 8 matches) gets their
#     career average instead of a distorted tiny sample.
#   - A new signing from La Liga with zero PL history still gets a sensible estimate
#     from FPL's own pricing model rather than a blank 0.0.

# Identify the latest completed historical season
latest_hist_season = player_gw_history.filter(F.col("season") != current_season) \
    .agg(F.max("season")).collect()[0][0]

print(f"Historical baseline benchmark season: {latest_hist_season}")

# Tier 1: Last season stats (only matches where player actually played)
last_season_stats = player_gw_history.filter(
    (F.col("season") == latest_hist_season) & (F.col("minutes") > 0)
).groupBy("player_key") \
 .agg(
     F.count("gameweek").alias("last_szn_matches"),
     F.sum("minutes").alias("last_szn_minutes"),
     F.sum("total_points").alias("last_szn_total_points"),
     F.sum("goals_scored").alias("last_szn_goals"),
     F.sum("assists").alias("last_szn_assists"),
     F.sum("clean_sheets").alias("last_szn_clean_sheets"),
     F.round(F.avg("total_points"), 2).alias("last_szn_ppg"),
     F.round(F.sum("expected_goals"), 2).alias("last_szn_xg"),
     F.round(F.sum("expected_assists"), 2).alias("last_szn_xa"),
     F.round(F.sum("expected_goal_involvements"), 2).alias("last_szn_xgi")
 )

# Tier 2: Career PL average across ALL historical seasons (for fallback)
career_stats = player_gw_history.filter(
    (F.col("season") != current_season) & (F.col("minutes") > 0)
).groupBy("player_key") \
 .agg(
     F.count("gameweek").alias("career_matches"),
     F.sum("minutes").alias("career_minutes"),
     F.sum("total_points").alias("career_total_points"),
     F.sum("goals_scored").alias("career_goals"),
     F.sum("assists").alias("career_assists"),
     F.sum("clean_sheets").alias("career_clean_sheets"),
     F.round(F.avg("total_points"), 2).alias("career_ppg"),
     F.round(F.sum("expected_goals"), 2).alias("career_xg"),
     F.round(F.sum("expected_assists"), 2).alias("career_xa"),
     F.round(F.sum("expected_goal_involvements"), 2).alias("career_xgi")
 )

# Join both tiers to each player
hist_baseline = last_season_stats.join(career_stats, "player_key", "full")

# Apply the safety-net selection logic:
#   IF last_szn_matches >= 10 → use last season (strong recent sample)
#   ELSE IF career_matches > 0 → use career average (fallback for injured/limited players)
#   ELSE → will fall through to Tier 3 (FPL official estimate) after join
MIN_MATCHES_THRESHOLD = 10

hist_baseline = hist_baseline.withColumn(
    "baseline_source",
    F.when(F.col("last_szn_matches") >= MIN_MATCHES_THRESHOLD, "Last Season (Primary)")
     .when(F.col("career_matches") > 0, "Career PL Average (Fallback)")
     .otherwise("No PL History")
).withColumn(
    "hist_ppg",
    F.when(F.col("last_szn_matches") >= MIN_MATCHES_THRESHOLD, F.col("last_szn_ppg"))
     .otherwise(F.col("career_ppg"))
).withColumn(
    "hist_minutes",
    F.when(F.col("last_szn_matches") >= MIN_MATCHES_THRESHOLD, F.col("last_szn_minutes"))
     .otherwise(F.col("career_minutes"))
).withColumn(
    "hist_matches_played",
    F.when(F.col("last_szn_matches") >= MIN_MATCHES_THRESHOLD, F.col("last_szn_matches"))
     .otherwise(F.col("career_matches"))
).withColumn(
    "hist_total_points",
    F.when(F.col("last_szn_matches") >= MIN_MATCHES_THRESHOLD, F.col("last_szn_total_points"))
     .otherwise(F.col("career_total_points"))
).withColumn(
    "hist_goals",
    F.when(F.col("last_szn_matches") >= MIN_MATCHES_THRESHOLD, F.col("last_szn_goals"))
     .otherwise(F.col("career_goals"))
).withColumn(
    "hist_assists",
    F.when(F.col("last_szn_matches") >= MIN_MATCHES_THRESHOLD, F.col("last_szn_assists"))
     .otherwise(F.col("career_assists"))
).withColumn(
    "hist_clean_sheets",
    F.when(F.col("last_szn_matches") >= MIN_MATCHES_THRESHOLD, F.col("last_szn_clean_sheets"))
     .otherwise(F.col("career_clean_sheets"))
).withColumn(
    "hist_xgi",
    F.when(F.col("last_szn_matches") >= MIN_MATCHES_THRESHOLD, F.col("last_szn_xgi"))
     .otherwise(F.col("career_xgi"))
).withColumn(
    "hist_avg_minutes_per_match",
    F.when(F.col("hist_matches_played") > 0,
           F.round(F.col("hist_minutes") / F.col("hist_matches_played"), 1))
     .otherwise(0.0)
).select(
    "player_key",
    "hist_ppg",
    "hist_minutes",
    "hist_matches_played",
    "hist_avg_minutes_per_match",
    "hist_total_points",
    "hist_goals",
    "hist_assists",
    "hist_clean_sheets",
    "hist_xgi",
    "baseline_source"
)

# COMMAND ----------
# 4. Join Active Players with Fixture Ease & Historical Baseline
#
# Tier 3 (New Signing Fallback): After the join, any player with NULL hist_ppg
# (i.e. zero PL match history) gets FPL's official points_per_game estimate.

active_players = players.filter(F.coalesce(F.col("status"), F.lit("a")) != "u") \
    .join(fixture_ease, "team_id", "left") \
    .join(hist_baseline, "player_key", "left") \
    .withColumn(
        "baseline_source",
        F.when(F.col("baseline_source").isNotNull(), F.col("baseline_source"))
         .otherwise("FPL Official Estimate (New Signing)")
    ).withColumn(
        "hist_ppg",
        F.when(F.col("hist_ppg").isNotNull(), F.col("hist_ppg"))
         .otherwise(F.col("points_per_game"))  # Tier 3: FPL's own estimate
    ).na.fill({
        "fixture_ease_score": 2.5, 
        "avg_upcoming_fdr": 2.5,
        "hist_ppg": 0.0,
        "hist_minutes": 0,
        "hist_matches_played": 0,
        "hist_avg_minutes_per_match": 0.0,
        "hist_total_points": 0,
        "hist_goals": 0,
        "hist_assists": 0,
        "hist_clean_sheets": 0,
        "hist_xgi": 0.0
    })

# COMMAND ----------
# 5. Gradual Baseline Blend (Historical → Live Transition)
#
# WHY A GRADUAL BLEND:
# After GW1, a player's live "form" is based on just 1 game — extremely noisy.
# If Haaland blanks in GW1 (2 pts), a hard switch would drop him from Rank #1
# to the bottom, even though he scored 240 pts last season. That's overreaction.
#
# Instead, we gradually increase trust in live data as the sample grows:
#
#   GW0 (Pre-Season):  100% Historical,   0% Live  → No live data exists
#   GW1–GW3:            80% Historical,  20% Live  → Live sample tiny (1-3 games)
#   GW4–GW6:            60% Historical,  40% Live  → Live data starting to stabilize
#   GW7–GW9:            40% Historical,  60% Live  → Live form now meaningful
#   GW10+:               0% Historical, 100% Live  → Fully trust this season
#
# IMPORTANT: The historical evidence columns (hist_ppg, hist_goals, hist_assists,
# hist_total_points, etc.) are ALWAYS on every row regardless of blend weight.
# They never disappear. Dashboards can always show "Last Season vs This Season".

finished_fixtures_count = fixtures.filter(F.col("finished") == True).count()

if finished_fixtures_count == 0:
    current_gw = 0
else:
    # Determine the latest gameweek that has at least 1 finished fixture
    current_gw = fixtures.filter(F.col("finished") == True) \
        .agg(F.max("gameweek")).collect()[0][0]

# Determine blend weights based on current gameweek
if current_gw == 0:
    live_weight = 0.0
elif current_gw <= 3:
    live_weight = 0.20
elif current_gw <= 6:
    live_weight = 0.40
elif current_gw <= 9:
    live_weight = 0.60
else:
    live_weight = 1.0

hist_weight = round(1.0 - live_weight, 2)

print(f"Current Gameweek: GW{current_gw}")
print(f"Blend Weights: {int(hist_weight * 100)}% Historical + {int(live_weight * 100)}% Live Form")

# Compute the blended effective_form
# For pre-season (live_weight=0): effective_form = hist_ppg (pure historical)
# For GW10+ (live_weight=1): effective_form = form (pure live)
# For GW1-9: a weighted blend of both
evaluated_df = active_players \
    .withColumn(
        "effective_form",
        F.round(
            (F.lit(hist_weight) * F.col("hist_ppg")) +
            (F.lit(live_weight) * F.col("form")),
            2
        )
    ).withColumn(
        "effective_minutes",
        F.when(F.lit(current_gw) == 0, F.col("hist_minutes"))
         .otherwise(F.col("minutes"))
    )

# COMMAND ----------
# 6. Position-Normalized Z-Scores per Position Group (GKP, DEF, MID, FWD)
#
# WHY Z-SCORES:
# A raw PPG of 5.0 means very different things for a Goalkeeper vs a Forward.
# 5.0 PPG for a GKP is elite (top 1%), but 5.0 for a FWD is just above average.
# Z-scores compare each player ONLY against others in the same position,
# telling you "how many standard deviations above/below the position average is he?"
#
# Z = (player_value - position_average) / position_std_deviation
#
# A Z-score of +2.0 means the player is 2 standard deviations above his position peers.
# A Z-score of  0.0 means the player is exactly average for his position.
# A Z-score of -1.5 means the player is well below average for his position.

window_pos = Window.partitionBy("position_name")

z_df = evaluated_df \
    .withColumn("form_mean", F.avg("effective_form").over(window_pos)) \
    .withColumn("form_std", F.stddev("effective_form").over(window_pos)) \
    .withColumn("form_z", F.round(F.when(F.col("form_std") > 0, (F.col("effective_form") - F.col("form_mean")) / F.col("form_std")).otherwise(0.0), 2)) \
    .withColumn("ease_mean", F.avg("fixture_ease_score").over(window_pos)) \
    .withColumn("ease_std", F.stddev("fixture_ease_score").over(window_pos)) \
    .withColumn("fixture_ease_z", F.round(F.when(F.col("ease_std") > 0, (F.col("fixture_ease_score") - F.col("ease_mean")) / F.col("ease_std")).otherwise(0.0), 2)) \
    .withColumn("min_mean", F.avg("effective_minutes").over(window_pos)) \
    .withColumn("min_std", F.stddev("effective_minutes").over(window_pos)) \
    .withColumn("minutes_reliability_z", F.round(F.when(F.col("min_std") > 0, (F.col("effective_minutes") - F.col("min_mean")) / F.col("min_std")).otherwise(0.0), 2))

# COMMAND ----------
# 7. Dual Scoring Engine: Quality Score (Best Players) & Value Score (Best ROI)
#
# QUALITY SCORE: "Who is the best player regardless of price?"
#   = 85% Form/PPG (pure scoring power) + 15% Fixture Ease (schedule boost)
#   → Used for: Starting XI selection and Captaincy shortlisting
#
# VALUE SCORE: "Who gives the most points per million pounds spent?"
#   = (Quality Score + 3.0) / Price in £m
#   → The +3.0 shifts all quality scores into positive territory before dividing.
#   → Used for: Finding budget enablers and bench picks that free up cash for stars.

scored_df = z_df.withColumn(
    "quality_score",
    F.round((F.lit(0.85) * F.col("form_z")) + (F.lit(0.15) * F.col("fixture_ease_z")), 2)
).withColumn(
    "value_score",
    F.round((F.col("quality_score") + F.lit(3.0)) / F.col("price_gbp"), 2)
).withColumn(
    "position_quality_rank",
    F.row_number().over(Window.partitionBy("position_name").orderBy(F.col("quality_score").desc()))
).withColumn(
    "position_value_rank",
    F.row_number().over(Window.partitionBy("position_name").orderBy(F.col("value_score").desc()))
).withColumn(
    # STRATEGY TIER: Driven by our own quality_score ranking, NOT arbitrary price tags.
    # Top 8 players per position = Season Anchors (the core you never sell).
    # Everyone else = Rolling Targets (rotate based on fixture swings).
    #
    # WHY 8? In a 15-man FPL squad (~2 GKP, 5 DEF, 5 MID, 3 FWD), the top 8 per
    # position covers the realistic pool that competitive managers choose from.
    # Anyone outside that tier is a fixture-dependent rotation pick.
    "strategy_tier",
    F.when(
        F.col("position_quality_rank") <= 8,
        "🛡️ Season Anchor (Set & Forget)"
    ).otherwise("🔄 Rolling Transfer Target")
).withColumn(
    "is_penalty_taker",
    F.when(F.col("penalties_order") == 1, True).otherwise(False)
).withColumn(
    "is_set_piece_taker",
    F.when((F.col("corners_and_indirect_freekicks_order") == 1) | (F.col("direct_freekicks_order") == 1), True).otherwise(False)
).select(
    "player_key",
    "player_id",
    "web_name",
    "team_name",
    "team_short_name",
    "position_name",
    "price_gbp",
    "ownership_percent",
    "position_quality_rank",
    "quality_score",
    "position_value_rank",
    "value_score",
    "strategy_tier",
    "baseline_source",
    "effective_form",
    "hist_total_points",
    "hist_goals",
    "hist_assists",
    "hist_clean_sheets",
    "hist_ppg",
    "hist_xgi",
    "hist_minutes",
    "hist_matches_played",
    "hist_avg_minutes_per_match",
    "avg_upcoming_fdr",
    "fixture_ease_score",
    "form_z",
    "fixture_ease_z",
    "minutes_reliability_z",
    "is_penalty_taker",
    "is_set_piece_taker",
    "news",
    "chance_of_playing_next_round",
    F.current_timestamp().alias("_updated_at")
)

# COMMAND ----------
# 8. Save to fpl.gold.value_scores
scored_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable(target_table)

print(f"✅ Successfully created Gold Value Scores table: {target_table} ({scored_df.count()} rows)")
display(scored_df.filter(F.col("position_quality_rank") <= 5).orderBy("position_name", "position_quality_rank"))
