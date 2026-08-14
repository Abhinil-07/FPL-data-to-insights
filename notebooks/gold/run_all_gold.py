# Databricks notebook source
# COMMAND ----------
# run_all_gold.py
# Master orchestrator to run all Phase 4 Gold analytics transforms in correct dependency order.

print("🏆 Starting Gold Layer Analytics Execution...")

# COMMAND ----------
# 1. Value Scores (Position-Normalized Z-Scores)
dbutils.notebook.run("01_build_gold_value_scores", 0)
print("✅ Step 1/10: fpl.gold.value_scores created!")

# COMMAND ----------
# 2. Player Historical Trends (Home/Away & Venue Splits)
dbutils.notebook.run("02_build_gold_player_trends", 0)
print("✅ Step 2/10: fpl.gold.player_trends created!")

# COMMAND ----------
# 3. Opponent Matchup History
dbutils.notebook.run("03_build_gold_matchup_history", 0)
print("✅ Step 3/10: fpl.gold.matchup_history created!")

# COMMAND ----------
# 4. Team Trends (Rolling 6-game attacking & defensive form)
dbutils.notebook.run("04_build_gold_team_trends", 0)
print("✅ Step 4/10: fpl.gold.team_trends created!")

# COMMAND ----------
# 5. Fixture Difficulty Planner Matrix
dbutils.notebook.run("05_build_gold_fixture_planner", 0)
print("✅ Step 5/10: fpl.gold.fixture_planner created!")

# COMMAND ----------
# 6. Low-Ownership Differentials Finder
dbutils.notebook.run("06_build_gold_differentials", 0)
print("✅ Step 6/10: fpl.gold.differentials created!")

# COMMAND ----------
# 7. Price Momentum Tracker
dbutils.notebook.run("07_build_gold_price_momentum", 0)
print("✅ Step 7/10: fpl.gold.price_momentum created!")

# COMMAND ----------
# 8. Underlying Stats Layer (xG/xA vs. actual goals & "due a return" flags)
dbutils.notebook.run("08_build_gold_underlying_stats", 0)
print("✅ Step 8/10: fpl.gold.underlying_stats created!")

# COMMAND ----------
# 9. Personal Squad Tracker
dbutils.notebook.run("09_build_gold_my_squad_tracker", 0)
print("✅ Step 9/10: fpl.gold.my_squad_tracker created!")

# COMMAND ----------
# 10. Dedicated Captaincy Fit Shortlist
dbutils.notebook.run("10_build_gold_captaincy_fit", 0)
print("✅ Step 10/10: fpl.gold.captaincy_fit created!")

print("🎉 Entire Gold Layer (Phase 4) completed successfully!")
