# Databricks notebook source
# COMMAND ----------
# run_all_silver.py
# Master orchestrator to run all Phase 3 Silver transforms in correct dependency order.

print("🚀 Starting Silver Layer Transforms Execution...")

# COMMAND ----------
# 1. Team Dimension
dbutils.notebook.run("01_build_team_crosswalk", 0)
print("✅ Step 1/5: fpl.silver.teams created!")

# COMMAND ----------
# 2. Player Crosswalk (Reconciliation)
dbutils.notebook.run("02_build_player_crosswalk", 0)
print("✅ Step 2/5: fpl.silver.player_crosswalk created!")

# COMMAND ----------
# 3. Current Player Dimension
dbutils.notebook.run("03_build_silver_players", 0)
print("✅ Step 3/5: fpl.silver.players created!")

# COMMAND ----------
# 4. Multi-Season Gameweek History (Unified xG/xA)
dbutils.notebook.run("04_build_silver_player_gw_history", 0)
print("✅ Step 4/5: fpl.silver.player_gw_history created!")

# COMMAND ----------
# 5. Cleaned Fixtures & FDR Schedule
dbutils.notebook.run("05_build_silver_fixtures", 0)
print("✅ Step 5/5: fpl.silver.fixtures created!")

print("🎉 Entire Silver Layer (Phase 3) completed successfully!")
