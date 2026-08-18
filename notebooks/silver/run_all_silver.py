# Databricks notebook source
# COMMAND ----------
# run_all_silver.py
# Master orchestrator to build all Phase 3 Silver tables and crosswalk reconciliation.

print("🧹 Starting Silver Layer & Crosswalk Execution...")

# COMMAND ----------
# MAGIC %run ./01_build_team_crosswalk

# COMMAND ----------
# MAGIC %run ./02_build_player_crosswalk

# COMMAND ----------
# MAGIC %run ./03_build_silver_players

# COMMAND ----------
# MAGIC %run ./05_build_silver_fixtures

# COMMAND ----------
# MAGIC %run ./06_build_silver_gameweeks

# COMMAND ----------
# MAGIC %run ./07_build_silver_my_team

# COMMAND ----------
# MAGIC %run ./04_build_silver_player_gw_history

# COMMAND ----------
print("🎉 Entire Silver Layer (Phase 3) completed successfully!")
