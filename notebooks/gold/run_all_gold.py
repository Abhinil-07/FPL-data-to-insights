# Databricks notebook source
# COMMAND ----------
# run_all_gold.py
# Master orchestrator to run all Phase 4 Gold analytics transforms in correct dependency order.

print("🏆 Starting Gold Layer Analytics Execution...")

# COMMAND ----------
# MAGIC %run ./01_build_gold_value_scores

# COMMAND ----------
# MAGIC %run ./02_build_gold_player_trends

# COMMAND ----------
# MAGIC %run ./03_build_gold_matchup_history

# COMMAND ----------
# MAGIC %run ./04_build_gold_team_trends

# COMMAND ----------
# MAGIC %run ./05_build_gold_fixture_planner

# COMMAND ----------
# MAGIC %run ./06_build_gold_differentials

# COMMAND ----------
# MAGIC %run ./07_build_gold_price_momentum

# COMMAND ----------
# MAGIC %run ./08_build_gold_underlying_stats

# COMMAND ----------
# MAGIC %run ./09_build_gold_my_squad_tracker

# COMMAND ----------
# MAGIC %run ./10_build_gold_captaincy_fit

# COMMAND ----------
print("🎉 Entire Gold Layer (Phase 4) completed successfully!")
