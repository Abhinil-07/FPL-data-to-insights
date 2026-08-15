# Databricks notebook source
# COMMAND ----------
# run_full_pipeline.py
# Master End-to-End Orchestrator for FPL Decision-Support Medallion Pipeline.
# Uses Databricks %run magic commands to execute in-process on active compute cluster.

print("🚀 Starting End-to-End FPL Medallion Pipeline Execution (Bronze -> Silver -> Gold)...")

# COMMAND ----------
# STEP 1: BRONZE LAYER INGESTION
print("\n--- 📥 Step 1/3: Ingesting Bronze Raw Data (FPL API + GitHub Archive) ---")

# COMMAND ----------
# MAGIC %run ./bronze/00_init_schemas

# COMMAND ----------
# MAGIC %run ./bronze/01_ingest_players_raw

# COMMAND ----------
# MAGIC %run ./bronze/02_ingest_teams_raw

# COMMAND ----------
# MAGIC %run ./bronze/03_ingest_events_raw

# COMMAND ----------
# MAGIC %run ./bronze/04_ingest_fixtures_raw

# COMMAND ----------
# MAGIC %run ./bronze/05_ingest_my_team_raw

# COMMAND ----------
# MAGIC %run ./bronze/06_ingest_github_archive

# COMMAND ----------
# MAGIC %run ./bronze/07_ingest_player_gw_history

# COMMAND ----------
print("✅ Bronze Layer Execution Complete!")

# COMMAND ----------
# STEP 2: SILVER LAYER CLEANING & CROSSWALK RECONCILIATION
print("\n--- 🧹 Step 2/3: Building Silver Tables & Crosswalk Reconciliation ---")

# COMMAND ----------
# MAGIC %run ./silver/run_all_silver

# COMMAND ----------
print("✅ Silver Layer Execution Complete!")

# COMMAND ----------
# STEP 3: GOLD LAYER ANALYTICS & 2-TIER STRATEGY
print("\n--- 🏆 Step 3/3: Building Gold Analytics Tables & 2-Tier Strategy ---")

# COMMAND ----------
# MAGIC %run ./gold/run_all_gold

# COMMAND ----------
print("🎉 ENTIRE FPL PIPELINE EXECUTED SUCCESSFULLY!")
print("fpl.bronze.*, fpl.silver.*, and fpl.gold.* Delta tables are fully refreshed.")
