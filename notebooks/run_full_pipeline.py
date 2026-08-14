# Databricks notebook source
# COMMAND ----------
# run_full_pipeline.py
# Master End-to-End Orchestrator for FPL Decision-Support Medallion Pipeline
# Executes Bronze (Ingestion) -> Silver (Crosswalk & Cleaning) -> Gold (Analytics & 2-Tier Strategy) in one click.

import time

start_time = time.time()
print("🚀 Starting End-to-End FPL Medallion Pipeline Execution (Bronze -> Silver -> Gold)...")

# COMMAND ----------
# STEP 1: BRONZE LAYER INGESTION
print("\n--- 📥 Step 1/3: Ingesting Bronze Raw Data (FPL API + GitHub Archive) ---")
dbutils.notebook.run("bronze/00_init_schemas", 0)
print("  ✓ Schemas Initialized")

dbutils.notebook.run("bronze/01_ingest_players_raw", 0)
print("  ✓ Players Raw Ingested")

dbutils.notebook.run("bronze/02_ingest_teams_raw", 0)
print("  ✓ Teams Raw Ingested")

dbutils.notebook.run("bronze/03_ingest_events_raw", 0)
print("  ✓ Events Raw Ingested")

dbutils.notebook.run("bronze/04_ingest_fixtures_raw", 0)
print("  ✓ Fixtures Raw Ingested")

dbutils.notebook.run("bronze/05_ingest_my_team_raw", 0)
print("  ✓ Personal Squad Raw Processed")

dbutils.notebook.run("bronze/06_ingest_github_archive", 0)
print("  ✓ Historical GitHub Archive (3 Seasons) Ingested")
print("✅ Bronze Layer Execution Complete!")

# COMMAND ----------
# STEP 2: SILVER LAYER CLEANING & CROSSWALK RECONCILIATION
print("\n--- 🧹 Step 2/3: Building Silver Tables & Crosswalk Reconciliation ---")
dbutils.notebook.run("silver/run_all_silver", 0)
print("✅ Silver Layer Execution Complete!")

# COMMAND ----------
# STEP 3: GOLD LAYER ANALYTICS & 2-TIER STRATEGY
print("\n--- 🏆 Step 3/3: Building Gold Analytics Tables & 2-Tier Strategy ---")
dbutils.notebook.run("gold/run_all_gold", 0)
print("✅ Gold Layer Execution Complete!")

# COMMAND ----------
elapsed = time.time() - start_time
print(f"\n🎉 ENTIRE FPL PIPELINE EXECUTED SUCCESSFULLY IN {elapsed:.2f} SECONDS!")
print("Your live Databricks database (fpl.gold.*) and Streamlit Dashboard are fully refreshed!")
