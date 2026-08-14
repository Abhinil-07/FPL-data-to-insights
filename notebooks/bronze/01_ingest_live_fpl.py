# Databricks notebook source
# COMMAND ----------
# 01_ingest_live_fpl.py
# Phase 1: Ingest live FPL API endpoints into Bronze Delta tables.

import os
import sys
import yaml
from datetime import datetime
import pandas as pd

# Add src to python path for Databricks imports
sys.path.append(os.path.abspath("../../"))
sys.path.append(os.path.abspath("./"))

from src.fpl_api import FPLApiClient

# COMMAND ----------
# Load configuration
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze = config["databases"]["bronze"]
team_id = config.get("fpl_team_id")

client = FPLApiClient()
ingested_at = datetime.utcnow()

# COMMAND ----------
# 1. Ingest bootstrap-static
bootstrap_data = client.get_bootstrap_static()

if bootstrap_data:
    # 1.1 Players Raw (elements)
    players_pdf = pd.DataFrame(bootstrap_data["elements"])
    players_pdf["_ingested_at"] = ingested_at
    players_df = spark.createDataFrame(players_pdf)
    players_df.write.mode("overwrite").format("delta").saveAsTable(f"{db_bronze}.players_raw")
    print(f"Saved {players_df.count()} rows to {db_bronze}.players_raw")

    # 1.2 Teams Raw (teams)
    teams_pdf = pd.DataFrame(bootstrap_data["teams"])
    teams_pdf["_ingested_at"] = ingested_at
    teams_df = spark.createDataFrame(teams_pdf)
    teams_df.write.mode("overwrite").format("delta").saveAsTable(f"{db_bronze}.teams_raw")
    print(f"Saved {teams_df.count()} rows to {db_bronze}.teams_raw")

    # 1.3 Events Raw (gameweek metadata)
    events_pdf = pd.DataFrame(bootstrap_data["events"])
    events_pdf["_ingested_at"] = ingested_at
    events_df = spark.createDataFrame(events_pdf)
    events_df.write.mode("overwrite").format("delta").saveAsTable(f"{db_bronze}.events_raw")
    print(f"Saved {events_df.count()} rows to {db_bronze}.events_raw")

# COMMAND ----------
# 2. Ingest Fixtures
fixtures_data = client.get_fixtures()
if fixtures_data:
    fixtures_pdf = pd.DataFrame(fixtures_data)
    fixtures_pdf["_ingested_at"] = ingested_at
    fixtures_df = spark.createDataFrame(fixtures_pdf)
    fixtures_df.write.mode("overwrite").format("delta").saveAsTable(f"{db_bronze}.fixtures_raw")
    print(f"Saved {fixtures_df.count()} rows to {db_bronze}.fixtures_raw")

# COMMAND ----------
# 3. Ingest Personal Team Data (Optional & Graceful)
if team_id:
    team_data = client.get_my_team(team_id)
    if team_data:
        my_team_pdf = pd.DataFrame([team_data])
        my_team_pdf["_ingested_at"] = ingested_at
        my_team_df = spark.createDataFrame(my_team_pdf)
        my_team_df.write.mode("overwrite").format("delta").saveAsTable(f"{db_bronze}.my_team_raw")
        print(f"Saved squad data for Team ID {team_id} to {db_bronze}.my_team_raw")
else:
    print("No FPL Team ID configured. Skipping personal squad Bronze table ingestion gracefully.")

print("Phase 1 Live FPL API Ingestion completed successfully!")
