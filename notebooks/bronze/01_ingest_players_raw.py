# Databricks notebook source
# COMMAND ----------
# 01_ingest_players_raw.py
# Ingest ONLY `fpl_bronze.players_raw` from bootstrap-static/

import os
import sys
import yaml
from datetime import datetime
import pandas as pd

sys.path.append(os.path.abspath("../../"))
sys.path.append(os.path.abspath("./"))

from src.fpl_api import FPLApiClient

# COMMAND ----------
# Load config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze = config["databases"]["bronze"]
client = FPLApiClient()
ingested_at = datetime.utcnow()

# COMMAND ----------
# Fetch bootstrap-static
data = client.get_bootstrap_static()
assert data is not None, "Failed to fetch bootstrap-static payload from FPL API"

# COMMAND ----------
# Process players (elements) dataframe
players_pdf = pd.DataFrame(data["elements"])
players_pdf["_ingested_at"] = ingested_at

# Convert to Spark DataFrame
players_df = spark.createDataFrame(players_pdf)

# COMMAND ----------
# Save to Delta Table
target_table = f"{db_bronze}.players_raw"
players_df.write.mode("overwrite").format("delta").saveAsTable(target_table)

print(f"✅ Successfully written {players_df.count()} rows to Delta table: {target_table}")

# COMMAND ----------
# Display sample preview
display(players_df.select("id", "web_name", "team", "element_type", "now_cost", "total_points", "_ingested_at").limit(10))
