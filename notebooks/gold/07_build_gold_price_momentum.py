# Databricks notebook source
# COMMAND ----------
# 07_build_gold_price_momentum.py
# Phase 4: Build fpl.gold.price_momentum for transfer activity momentum and price change threshold proximity.

import os
import sys
import yaml
from pyspark.sql import functions as F

# COMMAND ----------
# Load config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_silver = config["databases"]["silver"]
db_gold = config["databases"]["gold"]

# COMMAND ----------
# Read Silver players table
players = spark.read.table(f"{db_silver}.players")

# COMMAND ----------
# Compute Net Transfers In/Out Momentum and Price Change Direction
price_momentum = players.withColumn(
    "net_transfers_event",
    F.col("transfers_in_event") - F.col("transfers_out_event")
).withColumn(
    "momentum_direction",
    F.when(F.col("net_transfers_event") > 10000, "Price Rise Candidate 📈")
     .when(F.col("net_transfers_event") < -10000, "Price Fall Candidate 📉")
     .otherwise("Neutral ➖")
).select(
    "player_key",
    "web_name",
    "team_name",
    "position_name",
    "price_gbp",
    "ownership_percent",
    "transfers_in_event",
    "transfers_out_event",
    "net_transfers_event",
    "momentum_direction",
    F.current_timestamp().alias("_updated_at")
)

# COMMAND ----------
# Save to fpl.gold.price_momentum
target_table = f"{db_gold}.price_momentum"
price_momentum.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(target_table)

print(f"✅ Successfully created Gold Price Momentum table: {target_table} ({price_momentum.count()} rows)")
display(price_momentum.orderBy(F.abs(F.col("net_transfers_event")).desc()).limit(15))
