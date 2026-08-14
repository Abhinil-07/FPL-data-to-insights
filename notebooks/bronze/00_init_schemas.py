# Databricks notebook source
# COMMAND ----------
# 00_init_schemas.py
# Initializes Catalog / Databases for Bronze, Silver, and Gold Medallion layers.
# Handles both Unity Catalog (`fpl.bronze`, `fpl.silver`, `fpl.gold`)
# and standard Hive Metastore (`fpl_bronze`, `fpl_silver`, `fpl_gold`).

import os
import yaml

# COMMAND ----------
# Load configuration
config_path = "../../config/config.yaml" if os.path.exists("../../config/config.yaml") else "config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_bronze = config["databases"]["bronze"]
db_silver = config["databases"]["silver"]
db_gold = config["databases"]["gold"]

# COMMAND ----------
# Try Unity Catalog creation first (if supported by workspace)
try:
    print("Attempting Unity Catalog initialization ('fpl' catalog)...")
    spark.sql("CREATE CATALOG IF NOT EXISTS fpl")
    spark.sql("CREATE SCHEMA IF NOT EXISTS fpl.bronze")
    spark.sql("CREATE SCHEMA IF NOT EXISTS fpl.silver")
    spark.sql("CREATE SCHEMA IF NOT EXISTS fpl.gold")
    print("✅ Unity Catalog 'fpl' created with schemas: fpl.bronze, fpl.silver, fpl.gold!")
except Exception as e:
    print(f"ℹ️ Unity Catalog not enabled or restricted ({e}). Falling back to standard Hive Metastore databases...")

# COMMAND ----------
# Always ensure standard database schemas exist
for db_name in [db_bronze, db_silver, db_gold]:
    print(f"Creating database if not exists: {db_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

print("✅ All Medallion databases/schemas initialized successfully!")
