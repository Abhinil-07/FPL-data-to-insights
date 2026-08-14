# Databricks notebook source
# COMMAND ----------
# 00_init_schemas.py
# Initializes databases/schemas for Bronze, Silver, and Gold Medallion layers.

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
# Create Databases in Databricks / PySpark SQL
for db_name in [db_bronze, db_silver, db_gold]:
    print(f"Creating database if not exists: {db_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

print("Databricks Medallion databases initialized successfully!")
