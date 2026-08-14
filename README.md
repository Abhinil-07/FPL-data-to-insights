# FPL Decision-Support Dashboard

A personal Fantasy Premier League (FPL) data engineering & BI project built using **Databricks Free Edition** and **Delta Lake** following the **Medallion Architecture (Bronze → Silver → Gold)**.

## 📌 Core Features & Principles
- **No Machine Learning / Predictions:** Transparent, formula-based composite Z-scores and diagnostic historical trend views.
- **Position-Normalized Scoring:** All scores are computed strictly within position groups (GKP / DEF / MID / FWD).
- **Manual Trigger Refresh:** Data updates on demand prior to transfer decisions—no background crons.
- **Graceful Personal Squad Integration:** Full public analytics operate automatically; personal squad tracking (`my_squad_tracker`) activates whenever an `fpl_team_id` is supplied in `config/config.yaml`.

---

## 📁 Repository Structure

```
fpl-dashboard/
├── config/
│   ├── config.yaml          # FPL Team ID, seasons, API endpoints, database names
│   └── player_overrides.csv  # Manual player crosswalk overrides for tricky name matches
├── notebooks/
│   ├── bronze/              # Live API & Historical GitHub Archive ingestion
│   │   ├── 00_init_schemas.py
│   │   └── 01_ingest_live_fpl.py
│   ├── silver/              # Cleaning, typing, and Player Crosswalk reconciliation
│   └── gold/                # Aggregates, position-normalized Z-scores, fixture planner
├── schemas/
│   └── bronze_schemas.py    # PySpark StructType definitions for Delta tables
├── src/
│   ├── fpl_api/             # FPL API client with rate-limiting & retries
│   ├── github_archive/      # Historical archive downloader (vaastav/Fantasy-Premier-League)
│   └── transforms/          # Shared Silver/Gold transform logic
├── tests/
│   └── test_fpl_api.py      # Unit tests for API client
├── requirements.txt
└── README.md
```

---

## 🚀 Getting Started on Databricks Free Edition

1. **Import Repository:** Import this repo into your Databricks workspace via **Workspace > Repos > Add Repo**.
2. **Run Initialization:** Run `notebooks/bronze/00_init_schemas.py` to create `fpl_bronze`, `fpl_silver`, and `fpl_gold` databases.
3. **Trigger Live Ingestion:** Run `notebooks/bronze/01_ingest_live_fpl.py` to populate Bronze Delta tables (`players_raw`, `teams_raw`, `events_raw`, `fixtures_raw`).
4. **Unit Testing:** Run `python tests/test_fpl_api.py` to test API connectivity locally or in Databricks terminals.
