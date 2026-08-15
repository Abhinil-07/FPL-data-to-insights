# 📥 Medallion Architecture Guide — Layer 1: Bronze Ingestion (`fpl.bronze.*`)

## 1. Executive Summary & Purpose

The **Bronze Layer** is the foundational raw landing zone of the Medallion Data Engineering Architecture.

### Core Objectives:
- **Raw Fidelity Storage:** Preserves incoming JSON payloads from the **official FPL API** and raw CSV structures from the **`vaastav/Fantasy-Premier-League` 3-year historical archive** exactly as received, without altering raw data values or column schemas.
- **Delta Lake Persistence:** All raw data is persisted into **Delta Lake tables** under the `fpl.bronze` catalog/schema, enabling ACID transactions, time-travel auditing, and rapid query performance.
- **Audit Traceability:** Every single record ingested is automatically tagged with an `_ingested_at` UTC timestamp to track data freshness.

---

## 2. Ingestion Notebook Breakdown

The Bronze layer is populated by **8 specialized Databricks notebooks** located in `notebooks/bronze/`:

| Notebook Name | Data Source | Target Bronze Delta Table | Description & Output |
|---|---|---|---|
| `00_init_schemas.py` | Metadata | Catalog & Schemas | Initializes `fpl.bronze`, `fpl.silver`, `fpl.gold` Unity Catalog schemas. |
| `01_ingest_players_raw.py` | Official FPL API | `fpl.bronze.players_raw` | Ingests live player master data (600+ players), current price, position ID, ownership, and season stats. |
| `02_ingest_teams_raw.py` | Official FPL API | `fpl.bronze.teams_raw` | Ingests 20 Premier League teams master metadata, short names, home/away strength ratings. |
| `03_ingest_events_raw.py` | Official FPL API | `fpl.bronze.events_raw` | Ingests 38 gameweek calendar metadata, deadlines, finished status, and highest scoring managers. |
| `04_ingest_fixtures_raw.py` | Official FPL API | `fpl.bronze.fixtures_raw` | Ingests full 380-match Premier League schedule with home/away teams, kickoff times, and official FDR ratings. |
| `05_ingest_my_team_raw.py` | Official FPL API | `fpl.bronze.my_team_raw` | Automatically pulls the user's personal squad, rank over time, transfers, and captain picks via FPL Team ID. |
| `06_ingest_github_archive.py` | GitHub Archive | `fpl.bronze.archive_player_gws` | Pulls ~50,000+ gameweek match logs across 3 seasons (`2023-24`, `2024-25`, `2025-26`) with xG, xA, ICT, DefCon. |
| `06_ingest_github_archive.py` | GitHub Archive | `fpl.bronze.archive_players_raw` | Pulls seasonal player master snapshots across 3 seasons for crosswalk reconciliation. |

---

## 3. Bronze Table Schemas & Key Fields

### 1. `fpl.bronze.players_raw`
*Live player master data from official FPL API (`bootstrap-static/ elements`).*
- `id` (int): FPL Official current season Player ID (e.g. `318` for Haaland).
- `web_name` (string): Short display name on shirts/FPL interface (e.g. `"Haaland"`).
- `first_name` & `second_name` (string): Full legal name (`"Erling"`, `"Haaland"`).
- `element_type` (int): Position code (`1` = GKP, `2` = DEF, `3` = MID, `4` = FWD).
- `team` (int): Official team ID code (1 to 20).
- `now_cost` (int): Player price in tenths of a million (e.g. `155` = £15.5m).
- `selected_by_percent` (string/float): Overall ownership percentage (e.g. `73.5%`).
- `form` (string/float): Average points scored per game over recent gameweeks.
- `minutes`, `goals_scored`, `assists`, `clean_sheets`, `goals_conceded`: Cumulative current season totals.
- `_ingested_at` (timestamp): Ingestion audit timestamp.

---

### 2. `fpl.bronze.teams_raw`
*Live Premier League team master metadata (`bootstrap-static/ teams`).*
- `id` (int): Official Team ID (1 to 20).
- `name` (string): Full team name (e.g. `"Arsenal"`, `"Liverpool"`).
- `short_name` (string): 3-letter abbreviation (e.g. `"ARS"`, `"LIV"`).
- `strength` (int): General overall team strength rating (1 to 5).
- `strength_overall_home` & `strength_overall_away`: Home/Away specific strength indices.
- `strength_attack_home` & `strength_defense_home`: Granular attacking & defensive ratings.

---

### 3. `fpl.bronze.fixtures_raw`
*Full 380-match Premier League schedule (`fixtures/`).*
- `id` (int): Unique fixture match ID.
- `event` (int): Gameweek number (1 through 38).
- `home_team` & `away_team` (int): Team IDs for home and away clubs.
- `finished` (boolean): `true` if match completed, `false` if upcoming.
- `team_h_difficulty` & `team_a_difficulty` (int): Official Fixture Difficulty Rating (FDR 1 to 5).
- `kickoff_time` (timestamp): Kickoff timestamp.

---

### 4. `fpl.bronze.archive_player_gws`
*3-Year Gameweek-level historical match logs from `vaastav/Fantasy-Premier-League` repo.*
- `season` (string): Season identifier (e.g. `"2023-24"`, `"2024-25"`, `"2025-26"`).
- `name` (string): Historical player full name (e.g. `"Erling_Haaland_355"`).
- `element` (string/int): Season-specific player ID.
- `GW` (string/int): Gameweek number.
- `total_points` (string/int): Points scored in that specific match.
- `minutes`, `goals_scored`, `assists`, `clean_sheets`: Per-match stats.
- `expected_goals` (`xG`), `expected_assists` (`xA`): Underlying chance quality metrics (sourced from Understat integration).
- `ict_index`, `influence`, `creativity`, `threat`: FPL underlying indices.

---

## 4. Key Architectural Insights & Innovations

1. **Schema Sanitization (`sanitize_df_for_delta`):**
   - Official FPL API payloads contain nested JSON objects/lists (e.g. `element_type` metadata or fixture stat arrays).
   - Our Bronze ingestion pipelines automatically convert complex Python dictionaries/lists to JSON strings before writing to Delta Lake, preventing schema mismatch crashes!

2. **Cross-Season Schema Harmonization:**
   - GitHub archive headers vary slightly across historical seasons (e.g. `xG` vs `expected_goals`).
   - `06_ingest_github_archive.py` casts historical fields to compatible string representations at landing, ensuring smooth downstream reconciliation in the Silver layer without losing historical rows!

---

## 5. Next Layer Transition: Bronze $\rightarrow$ Silver

The Bronze layer contains **raw data from 2 completely separate data sources** that do NOT share a common ID scheme:
- Source A: Live FPL API (ID e.g. `318`)
- Source B: Historical GitHub Archive (Name string e.g. `"Erling_Haaland_355"`)

In **Layer 2 (Silver)**, we build a **durable Crosswalk Identity Map (`player_key`)** to unify live API records with 3 years of historical match logs into clean, queryable analytics tables!
