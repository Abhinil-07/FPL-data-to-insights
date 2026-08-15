# 🧹 Medallion Architecture Guide — Layer 2: Silver Cleaning & Crosswalk Layer (`fpl.silver.*`)

## 1. Executive Summary & Purpose

The **Silver Layer** is the cleansed, normalized, and reconciled tier of the Medallion Data Architecture.

### Core Objectives:
- **Crosswalk Reconciliation (`player_key`):** Unifies live API player IDs (e.g. `id: 318`) with 3 years of historical GitHub archive records (e.g. `"Erling_Haaland_355"`) using durable FPL player `code` and exact full-name matching.
- **Priority Deduplication:** Employs windowed priority ranking (`Window.partitionBy("season", "source_player_id").orderBy("priority")`) to prevent duplicate row explosions.
- **Pre-Season Fallback Handling:** Implements default fallbacks (e.g., `coalesce(strength, 3)`, `coalesce(strength_overall, 1100)`) to gracefully handle null pre-season ratings.
- **Data Enrichment & Type Casting:** Converts raw JSON strings into clean DataTypes (Integers, Floats, Timestamps, Position Names `GKP`, `DEF`, `MID`, `FWD`).

---

## 2. Silver Notebook Breakdown

The Silver layer is populated by **5 specialized Databricks notebooks** located in `notebooks/silver/`:

| Notebook Name | Source Bronze Tables | Target Silver Delta Table | Description & Output |
|---|---|---|---|
| `01_build_team_crosswalk.py` | `fpl.bronze.teams_raw` | `fpl.silver.teams` | Cleaned 20-team master dimension table with standardized short names (`ARS`, `LIV`) and home/away strength scores. |
| `02_build_player_crosswalk.py` | `fpl.bronze.players_raw`, `archive_players_raw` | `fpl.silver.player_crosswalk` | Unifies live player IDs with 3-year historical records using durable `code` and full-name priority deduplication. |
| `03_build_silver_players.py` | `fpl.bronze.players_raw`, `fpl.silver.teams` | `fpl.silver.players` | Master current-season player dimension with position names (`GKP`/`DEF`/`MID`/`FWD`), price in GBP, ownership %, and team names. |
| `04_build_silver_player_gw_history.py` | `archive_player_gws`, `player_crosswalk`, `fixtures` | `fpl.silver.player_gw_history` | Cleaned ~50,000+ match log fact table across 3 seasons with Home/Away flags, Opponent Teams, xG, xA, and ICT. |
| `05_build_silver_fixtures.py` | `fpl.bronze.fixtures_raw`, `fpl.silver.teams` | `fpl.silver.fixtures` | Cleaned 380-match fixture schedule with joined home/away team names, short codes, finished status, and FDR ratings. |

---

## 3. Silver Table Schemas & Key Fields

### 1. `fpl.silver.teams`
*Master Team Dimension Table (20 Premier League Clubs).*
- `team_id` (int, Primary Key): Official Team ID (1 to 20).
- `team_code` (int): Durable FPL team code.
- `team_name` (string): Clean team name (e.g. `"Arsenal"`, `"Liverpool"`).
- `short_name` (string): 3-letter abbreviation (e.g. `"ARS"`, `"LIV"`).
- `strength` (int): Overall team difficulty rating (1 to 5).
- `strength_overall_home` & `strength_overall_away` (int): Home/Away strength ratings.
- `strength_attack_home` & `strength_defence_home` (int): Specific attacking/defensive indices.

---

### 2. `fpl.silver.player_crosswalk`
*Identity Resolution Mapping Table.*
- `player_key` (int, Primary Key): Durable FPL player code (unique identifier across seasons).
- `season` (string): Season identifier (e.g. `"2023-24"`, `"2024-25"`, `"2025-26"`, `"2026-27"`).
- `source` (string): Source layer identifier (`"live_api"` vs `"archive"`).
- `source_player_id` (int): Source-specific player ID.
- `first_name` & `second_name` (string): Player names.
- `crosswalk_status` (string): Matching strategy applied (`"live_master"`, `"matched_by_durable_code"`, `"matched_by_full_name"`).

---

### 3. `fpl.silver.players`
*Current Season Master Player Dimension.*
- `player_key` (int, Primary Key): Durable FPL player code.
- `player_id` (int): Current season live FPL player ID.
- `web_name` (string): Short shirt display name.
- `first_name` & `second_name` (string): Full names.
- `position_name` (string): Position group (`"GKP"`, `"DEF"`, `"MID"`, `"FWD"`).
- `team_id` (int, FK): Team ID.
- `team_name` & `team_short_name` (string): Team display names.
- `price_gbp` (float): Current price in £m (e.g. `15.5`).
- `ownership_percent` (float): Ownership % (e.g. `73.5`).
- `form` (float): Current form rating.
- `minutes`, `goals_scored`, `assists`, `clean_sheets`: Current season cumulative totals.

---

### 4. `fpl.silver.player_gw_history`
*3-Year Gameweek Match Log Fact Table (~50,000+ rows).*
- `player_key` (int, FK): Crosswalk durable player code.
- `season` (string): Season identifier (`"2023-24"`, `"2024-25"`, `"2025-26"`).
- `gameweek` (int): Gameweek number (1 to 38).
- `total_points` (int): Points scored in match.
- `minutes` (int): Minutes played.
- `goals_scored`, `assists`, `clean_sheets`, `goals_conceded`, `yellow_cards`, `red_cards` (int): Match events.
- `expected_goals` (`xG`, float): Expected goals quality.
- `expected_assists` (`xA`, float): Expected assists quality.
- `ict_index`, `influence`, `creativity`, `threat` (float): FPL underlying metrics.
- `was_home` (boolean): `true` if Home match, `false` if Away match.
- `opponent_team_id` (int, FK): Opponent Team ID.
- `opponent_short_name` (string): Opponent 3-letter code (`"MCI"`, `"ARS"`).

---

### 5. `fpl.silver.fixtures`
*Cleaned 380-Match Schedule Table.*
- `fixture_id` (int, Primary Key): Fixture ID.
- `gameweek` (int): Gameweek number (1 to 38).
- `home_team_id` & `away_team_id` (int, FK): Team IDs.
- `home_team_name` & `away_team_name` (string): Team full names.
- `home_team_short` & `away_team_short` (string): 3-letter codes.
- `finished` (boolean): `true` if match finished, `false` if upcoming.
- `home_fdr` & `away_fdr` (int): Official Fixture Difficulty Ratings (FDR 1 to 5).
- `kickoff_time` (timestamp): Kickoff timestamp.

---

## 4. Architectural Deduplication & Safety Highlights

1. **Strict Priority Windowing:**
   - When joining historical archives, a player could match on both durable code AND full name.
   - `02_build_player_crosswalk.py` applies:
     `Window.partitionBy("season", "source_player_id").orderBy("priority")`
     ensuring priority 1 (Durable Code) always takes precedence over priority 2 (Full Name), guaranteeing 0 duplicate rows!

2. **Null Pre-Season Coalesce:**
   - Before Gameweek 1 starts, team strength columns in the raw API are null.
   - `01_build_team_crosswalk.py` uses `F.coalesce(F.col("strength"), F.lit(3))` to inject neutral baseline fallbacks, ensuring Gold layer algorithms never crash during pre-season!
