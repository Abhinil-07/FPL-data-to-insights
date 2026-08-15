# 📥 Medallion Architecture Guide — Layer 1: Bronze Ingestion (`fpl.bronze.*`)

## 1. Executive Summary & Purpose

The **Bronze Layer** is the foundational raw landing zone of the Medallion Data Engineering Architecture.

### Core Objectives:
- **Raw Fidelity Storage:** Preserves incoming JSON payloads from the **official FPL API** (80+ raw player fields) and raw CSV structures from the **`vaastav/Fantasy-Premier-League` 3-year historical archive** exactly as received, without altering raw data values or column schemas.
- **Delta Lake Persistence:** All raw data is persisted into **Delta Lake tables** under the `fpl.bronze` catalog/schema, enabling ACID transactions, time-travel auditing, and rapid query performance.
- **Audit Traceability:** Every single record ingested is automatically tagged with an `_ingested_at` UTC timestamp to track data freshness.

---

## 2. Ingestion Notebook Breakdown

The Bronze layer is populated by **8 specialized Databricks notebooks** located in `notebooks/bronze/`:

| Notebook Name | Data Source | Target Bronze Delta Table | Description & Output |
|---|---|---|---|
| `00_init_schemas.py` | Metadata | Catalog & Schemas | Initializes `fpl.bronze`, `fpl.silver`, `fpl.gold` Unity Catalog schemas. |
| `01_ingest_players_raw.py` | Official FPL API | `fpl.bronze.players_raw` | Ingests live player master payload (80+ raw columns for 600+ players) from `bootstrap-static/ elements`. |
| `02_ingest_teams_raw.py` | Official FPL API | `fpl.bronze.teams_raw` | Ingests 20 Premier League teams master metadata, short names, home/away strength ratings. |
| `03_ingest_events_raw.py` | Official FPL API | `fpl.bronze.events_raw` | Ingests 38 gameweek calendar metadata, deadlines, finished status, and highest scoring managers. |
| `04_ingest_fixtures_raw.py` | Official FPL API | `fpl.bronze.fixtures_raw` | Ingests full 380-match Premier League schedule with home/away teams, kickoff times, and official FDR ratings. |
| `05_ingest_my_team_raw.py` | Official FPL API | `fpl.bronze.my_team_raw` | Automatically pulls the user's personal squad, rank over time, transfers, and captain picks via FPL Team ID. |
| `06_ingest_github_archive.py` | GitHub Archive | `fpl.bronze.archive_player_gws` | Pulls ~50,000+ gameweek match logs across 3 seasons (`2023-24`, `2024-25`, `2025-26`) with xG, xA, ICT, DefCon. |
| `06_ingest_github_archive.py` | GitHub Archive | `fpl.bronze.archive_players_raw` | Pulls seasonal player master snapshots across 3 seasons for crosswalk reconciliation. |

---

## 3. Bronze Table Schemas & Complete Column Catalog

### 1. `fpl.bronze.players_raw` (Complete 80+ Raw API Fields)
*Raw player master payload landed directly from official FPL API (`bootstrap-static/ elements`). Preserves 100% raw fidelity.*

#### 🆔 Identifiers & Bio Metadata
- `id` (BIGINT, PK): Current season live player ID.
- `code` (BIGINT): Durable FPL player code (unique across all seasons).
- `first_name` (STRING): First name.
- `second_name` (STRING): Last name.
- `web_name` (STRING): Display shirt name (e.g. `"Haaland"`).
- `element_type` (BIGINT): Position code (`1`=GKP, `2`=DEF, `3`=MID, `4`=FWD).
- `team` (BIGINT, FK): Team ID (1 to 20).
- `team_code` (BIGINT): Durable team code.
- `squad_number` (BIGINT): Shirt number.
- `photo` (STRING): Player headshot filename.

#### 💷 Price, Transfers & Ownership Metrics
- `now_cost` (BIGINT): Price in tenths of a million (e.g. `155` = £15.5m).
- `now_cost_rank` (BIGINT): Overall price rank.
- `now_cost_rank_type` (BIGINT): Price rank within position group.
- `cost_change_event` (BIGINT): Price change in current gameweek.
- `cost_change_event_fall` (BIGINT): Price drop in current gameweek.
- `cost_change_start` (BIGINT): Price change since start of season.
- `cost_change_start_fall` (BIGINT): Price drop since start of season.
- `price_change_percent` (STRING): Price change percentage.
- `selected_by_percent` (STRING): Ownership % (e.g. `"73.5"`).
- `selected_rank` (BIGINT): Overall ownership rank.
- `selected_rank_type` (BIGINT): Ownership rank within position group.
- `transfers_in` (BIGINT): Cumulative transfers in.
- `transfers_in_event` (BIGINT): Transfers in during current gameweek.
- `transfers_out` (BIGINT): Cumulative transfers out.
- `transfers_out_event` (BIGINT): Transfers out during current gameweek.

#### ⚽ Cumulative Season Match Performance
- `event_points` (BIGINT): Points scored in current gameweek.
- `total_points` (BIGINT): Cumulative total points scored.
- `points_per_game` (STRING): Average points per game.
- `form` (STRING): Current form rating over recent gameweeks.
- `minutes` (BIGINT): Cumulative minutes played.
- `goals_scored` (BIGINT): Goals scored.
- `assists` (BIGINT): Assists.
- `clean_sheets` (BIGINT): Clean sheets.
- `goals_conceded` (BIGINT): Goals conceded.
- `own_goals` (BIGINT): Own goals.
- `penalties_saved` (BIGINT): Penalty saves.
- `penalties_missed` (BIGINT): Penalty misses.
- `yellow_cards` (BIGINT): Yellow cards.
- `red_cards` (BIGINT): Red cards.
- `saves` (BIGINT): Goalkeeper saves.
- `bonus` (BIGINT): Cumulative bonus points.
- `bps` (BIGINT): Bonus Points System raw score.

#### 📈 Underlying FPL Indices
- `influence` (STRING): Influence rating.
- `influence_rank` (BIGINT): Influence overall rank.
- `influence_rank_type` (BIGINT): Influence rank in position.
- `creativity` (STRING): Creativity rating.
- `creativity_rank` (BIGINT): Creativity overall rank.
- `creativity_rank_type` (BIGINT): Creativity rank in position.
- `threat` (STRING): Threat rating.
- `threat_rank` (BIGINT): Threat overall rank.
- `threat_rank_type` (BIGINT): Threat rank in position.
- `ict_index` (STRING): Composite ICT index.
- `ict_index_rank` (BIGINT): ICT overall rank.
- `ict_index_rank_type` (BIGINT): ICT rank in position.
- `ep_next` (STRING): Expected points for next round.
- `ep_this` (VOID/STRING): Expected points for current round.

#### 🩺 Player Availability & News Flags
- `status` (STRING): Availability status (`"a"`=Available, `"d"`=Doubtful, `"i"`=Injured, `"u"`=Unavailable).
- `chance_of_playing_next_round` (DOUBLE): Chance % of playing next round (e.g. `100.0`, `75.0`, `0.0`).
- `chance_of_playing_this_round` (VOID/DOUBLE): Chance % of playing current round.
- `news` (STRING): Official injury/transfer news notes.
- `news_added` (TIMESTAMP): Timestamp news was posted.

#### ⚙️ System & Transaction Controls
- `can_transact` (BOOLEAN): Can be transferred flag.
- `can_select` (BOOLEAN): Can be selected in squad flag.
- `in_dreamteam` (BOOLEAN): Currently in Dream Team flag.
- `dreamteam_count` (BIGINT): Number of times in Dream Team.
- `corners_and_indirect_freekicks_order` (BIGINT/VOID): Corner taker order.
- `direct_freekicks_order` (BIGINT/VOID): Free kick taker order.
- `penalties_order` (BIGINT/VOID): Penalty taker order.
- `_ingested_at` (TIMESTAMP): Bronze UTC ingestion timestamp.

---

### 2. `fpl.bronze.teams_raw`
*Live team metadata payload from official FPL API (`bootstrap-static/ teams`).*
- `id` (BIGINT, PK): Team ID (1 to 20).
- `code` (BIGINT): Durable team code.
- `name` (STRING): Full team name (e.g. `"Arsenal"`, `"Liverpool"`).
- `short_name` (STRING): 3-letter abbreviation (e.g. `"ARS"`, `"LIV"`).
- `strength` (BIGINT): General overall team strength rating (1 to 5).
- `strength_overall_home` & `strength_overall_away`: Home/Away specific strength indices.
- `strength_attack_home` & `strength_defence_home`: Granular attacking & defensive ratings.
- `pulse_id` (BIGINT): Premier League Pulse ID.
- `_ingested_at` (TIMESTAMP): Bronze UTC ingestion timestamp.

---

### 3. `fpl.bronze.fixtures_raw`
*Full 380-match Premier League schedule (`fixtures/`).*
- `id` (BIGINT, PK): Unique fixture match ID.
- `event` (BIGINT): Gameweek number (1 through 38).
- `home_team` & `away_team` (BIGINT, FK): Team IDs for home and away clubs.
- `finished` (BOOLEAN): `true` if match completed, `false` if upcoming.
- `team_h_difficulty` & `team_a_difficulty` (BIGINT): Official Fixture Difficulty Rating (FDR 1 to 5).
- `kickoff_time` (TIMESTAMP): Kickoff timestamp.
- `_ingested_at` (TIMESTAMP): Bronze UTC ingestion timestamp.

---

### 4. `fpl.bronze.archive_player_gws`
*3-Year Gameweek-level historical match logs from `vaastav/Fantasy-Premier-League` repo.*
- `season` (STRING): Season identifier (e.g. `"2023-24"`, `"2024-25"`, `"2025-26"`).
- `name` (STRING): Historical player full name (e.g. `"Erling_Haaland_355"`).
- `element` (STRING): Season-specific player ID.
- `GW` (STRING): Gameweek number.
- `total_points` (STRING): Points scored in that specific match.
- `minutes`, `goals_scored`, `assists`, `clean_sheets`: Per-match stats.
- `expected_goals` (`xG`), `expected_assists` (`xA`): Underlying chance quality metrics.
- `ict_index`, `influence`, `creativity`, `threat`: FPL underlying indices.
- `_ingested_at` (TIMESTAMP): Bronze UTC ingestion timestamp.

---

## 4. Key Architectural Insights & Innovations

1. **Schema Sanitization (`sanitize_df_for_delta`):**
   - Official FPL API payloads contain nested JSON objects/lists (e.g. `element_type` metadata or fixture stat arrays).
   - Our Bronze ingestion pipelines automatically convert complex Python dictionaries/lists to JSON strings before writing to Delta Lake, preventing schema mismatch crashes!

2. **Cross-Season Schema Harmonization:**
   - GitHub archive headers vary slightly across historical seasons (e.g. `xG` vs `expected_goals`).
   - `06_ingest_github_archive.py` casts historical fields to compatible string representations at landing, ensuring smooth downstream reconciliation in the Silver layer without losing historical rows!
