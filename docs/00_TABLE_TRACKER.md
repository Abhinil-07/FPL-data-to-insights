# 🗺️ Master Table Tracker — Medallion Architecture Table & Column Registry

## 1. Architecture Overview

| Medallion Layer | Database Schema | Table Count | Storage Engine | Purpose |
|---|---|---|---|---|
| 📥 **Bronze Layer** | `fpl.bronze` | 7 Tables | Delta Lake | Raw landing zone for official FPL API (80+ raw fields) & 3-year historical archive. |
| 🧹 **Silver Layer** | `fpl.silver` | 5 Tables | Delta Lake | Cleansed, normalized dimensions, facts, and identity crosswalk. |
| 🏆 **Gold Layer** | `fpl.gold` | 10 Tables | Delta Lake | Business-level analytical tables & 2-tier decision-support models. |

---

## 2. 📥 Bronze Layer Tables (`fpl.bronze.*`)

### Table 1: `fpl.bronze.players_raw` (Complete 80+ Raw API Fields)
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

### Table 2: `fpl.bronze.teams_raw`
*Live team metadata payload from official FPL API (`bootstrap-static/ teams`).*
- `id` (BIGINT, PK): Team ID (1 to 20).
- `code` (BIGINT): Durable team code.
- `name` (STRING): Team full name.
- `short_name` (STRING): 3-letter abbreviation (`"ARS"`).
- `strength` (BIGINT): Overall team rating.
- `strength_overall_home` (BIGINT): Home strength.
- `strength_overall_away` (BIGINT): Away strength.
- `strength_attack_home` (BIGINT): Home attack strength.
- `strength_attack_away` (BIGINT): Away attack strength.
- `strength_defence_home` (BIGINT): Home defense strength.
- `strength_defence_away` (BIGINT): Away defense strength.
- `pulse_id` (BIGINT): Premier League Pulse ID.
- `_ingested_at` (TIMESTAMP): Ingestion timestamp.

### Table 3: `fpl.bronze.events_raw`
*38 Gameweek calendar metadata payload (`bootstrap-static/ events`).*
- `id` (BIGINT, PK): Gameweek number (1 to 38).
- `name` (STRING): Gameweek display name (`"Gameweek 1"`).
- `deadline_time` (TIMESTAMP): Transfer deadline.
- `finished` (BOOLEAN): Completion status.
- `is_current` (BOOLEAN): Active gameweek flag.
- `_ingested_at` (TIMESTAMP): Ingestion timestamp.

### Table 4: `fpl.bronze.fixtures_raw`
*Full 380-match Premier League schedule (`fixtures/`).*
- `id` (BIGINT, PK): Fixture match ID.
- `event` (BIGINT): Gameweek number.
- `home_team` (BIGINT, FK): Home team ID.
- `away_team` (BIGINT, FK): Away team ID.
- `finished` (BOOLEAN): Completion status.
- `team_h_difficulty` (BIGINT): Home FDR rating.
- `team_a_difficulty` (BIGINT): Away FDR rating.
- `kickoff_time` (TIMESTAMP): Kickoff timestamp.
- `_ingested_at` (TIMESTAMP): Ingestion timestamp.

### Table 5: `fpl.bronze.my_team_raw`
*Personal squad payload via user FPL Team ID.*
- `event` (BIGINT): Gameweek number.
- `overall_rank` (BIGINT): Overall rank.
- `total_points` (BIGINT): Total cumulative points.
- `picks` (STRING): JSON string of squad player picks and captain selections.
- `_ingested_at` (TIMESTAMP): Ingestion timestamp.

### Table 6: `fpl.bronze.archive_player_gws`
*3-Year historical match logs (~50,000+ rows) from `vaastav/Fantasy-Premier-League` repo.*
- `season` (STRING): Season identifier (`"2023-24"`, `"2024-25"`, `"2025-26"`).
- `name` (STRING): Historical full name identifier.
- `element` (STRING): Historical player ID.
- `GW` (STRING): Gameweek number.
- `total_points` (STRING): Points scored.
- `minutes` (STRING): Minutes played.
- `goals_scored` (STRING): Goals scored.
- `assists` (STRING): Assists.
- `expected_goals` (STRING): xG metric.
- `expected_assists` (STRING): xA metric.
- `ict_index` (STRING): ICT rating.
- `_ingested_at` (TIMESTAMP): Ingestion timestamp.

### Table 7: `fpl.bronze.archive_players_raw`
*3-Year historical player master snapshots.*
- `season` (STRING): Season identifier.
- `id` (STRING): Historical player ID.
- `code` (STRING): Durable FPL code.
- `first_name` (STRING): First name.
- `second_name` (STRING): Last name.
- `_ingested_at` (TIMESTAMP): Ingestion timestamp.

---

## 3. 🧹 Silver Layer Tables (`fpl.silver.*`)

### Table 1: `fpl.silver.teams`
*Cleansed 20-team master dimension table.*
- `team_id` (INT, PK): Team ID.
- `team_code` (INT): Durable team code.
- `team_name` (STRING): Team name (`"Arsenal"`).
- `short_name` (STRING): 3-letter code (`"ARS"`).
- `strength` (INT): Overall rating.
- `strength_overall_home` (INT): Home strength index.
- `strength_overall_away` (INT): Away strength index.
- `strength_attack_home` (INT): Home attack index.
- `strength_attack_away` (INT): Away attack index.
- `strength_defence_home` (INT): Home defense index.
- `strength_defence_away` (INT): Away defense index.
- `_ingested_at` (TIMESTAMP): Ingestion timestamp.

### Table 2: `fpl.silver.player_crosswalk`
*Durable identity resolution mapping table.*
- `player_key` (INT, PK): Durable FPL code (unique player ID across all 3+ seasons).
- `season` (STRING): Season identifier (`"2023-24"`, `"2024-25"`, `"2025-26"`, `"2026-27"`).
- `source` (STRING): Source identifier (`"live_api"` vs `"archive"`).
- `source_player_id` (INT): Source-specific player ID.
- `first_name` (STRING): First name.
- `second_name` (STRING): Last name.
- `crosswalk_status` (STRING): Matching method (`"live_master"`, `"matched_by_durable_code"`, `"matched_by_full_name"`).

### Table 3: `fpl.silver.players`
*Current Season Master Player Dimension (Filters out 70+ raw noise columns!).*
- `player_key` (INT, PK): Durable player code.
- `player_id` (INT): Current season live player ID.
- `web_name` (STRING): Shirt display name.
- `first_name` (STRING): First name.
- `second_name` (STRING): Last name.
- `position_name` (STRING): Position group (`"GKP"`, `"DEF"`, `"MID"`, `"FWD"`).
- `team_id` (INT, FK): Team ID.
- `team_name` (STRING): Full team name.
- `team_short_name` (STRING): 3-letter code.
- `price_gbp` (FLOAT): Price in £m.
- `ownership_percent` (FLOAT): Ownership %.
- `form` (FLOAT): Form score.
- `minutes` (INT): Cumulative minutes.
- `goals_scored` (INT): Cumulative goals.
- `assists` (INT): Cumulative assists.
- `clean_sheets` (INT): Cumulative clean sheets.
- `_ingested_at` (TIMESTAMP): Ingestion timestamp.

### Table 4: `fpl.silver.player_gw_history`
*3-Year Gameweek Match Log Fact Table (~50,000+ rows).*
- `player_key` (INT, FK): Durable player code.
- `season` (STRING): Season identifier.
- `gameweek` (INT): Gameweek number.
- `total_points` (INT): Match points.
- `minutes` (INT): Minutes played.
- `goals_scored` (INT): Goals.
- `assists` (INT): Assists.
- `clean_sheets` (INT): Clean sheets.
- `goals_conceded` (INT): Goals conceded.
- `expected_goals` (FLOAT): Match xG.
- `expected_assists` (FLOAT): Match xA.
- `ict_index` (FLOAT): ICT Index.
- `was_home` (BOOLEAN): Home match flag.
- `opponent_team_id` (INT, FK): Opponent Team ID.
- `opponent_short_name` (STRING): Opponent 3-letter code.

### Table 5: `fpl.silver.fixtures`
*Cleaned 380-Match Schedule Table.*
- `fixture_id` (INT, PK): Fixture ID.
- `gameweek` (INT): Gameweek number.
- `home_team_id` (INT, FK): Home team ID.
- `away_team_id` (INT, FK): Away team ID.
- `home_team_name` (STRING): Home team name.
- `away_team_name` (STRING): Away team name.
- `home_team_short` (STRING): Home 3-letter code.
- `away_team_short` (STRING): Away 3-letter code.
- `finished` (BOOLEAN): Completion status.
- `home_fdr` (INT): Home FDR rating.
- `away_fdr` (INT): Away FDR rating.
- `kickoff_time` (TIMESTAMP): Kickoff timestamp.

---

## 4. 🏆 Gold Layer Tables (`fpl.gold.*`)

### Table 1: `fpl.gold.value_scores`
*Position-Normalized Composite Value Scores & Strategy Classification.*
- `player_key` (INT, PK): Durable player code.
- `player_id` (INT): Current live player ID.
- `web_name` (STRING): Display name.
- `team_name` (STRING): Team name.
- `team_short_name` (STRING): 3-letter code.
- `position_name` (STRING): Position (`"GKP"`, `"DEF"`, `"MID"`, `"FWD"`).
- `price_gbp` (FLOAT): Price in £m.
- `ownership_percent` (FLOAT): Ownership %.
- `form` (FLOAT): Form score.
- `form_z` (FLOAT): Position-normalized Form Z-score.
- `avg_upcoming_fdr` (FLOAT): Upcoming 5-GW average FDR.
- `fixture_ease_z` (FLOAT): Position-normalized Fixture Ease Z-score.
- `minutes` (INT): Minutes played.
- `minutes_reliability_z` (FLOAT): Position-normalized Minutes Z-score.
- `value_score` (FLOAT): Composite Z-score (`0.50*form_z + 0.35*fixture_ease_z + 0.15*minutes_reliability_z`).
- `strategy_tier` (STRING): Strategy tier (`"🛡️ Season Anchor (Set & Forget)"` vs `"🔄 Rolling Transfer Target"`).
- `_updated_at` (TIMESTAMP): Update timestamp.

### Table 2: `fpl.gold.fixture_planner`
*Wide 19-Gameweek Fixture Difficulty Matrix.*
- `team_id` (INT, PK): Team ID.
- `team_name` (STRING): Full team name.
- `short_name` (STRING): 3-letter code.
- `avg_5gw_fdr` (FLOAT): 5-GW average FDR.
- `avg_midseason_fdr` (FLOAT): 19-GW mid-season average FDR.
- `next_gw_1` through `next_gw_19` (STRING): Opponent & venue descriptions (e.g. `"COV (H) FDR:2"`).
- `fdr_gw_1` through `fdr_gw_19` (INT): Numeric FDR ratings (1 to 5) for cell formatting.
- `_updated_at` (TIMESTAMP): Update timestamp.

### Table 3: `fpl.gold.captaincy_fit`
*Weekly Dedicated Captaincy Selection Panel.*
- `captaincy_rank` (INT): Rank order (1, 2, 3...).
- `player_key` (INT, PK): Durable player code.
- `web_name` (STRING): Player name.
- `team_name` (STRING): Team name.
- `position_name` (STRING): Position (`MID`/`FWD`).
- `price_gbp` (FLOAT): Price.
- `haul_frequency_percent` (FLOAT): % of historical matches with 10+ points.
- `captaincy_fit_score` (FLOAT): Weighted captaincy fit score.
- `_updated_at` (TIMESTAMP): Update timestamp.

### Table 4: `fpl.gold.differentials`
*Low-Ownership (<10%) Hidden Gems.*
- `differential_rank` (INT): Rank order (1, 2, 3...).
- `player_key` (INT, PK): Durable player code.
- `web_name` (STRING): Player name.
- `team_name` (STRING): Team name.
- `position_name` (STRING): Position.
- `price_gbp` (FLOAT): Price.
- `ownership_percent` (FLOAT): Ownership % (<10.0%).
- `gi_per_90` (FLOAT): Goal involvements per 90 minutes.
- `total_xg` (FLOAT): Cumulative xG.
- `total_xa` (FLOAT): Cumulative xA.
- `_updated_at` (TIMESTAMP): Update timestamp.

### Table 5: `fpl.gold.underlying_stats`
*Expected Goals (xG) vs Actual Goals Delta Watchlist.*
- `player_key` (INT, PK): Durable player code.
- `web_name` (STRING): Player name.
- `team_name` (STRING): Team name.
- `position_name` (STRING): Position.
- `price_gbp` (FLOAT): Price.
- `total_points` (INT): Total points.
- `total_goals` (INT): Actual goals scored.
- `total_xg` (FLOAT): Total expected goals.
- `total_xa` (FLOAT): Total expected assists.
- `xg_delta` (FLOAT): `total_xg - total_goals`.
- `due_a_return_flag` (STRING): Flagged as `"Due a Goal 🎯 (Underperforming xG)"`.
- `_updated_at` (TIMESTAMP): Update timestamp.

### Table 6: `fpl.gold.price_momentum`
*Transfer Velocity Price Rise & Fall Tracker.*
- `player_key` (INT, PK): Durable player code.
- `web_name` (STRING): Player name.
- `team_name` (STRING): Team name.
- `position_name` (STRING): Position.
- `price_gbp` (FLOAT): Price.
- `net_transfers_event` (INT): Net transfers in minus transfers out.
- `momentum_direction` (STRING): `"Price Rise Candidate 📈"` vs `"Price Fall Candidate 📉"`.
- `_updated_at` (TIMESTAMP): Update timestamp.

### Table 7: `fpl.gold.player_trends`
*3-Year Home vs Away Performance Splits.*
- `player_key` (INT, PK): Durable player code.
- `web_name` (STRING): Player name.
- `position_name` (STRING): Position.
- `home_ppg` (FLOAT): Points per game at Home.
- `away_ppg` (FLOAT): Points per game Away.
- `ppg_home_diff` (FLOAT): `home_ppg - away_ppg`.
- `_updated_at` (TIMESTAMP): Update timestamp.

### Table 8: `fpl.gold.matchup_history`
*Historical Performance vs Top-6 Teams.*
- `player_key` (INT, PK): Durable player code.
- `web_name` (STRING): Player name.
- `opp_team_id` (INT, FK): Opponent team ID.
- `opp_short_name` (STRING): Opponent short code (`"MCI"`, `"ARS"`).
- `matches_played` (INT): Matches played against opponent.
- `avg_points` (FLOAT): Average points scored vs opponent.
- `_updated_at` (TIMESTAMP): Update timestamp.

### Table 9: `fpl.gold.team_trends`
*Rolling Team Attacking & Defensive Form.*
- `team_id` (INT, PK): Team ID.
- `team_name` (STRING): Team name.
- `short_name` (STRING): 3-letter code.
- `rolling_6_goals_scored` (INT): Goals scored in last 6 games.
- `rolling_6_goals_conceded` (INT): Goals conceded in last 6 games.
- `rolling_6_clean_sheets` (INT): Clean sheets in last 6 games.
- `_updated_at` (TIMESTAMP): Update timestamp.

### Table 10: `fpl.gold.my_squad_tracker`
*Personal Squad Weekly Performance & Rank Tracker.*
- `event` (INT, PK): Gameweek event.
- `overall_rank` (INT): Rank.
- `total_points` (INT): Total cumulative points.
- `squad_summary` (STRING): Summary string of active squad picks.
- `_updated_at` (TIMESTAMP): Update timestamp.
