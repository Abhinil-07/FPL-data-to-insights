# 🏆 Medallion Architecture Guide — Layer 3: Gold Analytics & 2-Tier Strategy (`fpl.gold.*`)

## 1. Executive Summary & Purpose

The **Gold Layer** is the business-level analytical and presentation tier of the Medallion Architecture.

### Core Principles:
- **No Machine Learning / No Point Forecasting:** All scores are diagnostic and descriptive analytics based on historical match logs, rolling fixture ease, and underlying quality (xG/xA).
- **Position-Normalized Scoring:** All Z-scores and value rankings are computed WITHIN position groups (`GKP`, `DEF`, `MID`, `FWD`) separately, never pooled across positions.
- **2-Tier Squad Strategy:** Categorizes players into:
  - 🛡️ **Season Core Anchors (Set & Forget):** Premium, high-reliability holds (**Haaland**, **Palmer**, **Gabriel**, **Saka**, **Raya**) to lock into your squad long-term.
  - 🔄 **Rolling Transfer Targets:** Budget/Mid-price spot players (**Rogers**, **Mbeumo**, **Ekitiké**, **Konsa**) evaluated on a rolling 5-GW fixture horizon for your 1–2 weekly free transfers.

---

## 2. Gold Notebook Breakdown

The Gold layer is populated by **10 specialized Databricks notebooks** in `notebooks/gold/`:

| Notebook Name | Source Silver Tables | Target Gold Table | Description & Output |
|---|---|---|---|
| `01_build_gold_value_scores.py` | `silver.players`, `silver.fixtures` | `fpl.gold.value_scores` | Composite value Z-scores per position group + 2-tier strategy tier classification. |
| `02_build_gold_player_trends.py` | `silver.player_gw_history` | `fpl.gold.player_trends` | 3-year Home vs. Away performance splits (points, goals, assists per game). |
| `03_build_gold_matchup_history.py` | `silver.player_gw_history`, `silver.teams` | `fpl.gold.matchup_history` | Performance breakdown against Top-6 teams vs. Promoted teams. |
| `04_build_gold_team_trends.py` | `silver.fixtures`, `silver.teams` | `fpl.gold.team_trends` | Rolling 6-game team attacking and defensive form (goals scored, clean sheets). |
| `05_build_gold_fixture_planner.py` | `silver.fixtures`, `silver.teams` | `fpl.gold.fixture_planner` | Wide 19-gameweek fixture difficulty heatmap matrix with numeric `fdr_gw_1..19` columns. |
| `06_build_gold_differentials.py` | `silver.players`, `silver.player_gw_history` | `fpl.gold.differentials` | Low-ownership (<10%) budget gems with top goal involvements per 90 (`gi_per_90`). |
| `07_build_gold_price_momentum.py` | `silver.players` | `fpl.gold.price_momentum` | Transfer velocity tracking players trending towards price rise 📈 or fall 📉. |
| `08_build_gold_underlying_stats.py` | `silver.players`, `silver.player_gw_history` | `fpl.gold.underlying_stats` | Expected goals (xG) vs actual goals delta (`total_xg - total_goals`) flagging players "Due a Goal 🎯". |
| `09_build_gold_my_squad_tracker.py` | `bronze.my_team_raw`, `silver.players` | `fpl.gold.my_squad_tracker` | Personal squad weekly rank, points, and captain choices tracker. |
| `10_build_gold_captaincy_fit.py` | `silver.players`, `silver.player_gw_history` | `fpl.gold.captaincy_fit` | Dedicated weekly captaincy ranking based on haul frequency (10+ pt games), FDR, and form. |

---

## 3. Gold Table Schemas & Key Fields

### 1. `fpl.gold.value_scores`
*Position-Normalized Composite Value Scores & Strategy Classification.*
- `player_key` (int, PK): Durable player code.
- `web_name` & `team_name` (string): Player name and team.
- `position_name` (string): Position group (`GKP`, `DEF`, `MID`, `FWD`).
- `price_gbp` & `ownership_percent` (float): Current price and ownership %.
- `form` (float): Current form rating.
- `form_z` (float): Position-normalized Form Z-score.
- `avg_upcoming_fdr` (float): Upcoming 5-gameweek average FDR.
- `fixture_ease_z` (float): Position-normalized Fixture Ease Z-score.
- `minutes_reliability_z` (float): Position-normalized Minutes Reliability Z-score.
- `value_score` (float): Composite Z-score (`0.50*form_z + 0.35*fixture_ease_z + 0.15*minutes_reliability_z`).
- `strategy_tier` (string): Strategy classification (`"🛡️ Season Anchor (Set & Forget)"` vs `"🔄 Rolling Transfer Target"`).

---

### 2. `fpl.gold.fixture_planner`
*Wide 19-Gameweek Fixture Difficulty Matrix.*
- `team_id` (int, PK): Team ID.
- `team_name` & `short_name` (string): Team display names (`"Arsenal"`, `"ARS"`).
- `avg_5gw_fdr` (float): Upcoming 5-gameweek average FDR.
- `avg_midseason_fdr` (float): 19-gameweek mid-season average FDR.
- `next_gw_1` through `next_gw_19` (string): Opponent & venue descriptions (e.g. `"COV (H) FDR:2"`).
- `fdr_gw_1` through `fdr_gw_19` (int): Numeric FDR ratings (1 to 5) for Power BI & Databricks color cell formatting.

---

### 3. `fpl.gold.captaincy_fit`
*Weekly Dedicated Captaincy Selection Panel.*
- `captaincy_rank` (int): Rank #1, #2, #3...
- `player_key` (int, PK): Durable player code.
- `web_name` & `team_name` (string): Player display name and club.
- `position_name` (string): Position (`MID` / `FWD`).
- `price_gbp` (float): Price in £m.
- `haul_frequency_percent` (float): Percentage of historical matches with 10+ points.
- `captaincy_fit_score` (float): Weighted captaincy rating.

---

### 4. `fpl.gold.differentials`
*Low-Ownership (<10%) Hidden Gems.*
- `differential_rank` (int): Rank #1, #2, #3...
- `web_name` & `team_name` (string): Player name and club.
- `position_name` (string): Position group.
- `price_gbp` (float): Price.
- `ownership_percent` (float): Ownership % (strictly < 10.0%).
- `gi_per_90` (float): Goal involvements per 90 minutes (`(goals + assists) / (minutes / 90)`).
- `total_xg` & `total_xa` (float): Cumulative expected goals and expected assists.

---

### 5. `fpl.gold.underlying_stats`
*xG & xA Quality Delta Watchlist.*
- `web_name` & `team_name` (string): Player name and team.
- `position_name` (string): Position group.
- `price_gbp` (float): Price.
- `total_points` & `total_goals` (int): Actual scored totals.
- `total_xg` & `total_xa` (float): Expected goals and expected assists.
- `xg_delta` (float): `total_xg - total_goals`.
- `due_a_return_flag` (string): Flagged as `"Due a Goal 🎯 (Underperforming xG)"` when `total_xg > total_goals`.

---

### 6. `fpl.gold.price_momentum`
*Transfer Velocity Price Rise & Fall Tracker.*
- `web_name` & `team_name` (string): Player name and club.
- `price_gbp` (float): Price.
- `net_transfers_event` (int): Net transfers in minus transfers out in current event.
- `momentum_direction` (string): Flagged as `"Price Rise Candidate 📈"` or `"Price Fall Candidate 📉"`.
