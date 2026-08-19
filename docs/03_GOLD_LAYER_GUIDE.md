# 🏆 Medallion Architecture Guide — Layer 3: Gold Analytics & Decision Support (`fpl.gold.*`)

---

## 1. Executive Summary & Architectural Principles

The **Gold Layer** is the business presentation and decision-support tier of the FPL Medallion Architecture. It consumes conformed, clean data from the Silver Layer to produce **10 specialized analytical data marts** designed to eliminate cognitive bias and solve tactical FPL problems.

### Core Architectural Principles:
1. **Descriptive & Diagnostic Focus (No Black-Box ML):** Every score is transparent, explainable, and derived from underlying statistical quality ($xG$, $xA$, $xGI$, $xGC$, ICT Index, venue splits, and match logs).
2. **Position-Normalized Scoring:** Scoring mechanics differ fundamentally across football positions. Clean sheets and saves govern Goalkeepers/Defenders; chance creation and finishing govern Midfielders/Forwards. All statistical Z-scores and value rankings are computed strictly within position peer groups (`GKP`, `DEF`, `MID`, `FWD`), never pooled across positions.
3. **Dual-Ranking Paradigm (Quality vs. Value):**
   * **`quality_score` (The Star Core):** Identifies high-ceiling, premium haulers for your starting XI and weekly captaincy choices.
   * **`value_score` (The Budget Enablers):** Identifies high-ROI, guaranteed 90-minute starters in low price brackets (£4.5m–£6.5m) to fund premium stars.
4. **2-Tier Tactical Strategy Classification:**
   * 🛡️ **Season Core Anchors (Set & Forget):** Premium, fixture-proof holds (**Haaland**, **Palmer**, **Gabriel**, **Saka**, **Bruno Fernandes**, **Raya**) to lock into your squad long-term without wasting weekly transfers.
   * 🔄 **Rolling Transfer Targets:** Budget/Mid-price spot players (**Rogers**, **Mbeumo**, **Ekitiké**, **Konsa**) evaluated on a rolling 5-GW fixture horizon for your 1–2 weekly free transfers.
5. **Adaptive Lifecycle (Pre-Season vs. In-Season):** Automatically derives baseline form from historical completed seasons when live season minutes equal 0, and seamlessly transitions to live rolling telemetry once official matches finish.

---

## 2. Gold Data Marts Breakdown & Dashboard Mapping

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                     GOLD DATA MARTS CATALOG                                     │
├────┬─────────────────────────────┬─────────────────────────────────┬─────────────────────────────┤
│ #  │ Table Name                  │ Primary Business Problem Solved │ Primary Dashboard View      │
├────┼─────────────────────────────┼─────────────────────────────────┼─────────────────────────────┤
│ 01 │ fpl.gold.value_scores       │ Transfer shortlisting & squad ROI│ Transfer Shortlist & Squad  │
│ 02 │ fpl.gold.player_trends      │ Home/Away & Multi-season trends │ Player Diagnostic Workspace │
│ 03 │ fpl.gold.matchup_history    │ Performance vs Top 6 & Promoted │ Opponent Matchup Radar      │
│ 04 │ fpl.gold.team_trends        │ Rolling 6-GW team attack/defense│ Club Momentum Matrix        │
│ 05 │ fpl.gold.fixture_planner    │ 19-GW Mid-Season difficulty map │ Fixture Swing & FDR Ticker  │
│ 06 │ fpl.gold.differentials      │ Low-ownership (<10%) hidden gems│ Differential Radar          │
│ 07 │ fpl.gold.price_momentum     │ Imminent price rise/fall timing │ Budget & Market Tracker     │
│ 08 │ fpl.gold.underlying_stats   │ xG/xA variance & regression (Due)│ Underlying Quality Layer   │
│ 09 │ fpl.gold.my_squad_tracker   │ Personal squad rank & bank flow │ Personal Portfolio Tracker  │
│ 10 │ fpl.gold.captaincy_fit      │ Weekly double-point captain pick│ Captaincy Optimization Panel│
└────┴─────────────────────────────┴─────────────────────────────────┴─────────────────────────────┘
```

---

## 3. Deep-Dive Table Documentation

---

### Data Mart 1: `fpl.gold.value_scores`

#### A. Business Problem Solved
* **Pre-Season Squad Builder (GW0):** Solves the initial £100.0m squad construction problem before GW1 kicks off by combining historical baseline output, starting 5-GW fixture difficulty, and set-piece taker hierarchies.
* **In-Season Transfer Shortlisting:** Solves weekly transfer prioritization by ranking transfer targets within position and budget limits, separating long-term anchors from fixture-swing targets.

#### B. Mathematical & Statistical Formulas

##### 1. Upcoming 5-Gameweek Fixture Ease Score
Extracts the next 5 upcoming matches chronologically per team and inverts the official 1–5 FDR:
$$\text{avg\_upcoming\_fdr} = \frac{1}{5} \sum_{i=1}^{5} \text{FDR}_i$$
$$\text{fixture\_ease\_score} = 5.0 - \text{avg\_upcoming\_fdr}$$
*(Higher score = Easier schedule. e.g., FDR 2.0 yields Ease 3.0; FDR 4.2 yields Ease 0.8)*.

##### 2. Position-Normalized Standardized Z-Scores
Computed strictly within each position group $P \in \{\text{GKP}, \text{DEF}, \text{MID}, \text{FWD}\}$:

$$Z_{\text{form}} = \frac{\text{Effective\_Form} - \mu_{\text{form}, P}}{\sigma_{\text{form}, P}}$$

$$Z_{\text{ease}} = \frac{\text{Fixture\_Ease\_Score} - \mu_{\text{ease}, P}}{\sigma_{\text{ease}, P}}$$

$$Z_{\text{min}} = \frac{\text{Effective\_Minutes} - \mu_{\text{min}, P}}{\sigma_{\text{min}, P}}$$

* **Pre-Season Mode:** $\text{Effective\_Form} = \text{hist\_ppg}$ (True Points Per Appearance with $\text{minutes} > 0$ from the previous season); $\text{Effective\_Minutes} = \text{hist\_minutes}$.
* **In-Season Mode:** $\text{Effective\_Form} = \text{form}$; $\text{Effective\_Minutes} = \text{minutes}$.

##### 3. Quality Score (The Star Ranking)
Weights pure point-scoring power with a sensible fixture modifier:
$$\mathbf{\text{Quality Score}} = 0.85 \times Z_{\text{form}} + 0.15 \times Z_{\text{ease}}$$
* **`position_quality_rank`:** $\text{ROW\_NUMBER() OVER (PARTITION BY position\_name ORDER BY quality\_score DESC)}$

##### 4. Value Score (The Budget ROI Ranking)
Measures standardized positive points/output generated per million pounds (£m) spent:
$$\mathbf{\text{Value Score}} = \frac{\text{Quality Score} + 3.0}{\text{Price (\pounds m)}}$$
* **`position_value_rank`:** $\text{ROW\_NUMBER() OVER (PARTITION BY position\_name ORDER BY value\_score DESC)}$

##### 5. 2-Tier Strategy Tier Classification Logic
```python
if position == "DEF" and (price_gbp >= 5.5 or ownership_percent >= 15.0):
    strategy_tier = "🛡️ Season Anchor (Set & Forget)"
elif position == "GKP" and (price_gbp >= 5.0 or ownership_percent >= 15.0):
    strategy_tier = "🛡️ Season Anchor (Set & Forget)"
elif position in ("MID", "FWD") and (price_gbp >= 9.0 or ownership_percent >= 25.0):
    strategy_tier = "🛡️ Season Anchor (Set & Forget)"
else:
    strategy_tier = "🔄 Rolling Transfer Target"
```

#### C. Output Schema & Data Dictionary

| Column Name | Data Type | Description & Business Impact |
|---|---|---|
| `player_key` | `INT` (PK) | Durable persistent Opta player ID across all seasons. |
| `player_id` | `INT` | Seasonal FPL element ID for the current 2026-27 season. |
| `web_name` | `STRING` | Standard display name (e.g., `"Haaland"`, `"Palmer"`, `"Gabriel"`). |
| `team_name` | `STRING` | Full club name (e.g., `"Manchester City"`, `"Chelsea"`). |
| `team_short_name` | `STRING` | 3-letter club code (e.g., `"MCI"`, `"CHE"`, `"ARS"`). |
| `position_name` | `STRING` | Position group (`GKP`, `DEF`, `MID`, `FWD`). |
| `price_gbp` | `DOUBLE` | Current player cost in millions (e.g., `15.0`, `10.5`, `5.5`). |
| `ownership_percent`| `DOUBLE` | Total FPL manager ownership percentage. |
| `position_quality_rank` | `INT` | Rank #1, #2, #3... by pure output power within position. |
| `quality_score` | `DOUBLE` | Position-normalized star rating ($0.85 \times Z_{\text{form}} + 0.15 \times Z_{\text{ease}}$). |
| `position_value_rank` | `INT` | Rank #1, #2, #3... by budget efficiency ROI per £m. |
| `value_score` | `DOUBLE` | Standardized points per million spent ($(\text{Quality} + 3.0) / \text{Price}$). |
| `strategy_tier` | `STRING` | Tag: `🛡️ Season Anchor (Set & Forget)` vs `🔄 Rolling Transfer Target`. |
| `effective_form` | `DOUBLE` | Active form value used in calculation (historical PPG or live form). |
| `hist_total_points`| `INT` | Total points accumulated in the previous baseline season. |
| `hist_goals` | `INT` | Total goals scored in the previous baseline season. |
| `hist_assists` | `INT` | Total assists scored in the previous baseline season. |
| `hist_clean_sheets`| `INT` | Total clean sheets kept in the previous baseline season. |
| `hist_ppg` | `DOUBLE` | True Points Per Appearance when played ($\text{minutes} > 0$). |
| `hist_xgi` | `DOUBLE` | Total Expected Goal Involvement ($xG + xA$) from baseline season. |
| `hist_minutes` | `INT` | Total minutes played in the previous baseline season. |
| `hist_matches_played` | `INT` | Count of matches with active minutes in baseline season. |
| `avg_upcoming_fdr` | `DOUBLE` | Average FDR across the next 5 upcoming matches. |
| `fixture_ease_score` | `DOUBLE` | Inverted FDR ease metric ($5.0 - \text{avg\_fdr}$). |
| `form_z` | `DOUBLE` | Standardized Form/PPG Z-score relative to position peers. |
| `fixture_ease_z` | `DOUBLE` | Standardized Fixture Ease Z-score relative to position peers. |
| `minutes_reliability_z` | `DOUBLE` | Standardized Minutes Z-score relative to position peers. |
| `is_penalty_taker` | `BOOLEAN` | `TRUE` if player is the #1 designated penalty taker. |
| `is_set_piece_taker`| `BOOLEAN`| `TRUE` if player takes direct free-kicks or primary corners. |
| `news` | `STRING` | Official injury, suspension, or transfer news update. |
| `chance_of_playing_next_round` | `INT` | Percentage availability (`0`, `25`, `50`, `75`, `100`, or `NULL` if fit). |
| `_updated_at` | `TIMESTAMP`| Ingestion and calculation timestamp. |

---

### Data Mart 2: `fpl.gold.player_trends` (Notebook 02)
* **Problem Solved:** Diagnostic multi-season drill-down into player performance splits by venue (Home vs. Away) and consistency over 3–4 seasons.
* **Core Metrics:** `matches_played`, `total_minutes`, `total_points`, `avg_points_per_game`, `goals_scored`, `assists`, `total_xg`, `total_xa`, `total_ict`.

---

### Data Mart 3: `fpl.gold.matchup_history` (Notebook 03)
* **Problem Solved:** Opponent-specific matchup analysis (how assets perform against Top-6 defenses vs. Promoted/Bottom-tier defenses).
* **Core Metrics:** Performance categorized by opponent tier (`Top 6`, `Promoted`, `Rest of League`).

---

### Data Mart 4: `fpl.gold.team_trends` (Notebook 04)
* **Problem Solved:** Rolling 6-game team momentum tracker (identifying defensive double-up targets or weak defenses to target).
* **Core Metrics:** `goals_scored_last_6`, `goals_conceded_last_6`, `clean_sheets_last_6`, `avg_goals_scored_last_6`.

---

### Data Mart 5: `fpl.gold.fixture_planner` (Notebook 05)
* **Problem Solved:** 19-gameweek mid-season fixture difficulty planning.
* **Core Metrics:** Wide matrix of text descriptors (`next_gw_1..19`) and numeric FDR values (`fdr_gw_1..19`) for heatmap visualization.

---

### Data Mart 6: `fpl.gold.differentials` (Notebook 06)
* **Problem Solved:** Surfaces low-ownership ($< 10\%$) hidden gems with top-tier underlying $xGI$ per 90 minutes.
* **Core Metrics:** `ownership_percent`, `total_gi`, `gi_per_90`, `differential_rank`.

---

### Data Mart 7: `fpl.gold.price_momentum` (Notebook 07)
* **Problem Solved:** Tracks net transfer velocity to time transfers before overnight price rises or falls.
* **Core Metrics:** `net_transfers_event`, `momentum_direction` (`Price Rise Candidate 📈` vs `Price Fall Candidate 📉`).

---

### Data Mart 8: `fpl.gold.underlying_stats` (Notebook 08)
* **Problem Solved:** Regression and variance detection ($xG$ vs. Actual Goals) to identify players who are statistically "Due a Goal 🎯".
* **Core Metrics:** `total_xg`, `total_goals`, `xg_delta`, `due_a_return_flag`.

---

### Data Mart 9: `fpl.gold.my_squad_tracker` (Notebook 09)
* **Problem Solved:** 360-degree personal team tracking (rank trajectory, weekly score vs. world average, available bank cash).
* **Core Metrics:** `overall_rank`, `overall_points`, `event_points`, `bank_gbp`, `team_value_gbp`.

---

### Data Mart 10: `fpl.gold.captaincy_fit` (Notebook 10)
* **Problem Solved:** Weekly captaincy selection matrix based on high-ceiling haul frequency ($10+$ point games), form, and opponent vulnerability.
* **Core Metrics:** `haul_frequency_percent`, `max_single_match_haul`, `captaincy_fit_score`, `captaincy_rank`.
