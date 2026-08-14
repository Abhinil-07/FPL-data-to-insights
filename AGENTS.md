# FPL Decision-Support Dashboard — Project Requirements

## Overview

A personal Fantasy Premier League (FPL) data engineering and BI project. The goal is to build a decision-support dashboard — NOT a predictive/machine-learning model — that helps the user make informed FPL squad, transfer, and captaincy decisions using historical and current-season data.

This is a solo portfolio project (unlike the user's Valorant pipeline, which is for a 5-person team).

## Platform & Architecture

- **Platform:** Databricks Free Edition
- **Architecture:** Medallion pattern (Bronze → Silver → Gold), consistent with the user's other portfolio projects (Valorant pipeline, FIFA 2026 World Cup pipeline)
- **Storage:** Delta Lake
- **Refresh model:** Manual trigger only — no scheduled/automated jobs. The user refreshes data on demand (e.g., before making a transfer decision), not on a daily cron.
- **Access:** Web-browsable dashboard, queryable anytime (Databricks SQL dashboard, or AI/BI Genie) — not restricted to a once-a-week deadline check.

## Data Sources

1. **FPL Official API** (`fantasy.premierleague.com/api/`)
   - `bootstrap-static` — all players, teams, current gameweek, positions, prices, ownership
   - `fixtures` — full season fixture list with fixture difficulty ratings (FDR)
   - `element-summary/{player_id}` — per-player current-season gameweek history
   - Team-specific endpoint via the user's **FPL Team ID** — pulls his own squad, gameweek points, and overall rank automatically (no manual entry)
2. **Historical archive** — `vaastav/Fantasy-Premier-League` GitHub repository
   - Gameweek-level historical data for the last 2–3 seasons
   - Includes advanced/underlying stats: xG, xA, ICT index, DefCon (defensive contribution)
   - Note: xG/xA/ICT data is reliably available from the 2021/22 season onward (sourced from Understat integration) — full coverage aligns with the requested 2–3 season depth.

## Core Design Principles

- **No ML / no prediction model.** Everything is descriptive or diagnostic analytics — what happened, what's trending — never a forecast of future points.
- **Position-normalized scoring.** All rankings and composite scores are computed WITHIN position groups (GKP / DEF / MID / FWD) separately, never pooled across positions. Scoring mechanics differ fundamentally by position (e.g., clean sheets and saves drive GKP/DEF scoring; goals and shots drive FWD scoring), so cross-position comparisons of raw scores or "value" are misleading unless normalized this way.
- **Two-layer analysis style:**
  - **Composite/formula-based scores** (non-ML, user-defined weights) — a fast filtering layer, e.g. a "value score" to quickly shortlist players.
  - **Raw historical trend views** — a drill-down layer once a composite score flags someone interesting (charts/tables, not just a single number).

## Functional Requirements

### 1. Player Value & Form Analysis

Rank players (within their position group) by a blended score of recent form, upcoming fixture ease, and reliability (minutes played).
_Example: "Show me midfielders under £8m with the best value score for the next 3 gameweeks" returns a sorted shortlist instead of manual comparison._

### 2. Historical Trend Drill-Down

For any player, chart points/goals/assists across the last 2–3 seasons, split by home vs. away and by opponent strength.
_Example: Player X averages 8.2 points at home vs. 5.1 away over two seasons — informs whether to captain him for an away fixture._

### 3. Opponent/Matchup Analysis

Show how a player or position group has historically performed against a specific upcoming opponent, or against categories of opponents (promoted teams, top-6, etc.).
_Example: "How have Liverpool forwards performed against newly-promoted teams the last 2 seasons?" — informs whether an upcoming fixture is actually a good pick._

### 4. Team-Level Attacking/Defensive Trends

Track which teams are conceding or scoring more than usual recently, independent of any one player.
_Example: Team Y have kept 4 clean sheets in their last 6 games — flags their defenders as good picks regardless of which individual is chosen._

### 5. Fixture Difficulty Planner

Show upcoming fixture difficulty per team over the next N gameweeks, to plan transfers ahead of a good or bad run.
_Example: Team Z have 4 "easy" fixtures in a row starting GW6 — a good window to bring in an attacker from that team before prices rise._

### 6. Differential Finder

Surface low-ownership players with strong underlying data, to help gain rank against mini-league rivals rather than matching the crowd.
_Example: A £6.5m midfielder owned by only 4% of managers is outperforming a heavily-owned £9m player on underlying stats._

### 7. Price Momentum Tracker

Show which players are trending toward a price rise or fall based on transfer activity, to time transfers before a price change.
_Example: A player is about to rise in price tonight based on net transfers in — buy today, not tomorrow._

### 8. Underlying Stats Layer (xG, xA, ICT, DefCon)

Go beyond raw goals/assists to show quality of chances created/conceded, to spot players who are "due" a return or defenders quietly accumulating defensive points.
_Example: A striker hasn't scored in 3 games but his xG suggests he should have 2 goals — his underlying form is better than raw points suggest, worth holding rather than selling._

### 9. Personal Squad Tracker

Automatically pull the user's own team's weekly points, rank, and squad via his FPL Team ID — no manual entry.
_Example: A chart of rank over time, cross-referenced against which transfer decisions coincided with rank jumps or drops._

### 10. Manual Refresh Control

Data only updates when the user triggers it — no background scheduled job.
_Example: User opens the dashboard before making a transfer decision, hits refresh, and gets that day's latest prices/ownership/points._

### 11. Position-Normalized Rankings

All value scores and rankings are computed separately per position group (GKP/DEF/MID/FWD) — never as one pooled leaderboard across positions.
_Example: Instead of one universal ranking, the dashboard produces four separate leaderboards — Best GKPs, Best DEFs, Best MIDs, Best FWDs — each using position-appropriate scoring weights (e.g., defenders weighted on clean sheets + DefCon; forwards weighted on goals + shots-in-box)._

### 12. Captaincy Fit Panel

A dedicated weekly shortlist of the user's own squad, ranked specifically for the captaincy decision — separate from general value rankings. Blends:

- **Ceiling/explosiveness** — propensity for big scoring weeks, not just average output
- **This week's fixture strength** — specific to the upcoming opponent, not general season form
- **Home/away split** — some players perform notably better at home
- **Historical performance vs. this exact opponent**
- **Minutes reliability** — avoids captaining a rotation/substitution risk

_Example: Two forwards have similar season averages, but one has a track record of explosive hauls against weak defenses. If this week's fixture is against a weak defense, that player is flagged as the higher-ceiling captain pick even though season averages look similar._

## Explicitly Out of Scope

- No predictive/points-forecasting model of any kind
- Not shared with the user's mini-league group — this is a solo project (contrast with the Valorant pipeline, which is for his 5-person team)
- No scheduled/automated data refresh — manual trigger only

## Open / Next Steps (not yet finalized)

- Bronze/Silver/Gold table schema design
- Ingestion approach for reconciling the two data sources (live FPL API vs. historical GitHub archive), which do not share a common ID scheme
- Exact formula/weights for composite scores (value score, captaincy score) — to be defined per position group
