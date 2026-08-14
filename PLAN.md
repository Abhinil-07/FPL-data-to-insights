# FPL Decision-Support Dashboard — Implementation Plan

This plan is written to be handed to Claude Code as a build spec. It sequences work into phases, each with concrete deliverables, file/notebook structure, and acceptance checks. Design principles (no ML, position-normalized, manual refresh) are carried through every phase — don't relitigate them per-phase.

---

## Phase 0 — Project Scaffolding

**Goal:** Repo structure and Databricks workspace ready before any ingestion code is written.

**Tasks:**
- Create repo folder structure:
  ```
  fpl-dashboard/
    notebooks/
      bronze/
      silver/
      gold/
    src/
      fpl_api/          # API client functions
      github_archive/   # archive download/parse functions
      transforms/        # shared Silver/Gold transform logic
    schemas/              # Delta table schema definitions (as code, not docs)
    config/
      config.yaml         # FPL Team ID, season list, table paths
    tests/
    README.md
  ```
- Set up a Databricks Free Edition workspace, confirm Delta Lake is available by default.
- Store the user's **FPL Team ID** and target season(s) in `config/config.yaml` — not hardcoded in notebooks.
- Decide catalog/schema naming convention up front, e.g. `fpl.bronze`, `fpl.silver`, `fpl.gold` (Unity Catalog three-level namespace if available in Free Edition; otherwise database-per-layer).

**Acceptance check:** Empty Delta tables can be created and queried in each of the three schemas.

---

## Phase 1 — Bronze: Live FPL API Ingestion

Build in this order — each step is additive and independently testable.

### 1.1 `bootstrap-static` ingestion
- Single GET to `https://fantasy.premierleague.com/api/bootstrap-static/`
- Land raw JSON response into three Bronze Delta tables (don't flatten yet, just split top-level keys):
  - `bronze.players_raw` (from `elements`)
  - `bronze.teams_raw` (from `teams`)
  - `bronze.events_raw` (from `events` — gameweek metadata, deadlines, current GW flag)
- Each row gets an `_ingested_at` timestamp column. This is your only "freshness" signal since refresh is manual.
- **Write mode:** overwrite each run (bootstrap-static is a current-state snapshot, not historical — see note in Phase 4 about snapshotting for trend purposes).

### 1.2 Team ID endpoint (your squad)
- GET `https://fantasy.premierleague.com/api/entry/{TEAM_ID}/`
- GET `https://fantasy.premierleague.com/api/entry/{TEAM_ID}/history/`
- Land into `bronze.my_team_raw` and `bronze.my_team_history_raw`
- Same `_ingested_at` pattern.

### 1.3 `fixtures` ingestion
- GET `https://fantasy.premierleague.com/api/fixtures/`
- Land into `bronze.fixtures_raw` — includes FDR per fixture per team.
- Overwrite each run (fixtures update as results come in and FDR can be revised).

### 1.4 `element-summary/{player_id}` ingestion
- This is per-player, so it's a loop — 600+ requests. Build with:
  - Rate limiting / small delay between calls (be a polite API citizen — no documented rate limit, but don't hammer it)
  - Retry-on-failure logic, since this is the most likely step to partially fail
  - Land into `bronze.player_gw_history_raw` (current season gameweek-by-gameweek) and `bronze.player_fixtures_raw` (their upcoming fixtures)
- This step is the slowest and most fragile — build it last within Phase 1 and test on a small player subset (e.g. 10 IDs) before running full.

**Acceptance check:** All five Bronze tables populated from a single manual run; row counts sanity-checked against known values (e.g. ~700 players, 20 teams, 38 events).

---

## Phase 2 — Bronze: Historical Archive Ingestion

**Goal:** Land 2–3 seasons of `vaastav/Fantasy-Premier-League` data, unmodified, into Bronze.

- Pull via GitHub raw content URLs (season folders, e.g. `data/2023-24/gws/merged_gw.csv`, `data/2023-24/players_raw.csv`, `data/2023-24/teams.csv`)
- One Bronze table per file-type-per-season, or a unioned table with a `season` column added at ingestion — recommend the latter for anything gameweek-level, since schema is consistent across seasons:
  - `bronze.archive_player_gws` (unioned across seasons, `season` column added)
  - `bronze.archive_players_raw` (per-season player list snapshot)
  - `bronze.archive_teams_raw`
- Keep raw column names as-is at this stage — no renaming, no ID matching yet. Bronze = "what the source actually said."

**Acceptance check:** Row counts match expected gameweeks × players per season; spot-check a known player-season (e.g. Salah 2023/24 total points) against the public FPL site.

---

## Phase 3 — Silver: Cleaning, Typing, and the Player Crosswalk

This is the most important phase — it's where the ID reconciliation problem actually gets solved.

### 3.1 Standard cleaning
For each Bronze table → Silver equivalent:
- Explicit schema (typed columns, not inferred)
- Drop `_raw` suffix, rename to clear business names
- Standard column set: consistent `player_id`, `team_id`, `position`, `season`, `gameweek` naming across every Silver table

### 3.2 Player crosswalk table — `silver.player_crosswalk`
This is the deliverable that solves the ID reconciliation problem described earlier. Build it as its own notebook, run once per refresh, before any other Silver table that needs cross-season player identity.

**Approach:**
1. Generate a durable `player_key` — prefer FPL's `code` field where present and stable; verify stability by re-pulling bootstrap-static a day apart during development and confirming `code` doesn't change for the same person.
2. Build one row per season per source, with columns: `player_key`, `season`, `source` (`live_api` / `archive`), `source_id`, `first_name`, `second_name`, `team_id`, `position`.
3. Match `archive` rows to a `player_key` via:
   - Primary: exact match on normalized name (`lower`, strip accents/diacritics, strip whitespace) + team + season
   - Fallback: fuzzy name match (e.g. Levenshtein distance below a threshold) + position match, for cases where team differs due to mid-season transfer
   - Unmatched rows get flagged (`crosswalk_status = 'unmatched'`) rather than silently dropped — surface these for manual review, don't guess silently
4. Maintain a small manual-override CSV (`config/player_overrides.csv`) for known ambiguous cases (duplicate names, mid-season transfers, name spelling changes) that gets left-joined in and takes precedence over the automated match.

**Acceptance check:** Every archive player-season row resolves to exactly one `player_key`, or is explicitly flagged unmatched with a reason. Spot check ~15 players by hand, including at least one known tricky case (common surname, mid-season transfer).

### 3.3 Remaining Silver tables (built using the crosswalk)
- `silver.players` — current player dimension, `player_key` as the joinable ID, current price/position/team/ownership
- `silver.player_gw_history` — unioned current + archive gameweek history, keyed on `player_key` + `season` + `gameweek`
- `silver.teams` — team dimension, also needs a lightweight crosswalk (team IDs are also reassigned yearly) — same pattern as player crosswalk, simpler since only ~20 teams and name matching is far less ambiguous
- `silver.fixtures` — cleaned fixture list with FDR, joined to stable `team_key`
- `silver.my_team` — cleaned personal squad/points/rank history

**Acceptance check:** A single query can chart one player's points across 3 seasons using only `player_key` — no manual ID lookups needed.

---

## Phase 4 — Gold: Aggregates and Composite Scores

Build one Gold table per functional requirement. Each should be a documented, versioned SQL transform (not ad hoc) so composite score weights can be tuned without rewriting logic.

| Gold table | Functional requirement(s) | Notes |
|---|---|---|
| `gold.value_scores` | #1, #11 | Computed **separately per position group** — four independent scoring passes, not one pooled query with a position filter. Inputs: recent form, upcoming fixture ease (avg FDR next N GWs), minutes reliability. |
| `gold.player_trends` | #2 | Pre-aggregated home/away and vs-opponent-strength splits per player per season, feeding drill-down charts. |
| `gold.matchup_history` | #3 | Player/position performance vs. opponent categories (promoted teams, top-6, etc.) — requires a `team_category` mapping table (can be manually maintained, small and slow-changing). |
| `gold.team_trends` | #4 | Rolling clean-sheet / goals-conceded / goals-scored windows (last 6 games) per team. |
| `gold.fixture_planner` | #5 | Upcoming N-gameweek FDR per team, pivoted wide (one column per GW) for easy scanning. |
| `gold.differentials` | #6 | Low-ownership + strong underlying-stats filter, position-normalized. |
| `gold.price_momentum` | #7 | Requires **daily-ish** bootstrap-static snapshots (see note below) — net transfers in/out trend, proximity to price-change threshold. |
| `gold.underlying_stats` | #8 | xG/xA/ICT/DefCon vs. actual points, "due a return" flag (actual < expected over trailing window). |
| `gold.my_squad_tracker` | #9 | Rank-over-time chart data, joined against a manually-logged transfer-decision date (simple append-only log table, `silver.my_transfers_log`, since FPL API doesn't cleanly expose "why" a transfer was made). |
| `gold.captaincy_fit` | #12 | Separate scoring pass from `value_scores` — blends ceiling/explosiveness (variance of past hauls, not just mean), this-week fixture strength, home/away split, historical performance vs. this exact opponent, minutes reliability. Only scores players in `silver.my_team` current squad. |

**Note on price momentum (#7):** `bootstrap-static` only gives a current snapshot of transfer counts, not a trend, unless you snapshot it over time. Since refresh is manual (not scheduled), price momentum will only be as good as how often the user manually refreshes. Flag this limitation in the dashboard UI rather than over-engineering a workaround — this is a known tradeoff of the "no scheduled jobs" design principle. Optionally: append (not overwrite) a lightweight snapshot table (`bronze.price_snapshot_log`, just `player_id`, `now_cost`, `transfers_in_event`, `transfers_out_event`, `_ingested_at`) every time bootstrap-static runs, so momentum has something to compute against once enough manual runs accumulate.

**Composite score formula weights:** Start with simple, transparent weighted sums (not black-box), e.g.:
```
value_score = (w1 * form_z) + (w2 * fixture_ease_z) + (w3 * minutes_reliability_z)
```
computed as z-scores within position group. Leave weights (`w1, w2, w3`) as named constants in a config file, not hardcoded magic numbers, so they're easy to tune per position without touching transform logic.

**Acceptance check:** Each Gold table can be queried standalone and produces the exact example output described in the requirements doc (e.g. "midfielders under £8m, best value score, next 3 GWs" returns a sensible sorted list).

---

## Phase 5 — Dashboard / Presentation Layer

- Build as a Databricks SQL Dashboard (start here — faster to iterate) with one page or section per functional requirement, mapping directly to the Gold tables above.
- Evaluate AI/BI Genie as a secondary/complementary interface once Gold tables are stable — lets you ask ad hoc questions ("how did Salah do away against top-6 teams last season") without pre-building every chart.
- Add a visible "last refreshed" timestamp (from the Bronze `_ingested_at` columns) on every dashboard page — critical given the manual-refresh model, so it's always clear how stale the data is.
- Manual refresh trigger: a single "Run all Bronze + Silver + Gold notebooks" job, triggered on demand (Databricks Workflow that is NOT scheduled — run-on-click only).

**Acceptance check:** Full pipeline (Bronze → Silver → Gold) runs end-to-end from one manual trigger, dashboard reflects fresh data afterward.

---

## Suggested Build Order Summary

1. Phase 0 — scaffolding
2. Phase 1.1–1.3 — bootstrap-static, team ID, fixtures (fast wins, proves the pattern)
3. Phase 2 — one archive season only, to validate the approach before pulling all 2–3
4. Phase 3.2 — player crosswalk (highest-risk, most important piece — do this before building more Bronze/Silver)
5. Phase 1.4 — element-summary loop (slow, do once crosswalk logic is proven so you know what you're matching against)
6. Phase 2 (remaining seasons) + Phase 3.1/3.3 — finish Silver layer
7. Phase 4 — Gold tables, roughly in the order listed in the table above (value scores and fixture planner first, since they're prerequisites for differentials and captaincy fit)
8. Phase 5 — dashboard wiring

## Things to explicitly flag to Claude Code as constraints, every session
- No ML/prediction — composite scores only, transparent weighted formulas
- Position-normalized — never a pooled cross-position ranking
- Manual refresh only — no scheduled Databricks Jobs/Workflows
- Medallion pattern — Bronze is raw/untransformed, Silver is cleaned/typed/matched, Gold is aggregated/business-logic