# Databricks notebook source
# COMMAND ----------
# run_full_pipeline.py
# Master End-to-End Orchestrator for FPL Decision-Support Medallion Pipeline.
#
# Execution order:
#   1. 00_init_schemas          — sequential (must be first)
#   2. 01–06 Bronze notebooks   — parallel   (all independent, I/O bound)
#   3. 07_player_gw_history     — sequential (depends on 01 + 03 being done)
#   4. Silver notebooks         — sequential (depend on all Bronze)
#   5. Gold notebooks           — sequential (depend on all Silver)
#
# Uses dbutils.notebook.run() + ThreadPoolExecutor for parallel Bronze execution.
# A single Databricks Job task pointing at this notebook is all you need.

from concurrent.futures import ThreadPoolExecutor, as_completed
import time

NOTEBOOK_TIMEOUT = 3600  # 1 hour per notebook max

def run_nb(path: str, timeout: int = NOTEBOOK_TIMEOUT) -> str:
    """Run a notebook and return its exit value or 'success'."""
    result = dbutils.notebook.run(path, timeout)
    return result or "success"

print("=" * 60)
print("FPL Medallion Pipeline — Manual Refresh Started")
print("=" * 60)

# COMMAND ----------
# ── STAGE 0: Init schemas (must run before everything else) ──────────────────
print("\n[Stage 0] Initialising schemas...")
run_nb("./bronze/00_init_schemas")
print("  ✅ Schemas ready")

# COMMAND ----------
# ── STAGE 1: Bronze 01–06 in parallel ────────────────────────────────────────
# All six notebooks are independent — they each call different API endpoints
# or download from GitHub. Running them in parallel via threads cuts this
# stage from ~10 min sequential to ~5 min (bottleneck = github archive).

PARALLEL_BRONZE = [
    "./bronze/01_ingest_players_raw",
    "./bronze/02_ingest_teams_raw",
    "./bronze/03_ingest_events_raw",
    "./bronze/04_ingest_fixtures_raw",
    "./bronze/05_ingest_my_team_raw",
    "./bronze/06_ingest_github_archive",
]

print(f"\n[Stage 1] Running {len(PARALLEL_BRONZE)} Bronze notebooks in parallel...")
stage1_start = time.time()

failed = []

with ThreadPoolExecutor(max_workers=len(PARALLEL_BRONZE)) as executor:
    future_to_nb = {executor.submit(run_nb, nb): nb for nb in PARALLEL_BRONZE}

    for future in as_completed(future_to_nb):
        nb = future_to_nb[future]
        nb_name = nb.split("/")[-1]
        try:
            result = future.result()
            print(f"  ✅ {nb_name} — {result}")
        except Exception as e:
            print(f"  ❌ {nb_name} — FAILED: {e}")
            failed.append(nb_name)

stage1_elapsed = round(time.time() - stage1_start, 1)
print(f"\n  Stage 1 complete in {stage1_elapsed}s")

if failed:
    raise Exception(f"Stage 1 failed notebooks: {failed}. Halting pipeline.")

# COMMAND ----------
# ── STAGE 2: Player GW history & economics snapshot — sequential after Stage 1 ────
# 07 depends on events_raw (from 03) to fetch match stats for new GWs.
# 08 depends on players_raw (from 01) and MUST run in the same refresh to snapshot
# price & ownership before the next refresh overwrites players_raw.
# Both exit cleanly if no new GWs are finalised.

print("\n[Stage 2] Running incremental player GW history & economics snapshot...")
res_07 = run_nb("./bronze/07_ingest_player_gw_history")
print(f"  ✅ 07_ingest_player_gw_history — {res_07}")

res_08 = run_nb("./bronze/08_snapshot_player_economics")
print(f"  ✅ 08_snapshot_player_economics — {res_08}")

print("\n✅ Bronze Layer complete")

# COMMAND ----------
# ── STAGE 3: Silver — sequential ─────────────────────────────────────────────
# Silver notebooks have internal dependencies (crosswalk must run before
# player/fixture silver tables). run_all_silver handles that order internally.

print("\n[Stage 3] Building Silver tables & crosswalk reconciliation...")
run_nb("./silver/run_all_silver")
print("  ✅ Silver Layer complete")

# COMMAND ----------
# ── STAGE 4: Gold — sequential ───────────────────────────────────────────────
# Gold tables depend on Silver being fully built.

print("\n[Stage 4] Building Gold analytics tables...")
run_nb("./gold/run_all_gold")
print("  ✅ Gold Layer complete")

# COMMAND ----------
print("\n" + "=" * 60)
print("FPL Pipeline — ALL STAGES COMPLETE")
print("fpl.bronze.*, fpl.silver.*, fpl.gold.* are fully refreshed.")
print("=" * 60)
