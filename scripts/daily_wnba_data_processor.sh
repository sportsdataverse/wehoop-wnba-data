#!/bin/bash
# Compile wehoop-wnba-data datasets, per season (Python-first cutover).
#
# The 11 daily raw-derived datasets are built by `wnba_data_build` (parity-
# validated port of espn_wnba_01..10). Build order matters: shots project the
# built pbp parquet; schedules stamp flags from the built pbp/team_box/
# player_box parquets (and rebuild the wnba_schedule_master +
# wnba_games_in_data_repo extras). Draft (08) is annual cadence and runs from
# annual_wnba_draft.yml, not here. Crosswalks (wnba_11-13) stay on R (live
# ESPN+Torvik+Fox inputs), and R also serializes every Python parquet to .rds
# (wehoop's load_* reads rds) via R/serialize_rds.R.
#
# Usage: bash scripts/daily_wnba_data_processor.sh -s 2025 -e 2025
set -uo pipefail

while getopts s:e: flag; do
  case "${flag}" in
    s) START_YEAR=${OPTARG};;
    e) END_YEAR=${OPTARG};;
  esac
done
END_YEAR=${END_YEAR:-$START_YEAR}

# The raw repo can't be checked out in CI -- read it over HTTP like the R
# pipeline did (per-run cache under .wnba_raw_cache/, gitignored).
export WEHOOP_WNBA_RAW_ROOT="${WEHOOP_WNBA_RAW_ROOT:-https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main}"

# Scrape-log conventions: unbuffered + utf-8 so wnba_data_build's timestamped
# log lines land in the Actions console AND the tee'd season logfile live.
export PYTHONUNBUFFERED=1
export PYTHONIOENCODING=utf-8

# Dependency order: pbp/team_box/player_box first (schedules reads their
# game-id sets; shots read the pbp parquet), then the rest.
PY_DATASETS="pbp team_box player_box player_core schedules shots rosters player_season_stats team_season_stats standings game_rosters officials"
R_CROSSWALKS=(R/wnba_11_team_crosswalk_creation.R R/wnba_12_schedule_crosswalk_creation.R R/wnba_13_player_crosswalk_creation.R)

mkdir -p logs
ANY_FAILED=0
for i in $(seq "${START_YEAR}" "${END_YEAR}"); do
  LOGFILE="logs/wehoop_wnba_data_logfile_${i}.log"
  TMPLOG=$(mktemp "/tmp/wehoop_wnba_data_${i}.XXXXXX.log")
  # Tee inside the block writes to /tmp (untracked) so the `git pull` calls
  # don't trip over their own log output being written to a tracked file.
  {
    git pull >/dev/null
    git config --local user.email "action@github.com"
    git config --local user.name "Github Action"
    SEASON_RC=0

    # ::group:: markers collapse each dataset in the Actions UI; in the tee'd
    # season logfile they read as plain section headers.
    run_py() {
      local ds="$1"
      echo "::group::wnba_data_build $ds $i"
      (cd python && uv run python -m wnba_data_build --dataset "$ds" --base ../wnba -s "$i" -e "$i" --publish) || {
        rc=$?; echo "::warning ::wnba_data_build $ds for season $i exited with code $rc"; SEASON_RC=$rc
      }
      echo "::endgroup::"
    }
    for ds in $PY_DATASETS; do run_py "$ds"; done

    for SCRIPT in "${R_CROSSWALKS[@]}"; do
      echo "::group::$SCRIPT $i"
      # Crosswalks build from LIVE ESPN+Torvik+Fox sources and are known-fragile
      # (segfault/timeout on external flakiness). Best-effort: warn only, do NOT
      # flip SEASON_RC -- the 11 core python datasets are the daily deliverable
      # and must not be reported as failed because of a live external source.
      Rscript "$SCRIPT" -s "$i" -e "$i" || echo "::warning ::$SCRIPT for season $i exited with code $? (crosswalk; non-fatal, live external source)"
      echo "::endgroup::"
    done

    echo "::group::serialize_rds $i"
    Rscript R/serialize_rds.R -s "$i" -e "$i" || {
      rc=$?; echo "::warning ::serialize_rds for season $i exited with code $rc"; SEASON_RC=$rc
    }
    echo "::endgroup::"

    echo "RSCRIPT_RC=$SEASON_RC" > "/tmp/_rc_${i}"
    # Grep-able terminal line for the season logfile (scrape-log convention).
    echo "season $i EXIT=$SEASON_RC"
    # Commit whatever datasets succeeded even if one step errored -- the
    # per-dataset error handling keeps partial output usable.
    git pull >/dev/null
    git add wnba/* >/dev/null 2>&1 || true
    # Load-bearing subject: downstream tooling parses the years out of it.
    git commit -m "WNBA Data Update (Start: $i End: $i)" || echo "No changes to commit"
    git pull >/dev/null
    git push >/dev/null
  } 2>&1 | tee "$TMPLOG"

  RSCRIPT_RC=$(sed 's/RSCRIPT_RC=//' "/tmp/_rc_${i}" 2>/dev/null); rm -f "/tmp/_rc_${i}"
  cp "$TMPLOG" "$LOGFILE"
  git pull --rebase >/dev/null || true
  git add "$LOGFILE"
  git commit -m "WNBA Data log update (Start: $i End: $i)" >/dev/null || echo "No log changes to commit"
  git push >/dev/null
  rm -f "$TMPLOG"
  if [ "${RSCRIPT_RC:-0}" != "0" ]; then
    echo "::error ::At least one creation step for season $i exited with code $RSCRIPT_RC"
    ANY_FAILED=1
  fi
done

# ---- Run summary: updated releases + remaining warnings/errors ----
Rscript R/run_summary.R -s "$START_YEAR" -e "$END_YEAR" || true
[ "${ANY_FAILED:-0}" = "0" ] || exit 1
