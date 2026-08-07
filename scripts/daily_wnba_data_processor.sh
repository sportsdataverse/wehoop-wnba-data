#!/bin/bash
# Compile wehoop-wnba-data datasets, per season (Python-first cutover).
#
# The 11 daily raw-derived datasets are built by `wnba_data_build` (parity-
# validated port of espn_wnba_01..10). Build order matters: shots project the
# built pbp parquet; schedules stamp flags from the built pbp/team_box/
# player_box parquets (and rebuild the wnba_schedule_master +
# wnba_games_in_data_repo extras). Draft (08) is annual cadence and runs from
# annual_wnba_draft.yml, not here. Crosswalks (wnba_11-13) stay on R (live
# ESPN+Torvik+Fox inputs); the .rds is written natively by io.write_dataset
# in the same pass as the parquet (R/serialize_rds.R was retired in 120deafe).
#
# `-l R` is the rollback path over espn_wnba_01..10 (design D20/D21); it used
# to live in a separate daily_wnba_R_processor.sh, now a deprecation shim.
#
# Usage: bash scripts/daily_wnba_data_processor.sh -s 2025 -e 2025 [-l python|R]
set -uo pipefail

# -l selects the language for the raw-derived datasets. Python is the
# production default; `-l R` is the rollback path that used to live in a
# separate daily_wnba_R_processor.sh. One script, so the two paths cannot drift
# in season handling, logging or the load-bearing commit format.
while getopts s:e:l: flag; do
  case "${flag}" in
    s) START_YEAR=${OPTARG};;
    e) END_YEAR=${OPTARG};;
    l) LANG_MODE=${OPTARG};;
    *) echo "usage: $0 -s <start> [-e <end>] [-l python|R]" >&2; exit 2;;
  esac
done
START_YEAR=${START_YEAR:-}
END_YEAR=${END_YEAR:-$START_YEAR}
LANG_MODE=${LANG_MODE:-python}
if [ -z "$START_YEAR" ]; then
  echo "usage: $0 -s <start_year> [-e <end_year>] [-l python|R]" >&2
  exit 1
fi
case "$LANG_MODE" in
  python|R) ;;
  *) echo "::error ::unknown -l '$LANG_MODE' (expected python or R)" >&2; exit 2;;
esac

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
# The `-l R` rollback path. R has no counterpart for player_core, schedules or
# shots (espn_wnba_01 writes the schedules + shots subsets inline) -- hence the
# gaps, which are deliberate. Draft (08) is annual cadence and intentionally
# excluded in both modes: it runs from annual_wnba_draft.yml.
R_DATASETS=(
  R/espn_wnba_01_pbp_creation.R
  R/espn_wnba_02_team_box_creation.R
  R/espn_wnba_03_player_box_creation.R
  R/espn_wnba_04_rosters_creation.R
  R/espn_wnba_05_player_season_stats_creation.R
  R/espn_wnba_06_team_season_stats_creation.R
  R/espn_wnba_07_standings_creation.R
  R/espn_wnba_09_game_rosters_creation.R
  R/espn_wnba_10_officials_creation.R
)

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
    run_r() {
      local script="$1"
      echo "::group::$script $i"
      Rscript "$script" -s "$i" -e "$i" || {
        rc=$?; echo "::warning ::$script for season $i exited with code $rc"; SEASON_RC=$rc
      }
      echo "::endgroup::"
    }

    if [ "$LANG_MODE" = "R" ]; then
      for SCRIPT in "${R_DATASETS[@]}"; do run_r "$SCRIPT"; done
    else
      for ds in $PY_DATASETS; do run_py "$ds"; done
    fi

    for SCRIPT in "${R_CROSSWALKS[@]}"; do
      echo "::group::$SCRIPT $i"
      # Crosswalks build from LIVE ESPN+Torvik+Fox sources and are known-fragile
      # (segfault/timeout on external flakiness). Best-effort: warn only, do NOT
      # flip SEASON_RC -- the 11 core python datasets are the daily deliverable
      # and must not be reported as failed because of a live external source.
      Rscript "$SCRIPT" -s "$i" -e "$i" || echo "::warning ::$SCRIPT for season $i exited with code $? (crosswalk; non-fatal, live external source)"
      echo "::endgroup::"
    done


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
# Prints a cli summary to the Action log and (when set) writes markdown to
# $GITHUB_STEP_SUMMARY so the run's Summary tab shows what landed and what didn't.
if [ "$LANG_MODE" = "R" ]; then
  Rscript R/run_summary.R -s "$START_YEAR" -e "$END_YEAR" || true
else
  ( cd python && uv run python -m wnba_data_build.summary --logs ../logs -s "$START_YEAR" -e "$END_YEAR" ) || true
fi
[ "${ANY_FAILED:-0}" = "0" ] || exit 1
