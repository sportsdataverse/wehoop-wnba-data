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
# Crosswalks (stages 11-13). Python is production; `-l R` still runs the R
# scripts unchanged (design D20 rollback).
PY_CROSSWALKS="team_crosswalk schedule_crosswalk player_crosswalk"
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

# Commit + push, surviving a remote that moved while the build was running.
#
# The previous form pulled BEFORE staging, which can only ever abort: the build
# has just rewritten 23 tracked parquet/csv files, so `git pull` refuses with
# "Your local changes would be overwritten by merge". It then committed anyway,
# pushed into a non-fast-forward rejection, and swallowed the whole thing in
# `>/dev/null` with no rc check -- so the job went green having published
# nothing to the repo. (Release assets upload separately and were fine.)
#
# Order matters: stage and commit FIRST so the tree is clean, and only then
# reconcile with origin. `rebase --merge` rather than `pull --rebase` because
# git's default am backend base64-encodes every parquet blob it replays.
sdv_commit_push() {
  local msg="$1"; shift
  git add -- "$@" >/dev/null 2>&1 || true
  if git diff --cached --quiet; then
    echo "nothing to commit for: $msg"
    return 0
  fi
  git commit -m "$msg" >/dev/null || { echo "::warning ::commit failed: $msg"; return 1; }

  local attempt
  for attempt in 1 2 3; do
    if git push origin HEAD >/dev/null 2>&1; then
      echo "pushed: $msg (attempt $attempt)"
      return 0
    fi
    echo "push rejected (attempt $attempt); syncing with origin"
    git fetch --quiet origin main || true
    if ! git rebase --merge origin/main >/dev/null 2>&1; then
      git rebase --abort >/dev/null 2>&1 || true
      echo "::error ::cannot rebase onto origin/main for: $msg"
      return 1
    fi
  done
  echo "::error ::push still rejected after 3 attempts: $msg"
  return 1
}

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

    # Crosswalks build from LIVE ESPN+WNBA Stats+Fox sources and are
    # known-fragile (timeout on external flakiness). Best-effort in BOTH modes:
    # warn only, do NOT flip SEASON_RC -- the 11 core datasets are the daily
    # deliverable and must not be reported as failed because of a live
    # external source.
    if [ "$LANG_MODE" = "R" ]; then
      for SCRIPT in "${R_CROSSWALKS[@]}"; do
        echo "::group::$SCRIPT $i"
        Rscript "$SCRIPT" -s "$i" -e "$i" || echo "::warning ::$SCRIPT for season $i exited with code $? (crosswalk; non-fatal, live external source)"
        echo "::endgroup::"
      done
    else
      for ds in $PY_CROSSWALKS; do
        echo "::group::wnba_data_build $ds $i"
        (cd python && uv run python -m wnba_data_build --dataset "$ds" --base ../wnba -s "$i" -e "$i" --publish) \
          || echo "::warning ::wnba_data_build $ds for season $i exited with code $? (crosswalk; non-fatal, live external source)"
        echo "::endgroup::"
      done
    fi


    echo "RSCRIPT_RC=$SEASON_RC" > "/tmp/_rc_${i}"
    # Grep-able terminal line for the season logfile (scrape-log convention).
    echo "season $i EXIT=$SEASON_RC"
    # Commit whatever datasets succeeded even if one step errored -- the
    # per-dataset error handling keeps partial output usable.
    # Load-bearing subject: downstream tooling parses the years out of it.
    sdv_commit_push "WNBA Data Update (Start: $i End: $i)" wnba || PUSH_RC=1
  } 2>&1 | tee "$TMPLOG"

  RSCRIPT_RC=$(sed 's/RSCRIPT_RC=//' "/tmp/_rc_${i}" 2>/dev/null); rm -f "/tmp/_rc_${i}"
  cp "$TMPLOG" "$LOGFILE"
  sdv_commit_push "WNBA Data log update (Start: $i End: $i)" "$LOGFILE" || PUSH_RC=1
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
# A rejected push is a FAILED run, not a green one. Release assets upload on a
# separate path and can succeed while the repo mirror is left stale -- which is
# exactly how a concurrent-run race produced a green job that published nothing
# to the repo (2026-08-18, runs 32192069433 + 32192069566).
if [ "${PUSH_RC:-0}" != "0" ]; then
  echo "::error ::At least one commit failed to reach origin; the repo mirror is stale."
  ANY_FAILED=1
fi
[ "${ANY_FAILED:-0}" = "0" ] || exit 1
