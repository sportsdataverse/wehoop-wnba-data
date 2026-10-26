#!/usr/bin/env bash
# Droplet cron entry: fill the WNBA schedule + player crosswalks' Fox / Yahoo legs from the parsed
# wehoop-wnba-alt-data history (`python -m wnba_data_build --fill-alt`: offline, fills only null
# provider ids, refuses to drop data). A crosswalk is published, committed and pushed only when
# its parquet actually changed; the fill runs without --publish first and re-runs with it on a diff.
#
#   bash scripts/cron_alt_fill.sh -s YYYY [-e YYYY] [-n]   # -n = dry run into a temp tree,
#                                                          #      no commit, no publish
# Env: WEHOOP_WNBA_ALT_DATA_ROOT (default: the sibling wehoop-wnba-alt-data checkout).
#
# A season with no parsed Fox / Yahoo season or no committed crosswalk is skipped (exit 0).
set -uo pipefail
cd "$(dirname "$0")/.."
echo "=== cron_alt_fill $(date -u +%FT%TZ)"
LG=wnba; PREFIX=WNBA
ALT=${WEHOOP_WNBA_ALT_DATA_ROOT:-$(cd .. && pwd)/wehoop-wnba-alt-data}
START=""; END=""; DRY=0
usage() { echo "usage: $0 -s YYYY [-e YYYY] [-n]" >&2; exit 2; }
while getopts s:e:n flag; do
  case "$flag" in s) START=$OPTARG;; e) END=$OPTARG;; n) DRY=1;; *) usage;; esac
done
END=${END:-$START}
[[ "$START" =~ ^[0-9]{4}$ && "$END" =~ ^[0-9]{4}$ ]] || usage
fail() { echo "::error ::$*"; echo "EXIT=1"; exit 1; }

# The real run holds the git_pull sweep's lock (it would autostash this run's outputs); git / uv
# children get 9>&- so a daemonized gc / credential cache cannot keep the lock after this exits.
# -n touches only a temp tree, so it skips the lock (cron_daily_cfb.sh can hold it for hours),
# the dirty-tree check and the pull.
if [ "$DRY" = 0 ]; then
  exec 9>/tmp/git_pull_sdv.lock
  flock -w 3600 9 || fail "git_pull sweep held its lock for 1h"
  branch=$(git rev-parse --abbrev-ref HEAD)
  if [ "$branch" != main ] || [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    git status --short --untracked-files=no | head -20
    fail "checkout is on '$branch' or has tracked changes; refusing to fill"
  fi
  git pull -q --ff-only 9>&- || fail "git pull --ff-only failed"
fi

export PYTHONUNBUFFERED=1 PYTHONIOENCODING=utf-8 WEHOOP_WNBA_ALT_DATA_ROOT="$ALT"
fill() {   # dataset season base [--publish|--dry-run]
  (cd python && uv run --quiet python -m ${LG}_data_build --dataset "$1" -s "$2" -e "$2" \
     --fill-alt --archive-root "../$LG" --base "$3" ${4:+"$4"}) 9>&-
}

rc=0
for yr in $(seq "$START" "$END"); do
  parsed=0
  for p in fox yahoo; do
    [ -f "$ALT/$p/game_rosters/parquet/${LG}_${p}_game_rosters_$yr.parquet" ] && parsed=1
  done
  [ "$parsed" = 1 ] || { echo "$yr: no parsed Fox / Yahoo season under $ALT; skipped"; continue; }
  changed=()
  for ds in schedule_crosswalk player_crosswalk; do
    pq="$LG/crosswalk/parquet/${LG}_${ds}_$yr.parquet"
    manifest="$LG/crosswalk/${LG}_${ds}_in_data_repo.csv"
    [ -f "$pq" ] || { echo "$yr: no committed $ds; skipped"; continue; }
    if [ "$DRY" = 1 ]; then
      tmp=$(mktemp -d); mkdir -p "$tmp/crosswalk/parquet"; cp "$pq" "$tmp/crosswalk/parquet/"
      fill "$ds" "$yr" "$tmp" --dry-run || { echo "::error ::$ds $yr fill failed"; rc=1; rm -rf "$tmp"; continue; }
      cmp -s "$pq" "$tmp/${pq#"$LG"/}" && echo "$yr $ds: unchanged" \
        || echo "$yr $ds: WOULD CHANGE (publish + commit)"
      rm -rf "$tmp"; continue
    fi
    if ! fill "$ds" "$yr" "../$LG"; then
      git checkout -q -- "$pq" "$manifest" 9>&-
      echo "::error ::$ds $yr fill failed"; rc=1; continue
    fi
    if git diff --quiet -- "$pq"; then
      git checkout -q -- "$manifest" 9>&-   # the fill restamps the manifest even when unchanged
      echo "$yr $ds: unchanged"; continue
    fi
    if ! fill "$ds" "$yr" "../$LG" --publish; then
      git checkout -q -- "$pq" "$manifest" 9>&-
      echo "::error ::$ds $yr publish failed"; rc=1; continue
    fi
    changed+=("$pq" "$manifest")
  done
  [ "${#changed[@]}" = 0 ] && continue
  git add -- "${changed[@]}" 9>&-
  git commit -q -m "$PREFIX Data Updated (Start: $yr End: $yr)" 9>&- || fail "commit $yr failed"
  pushed=0
  for attempt in 1 2 3; do   # daily_wnba.yml may push meanwhile; rebase --merge (parquet blobs)
    git push -q origin HEAD 9>&- && { pushed=1; echo "pushed $PREFIX $yr alt fill"; break; }
    git fetch -q origin main 9>&- && git rebase -q --merge origin/main 9>&- \
      || { git rebase --abort 2>/dev/null; fail "cannot rebase onto origin/main"; }
  done
  [ "$pushed" = 1 ] || fail "push rejected 3 times"
done
[ "$DRY" = 1 ] && echo "dry run: no commit, no publish"
echo "EXIT=$rc"
exit "$rc"
