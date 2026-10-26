#!/usr/bin/env bash
# Droplet cron entry for the current-season WNBA crosswalks (stages 11-13) -- their only
# automatic producer since 2026-10-11. stats.wnba.com times out from GitHub-hosted runners,
# so daily_wnba.yml skips the crosswalks (SDV_SKIP_CROSSWALKS=1) and this builds them
# OFFLINE (--from-archive): the committed ESPN schedule + game-roster parquet, joined to the
# droplet's wehoop-wnba-stats-raw box-score archive (its daily_refresh cron runs at 09:00).
# The archive path adds the Fox team/player ids live (current season), plus the Fox / Yahoo
# player + game ids from wehoop-wnba-alt-data when the season is parsed there. Committed past
# seasons get those ids through scripts/cron_alt_fill.sh (--fill-alt, offline).
#
#   bash scripts/cron_crosswalks.sh [-s YYYY] [-n]    # -n = dry run into a temp tree, no commit
#
# Exit 0 without building when the season's ESPN game rosters do not exist yet (preseason).
set -uo pipefail
cd "$(dirname "$0")/.."
echo "=== cron_crosswalks $(date -u +%FT%TZ)"
LG=wnba; PREFIX=WNBA
YR=$(date +%Y); DRY=0   # WNBA season = calendar year
while getopts s:n flag; do
  case "$flag" in s) YR=$OPTARG;; n) DRY=1;; *) echo "usage: $0 [-s YYYY] [-n]" >&2; exit 2;; esac
done
fail() { echo "::error ::$*"; echo "EXIT=1"; exit 1; }

# The git_pull sweep runs `pull --rebase --autostash` over every repo; holding its lock makes
# it skip a round instead of stashing this build's outputs. git children get 9>&- so a
# daemonized gc / credential cache cannot keep the lock after this exits.
exec 9>/tmp/git_pull_sdv.lock
flock -w 3600 9 || fail "git_pull sweep held its lock for 1h"
branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$branch" != main ] || [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  git status --short --untracked-files=no | head -20
  fail "checkout is on '$branch' or has tracked changes; refusing to build"
fi
git pull -q --ff-only 9>&- || fail "git pull --ff-only failed"

[ -f "$LG/game_rosters/parquet/game_rosters_$YR.parquet" ] \
  || { echo "no ESPN game rosters for $YR yet; nothing to build"; echo "EXIT=0"; exit 0; }

base="../$LG"; mode=--publish
if [ "$DRY" = 1 ]; then base=$(mktemp -d); mode=--dry-run; fi
export PYTHONUNBUFFERED=1 PYTHONIOENCODING=utf-8
for ds in team_crosswalk schedule_crosswalk player_crosswalk; do
  (cd python && uv run --quiet python -m ${LG}_data_build --dataset "$ds" -s "$YR" -e "$YR" \
     --from-archive --archive-root "../$LG" --base "$base" "$mode") 9>&- \
    || fail "$ds $YR failed"
done
[ "$DRY" = 1 ] && { echo "dry run: wrote $base/crosswalk, no commit"; echo "EXIT=0"; exit 0; }

# .rds is gitignored and unmatched/ stays local: commit the parquet + manifests only.
git add -- "$LG/crosswalk/parquet" "$LG"/crosswalk/*_in_data_repo.csv
git diff --cached --quiet && { echo "crosswalks unchanged"; echo "EXIT=0"; exit 0; }
git commit -q -m "$PREFIX Data Update (Start: $YR End: $YR)" 9>&- || fail "commit failed"
for attempt in 1 2 3; do   # daily_wnba.yml may push meanwhile; rebase --merge (parquet blobs)
  git push -q origin HEAD 9>&- && { echo "pushed $PREFIX $YR crosswalks"; echo "EXIT=0"; exit 0; }
  git fetch -q origin main 9>&- && git rebase -q --merge origin/main 9>&- \
    || { git rebase --abort 2>/dev/null; fail "cannot rebase onto origin/main"; }
done
fail "push rejected 3 times"
