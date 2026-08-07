#!/usr/bin/env bash
# Weekly R <-> Python OUTPUT parity for one dataset/season.
#
# tests/test_r_python_parity.py is the CONTRACT gate: do both languages declare
# the same datasets under the same stage numbers. It never opens the data. This
# is the other half -- do the two pipelines produce the same VALUES -- and it is
# weekly rather than per-push because the R side is not cheap (see below).
#
# Why both sides are rebuilt into temp dirs rather than read from the repo:
# the two chains write to the SAME `wnba/<dataset>/{rds,parquet}/` path and
# clobber each other, so the checked-in tree only ever holds whichever ran last.
# Comparing it against the release asset compares a build to itself.
#
# Why the R side runs a chain and not one stage: the numbered R stages feed each
# other. espn_wnba_02_team_box reads `wnba/schedules/rds/wnba_schedule_{y}.rds`,
# which only stage 01 writes, and the repo does not retain schedules/rds. So
# every stage up to and including the target must run, in order. That is the
# cost that makes this weekly.
#
# Neither pipeline is authoritative. A divergence is a review item: decide which
# side is right, then fix the other. Do not "fix" it by editing one to match.
#
# Usage:
#   ops/output_parity.sh -d team_box -s 2025
#   RSCRIPT="/c/Program Files/R/R-4.6.1/bin/Rscript.exe" ops/output_parity.sh -d team_box -s 2025
#
# Env:
#   RSCRIPT                 R interpreter (default: Rscript on PATH). On the dev
#                           box, bare Rscript is 4.5.3 whose library lacks
#                           rlang/dplyr/arrow/wehoop -- point this at 4.6.x.
#   WEHOOP_WNBA_RAW_ROOT    raw store (default: the raw.githubusercontent base).
set -uo pipefail

DATASET=""
SEASON=""
while getopts "d:s:" flag; do
  case "${flag}" in
    d) DATASET=${OPTARG} ;;
    s) SEASON=${OPTARG} ;;
    *) echo "Usage: $0 -d <dataset> -s <season>" >&2; exit 2 ;;
  esac
done
if [ -z "${DATASET}" ] || [ -z "${SEASON}" ]; then
  echo "Usage: $0 -d <dataset> -s <season>" >&2
  exit 2
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RSCRIPT="${RSCRIPT:-Rscript}"
export WEHOOP_WNBA_RAW_ROOT="${WEHOOP_WNBA_RAW_ROOT:-https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main}"

# Join keys are MEASURED, not assumed -- each pair below was verified unique on
# a real season. Getting this wrong is not a soft failure: a non-identifying key
# fans the comparison join into a cross product. (game_id, team_id) is unique for
# team_box but NOT for player_box, which is exactly that trap.
case "${DATASET}" in
  team_box)   JOIN_KEYS="game_id team_id" ;;
  player_box) JOIN_KEYS="game_id athlete_id" ;;
  pbp)        JOIN_KEYS="game_id id" ;;
  *)
    echo "::error ::no verified join key for '${DATASET}'." >&2
    echo "Add one only after confirming it is unique on a real season -- do not guess." >&2
    exit 2
    ;;
esac

STAGE_FILE="$(ls "${REPO_DIR}"/R/espn_wnba_[0-9][0-9]_"${DATASET}"_creation.R 2>/dev/null | head -1)"
if [ -z "${STAGE_FILE}" ]; then
  echo "::error ::no R stage found for dataset '${DATASET}'" >&2
  exit 2
fi
TARGET_NN="$(basename "${STAGE_FILE}" | sed -E 's/^espn_wnba_([0-9]{2})_.*/\1/')"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
PY_OUT="${WORK}/py"
R_OUT="${WORK}/r"
mkdir -p "${PY_OUT}" "${R_OUT}"

echo "=== python build: ${DATASET} ${SEASON} ==="
( cd "${REPO_DIR}/python" && uv run python -m wnba_data_build \
    --dataset "${DATASET}" --base "${PY_OUT}" -s "${SEASON}" -e "${SEASON}" ) || {
  echo "::error ::python build failed for ${DATASET} ${SEASON}" >&2; exit 1; }

echo "=== R chain: stages 01..${TARGET_NN} (they feed each other) ==="
# The R stages call dir.create() one level at a time, which is NOT recursive, so
# `wnba/schedules` fails outright when `wnba/` is absent. In the repo that dir is
# tracked and always present; in a clean temp tree it is not. Seed it.
mkdir -p "${R_OUT}/wnba"
cd "${R_OUT}" || exit 1
for f in "${REPO_DIR}"/R/espn_wnba_[0-9][0-9]_*_creation.R; do
  nn="$(basename "$f" | sed -E 's/^espn_wnba_([0-9]{2})_.*/\1/')"
  # 10#: force base-10 so a leading zero is not read as octal.
  if [ "$((10#${nn}))" -le "$((10#${TARGET_NN}))" ]; then
    echo "--- Rscript $(basename "$f")"
    # NEVER invoke the stage directly: it publishes to the live release with no
    # dry-run gate. The wrapper replaces the publisher before sourcing it and
    # aborts if that swap fails, so this fails closed.
    SDV_PARITY_STAGE="$f" "${RSCRIPT}" "${REPO_DIR}/ops/_r_no_publish.R" \
      -s "${SEASON}" -e "${SEASON}" || {
      echo "::error ::R stage $(basename "$f") failed" >&2; exit 1; }
  fi
done

R_PARQUET="${R_OUT}/wnba/${DATASET}/parquet/${DATASET}_${SEASON}.parquet"
PY_PARQUET="${PY_OUT}/${DATASET}/parquet/${DATASET}_${SEASON}.parquet"
for p in "${R_PARQUET}" "${PY_PARQUET}"; do
  [ -f "$p" ] || { echo "::error ::expected artifact missing: $p" >&2; exit 1; }
done

echo "=== compare ==="
# --json, then parse. The CLI exits 1 for "findings were reported" AND python
# exits 1 for "the tool blew up", so the exit code alone cannot tell a real
# divergence from a crash. An earlier version of this script announced
# "R and Python disagree" for a ModuleNotFoundError -- reporting a broken tool
# as a data defect, which is the exact confusion this harness exists to avoid.
FINDINGS="${WORK}/findings.json"
# JOIN_KEYS is an intentional multi-word list, so it must stay unquoted here.
# shellcheck disable=SC2086
( cd "${REPO_DIR}/python" && uv run python -m tools.validation.cli compare \
    --dataset "wnba_${DATASET}" --domain wnba \
    --r-parquet "${R_PARQUET}" --py-parquet "${PY_PARQUET}" \
    --join-keys ${JOIN_KEYS} --json ) > "${FINDINGS}" 2> "${WORK}/compare.err"
rc=$?

if ! python -c "import json,sys; json.load(open(sys.argv[1]))" "${FINDINGS}" 2>/dev/null; then
  echo "::error ::the parity CHECK ITSELF failed to run -- this is a tooling problem, not a data divergence"
  sed -n '1,25p' "${WORK}/compare.err" >&2
  exit 2
fi

if [ $rc -eq 0 ]; then
  echo "R and Python agree on ${DATASET} ${SEASON}."
  exit 0
fi

echo "::error ::R and Python disagree on ${DATASET} ${SEASON} -- neither side is automatically right; decide which pipeline is correct, then fix the other"
python - "${FINDINGS}" <<'PY'
import json, sys
for f in json.load(open(sys.argv[1])):
    print(f"  {f['severity'].upper():5} {f['message']}")
PY
exit 1
