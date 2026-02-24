#!/usr/bin/env bash
# historical_load_s3.sh
# One-time upload of all WNBA parquet files to S3.
# Uploads every file unconditionally — intended for the initial historical load.
# Usage: ./scripts/historical_load_s3.sh <bucket-name> [--dry-run]

set -euo pipefail

BUCKET="${1:-}"
DRY_RUN="${2:-}"

if [[ -z "$BUCKET" ]]; then
  echo "Usage: $0 <bucket-name> [--dry-run]"
  echo "Example: $0 wehoop-wnba-data"
  exit 1
fi

LOCAL_DIR="$(cd "$(dirname "$0")/.." && pwd)/wnba"
S3_BASE="s3://$BUCKET/wnba"

DATASETS=(pbp player_box schedules team_box)

echo "WNBA Historical Load"
echo "  From: $LOCAL_DIR"
echo "  To:   $S3_BASE"
[[ "$DRY_RUN" == "--dry-run" ]] && echo "  Mode: DRY RUN (no files will be uploaded)"
echo ""

for dataset in "${DATASETS[@]}"; do
  src="$LOCAL_DIR/$dataset/parquet"
  dst="$S3_BASE/$dataset/parquet"

  if [[ ! -d "$src" ]]; then
    echo "[$dataset] Skipping — directory not found: $src"
    continue
  fi

  file_count=$(find "$src" -name "*.parquet" | wc -l)
  echo "[$dataset] Uploading $file_count parquet files..."

  aws s3 cp \
    "$src" \
    "$dst" \
    --recursive \
    --include "*.parquet" \
    --storage-class INTELLIGENT_TIERING \
    ${DRY_RUN:+--dryrun}

  echo "[$dataset] Done."
  echo ""
done

echo "Historical load complete."
