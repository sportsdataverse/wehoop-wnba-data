#!/bin/bash
# DEPRECATED shim -- retained so existing callers keep working.
# The single entrypoint is scripts/daily_wnba_data_processor.sh (design D21);
# the R path is selected there with -l R rather than by a separate script.
echo "::warning ::daily_wnba_R_processor.sh is deprecated; use scripts/daily_wnba_data_processor.sh -l R" >&2
exec bash "$(dirname "$0")/daily_wnba_data_processor.sh" "$@" -l R
