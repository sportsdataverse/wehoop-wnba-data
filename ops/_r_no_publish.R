# Run one R creation stage with publishing made IMPOSSIBLE.
#
# The espn_wnba_*_creation.R stages call
# `sportsdataversedata::sportsdataverse_save()` unconditionally -- there is no
# dry-run flag -- so simply running one against a machine with credentials
# publishes to the live sportsdataverse-data release. That is how the 2025
# pbp/shots/team_box assets were overwritten on 2026-08-07.
#
# Blanking GITHUB_PAT is NOT the fix: the save is wrapped in
# `purrr::insistently(rate = rate_backoff(pause_min = 60, max_times = 10))`, so
# an unauthorized upload retries for ~10 minutes per dataset before failing.
# Replacing the function is both instant and fails CLOSED -- a parity run cannot
# reach a release even when valid credentials are present in the environment.
#
# Usage:
#   SDV_PARITY_STAGE=R/espn_wnba_02_team_box_creation.R \
#     Rscript ops/_r_no_publish.R -s 2025 -e 2025
#
# The stage's own `-s/-e` flags pass straight through: optparse reads
# commandArgs(), which this wrapper does not consume.

stage <- Sys.getenv("SDV_PARITY_STAGE")
if (!nzchar(stage)) stop("SDV_PARITY_STAGE is not set")
if (!file.exists(stage)) stop(sprintf("stage not found: %s", stage))

suppressPackageStartupMessages(library(sportsdataversedata))

.parity_blocked_save <- function(...) {
  args <- list(...)
  message(sprintf(
    "[parity] publish SUPPRESSED for '%s' -- this run must not touch a release",
    if (!is.null(args$file_name)) args$file_name else "<unknown>"
  ))
  invisible(NULL)
}

assignInNamespace(
  "sportsdataverse_save",
  .parity_blocked_save,
  ns = "sportsdataversedata"
)

# Guard the guard: if the swap silently failed, the stage below would publish
# for real. Refuse to run rather than find out afterwards.
if (!identical(
  body(sportsdataversedata::sportsdataverse_save),
  body(.parity_blocked_save)
)) {
  stop("failed to suppress sportsdataverse_save -- refusing to run the stage")
}

# `rm(list = ls())` at the top of every stage clears the global env, but the
# namespace binding above is unaffected, so the suppression survives it.
source(stage, echo = FALSE)
