# Create the GitHub releases on sportsdataverse/sportsdataverse-data that
# the espn_wnba_*_creation.R parsers upload artifacts to via
# piggyback::pb_upload. Each release is created with an empty body; assets
# land later during the daily/weekly parser runs.
#
# Source-specific: this repo (wehoop-wnba-data) owns the espn_wnba_* tags.
# The sister init scripts in wehoop-wbb-data and wehoop-wnba-stats-data own
# their own source's tags.
#
# Idempotent: a release that already exists is skipped, not re-created.
# Re-run this any time a new espn_wnba_*_creation.R script lands.

create_release <- function(tag, body) {
  tryCatch(
    piggyback::pb_release_create(
      repo = "sportsdataverse/sportsdataverse-data",
      tag  = tag,
      name = tag,
      body = body,
      .token = Sys.getenv("GITHUB_PAT")
    ),
    error = function(e) {
      # piggyback can wrap "already exists" across a newline depending on tag
      # length / cli width; collapse whitespace before matching so we don't
      # miss the line-broken variant.
      msg <- gsub("\\s+", " ", conditionMessage(e))
      if (grepl("already exists|already_exists|Validation Failed", msg, ignore.case = TRUE)) {
        message("Skipping (already exists): ", tag)
      } else {
        stop(e)
      }
    }
  )
}

#--- ESPN WNBA ---------------------------------------------------------------

# Original 4 (pre-Phase 1; pre-existing on sportsdataverse-data)
create_release("espn_wnba_schedules",        "WNBA Schedules Data (from ESPN)")
create_release("espn_wnba_team_boxscores",   "WNBA Team Boxscores Data (from ESPN)")
create_release("espn_wnba_player_boxscores", "WNBA Player Boxscores Data (from ESPN)")
create_release("espn_wnba_pbp",              "WNBA Play-by-Play Data (from ESPN)")

# Phase 1-6 additions (per-season + per-game + annual draft)
create_release("espn_wnba_rosters",             "WNBA Team Rosters Data (from ESPN)")
create_release("espn_wnba_player_season_stats", "WNBA Player Season Stats Data (from ESPN)")
create_release("espn_wnba_team_season_stats",   "WNBA Team Season Stats Data (from ESPN)")
create_release("espn_wnba_standings",           "WNBA Standings Data (from ESPN)")
create_release("espn_wnba_shots",               "WNBA Shots Data (from ESPN)")
create_release("espn_wnba_draft",               "WNBA Draft Data (from ESPN)")
create_release("espn_wnba_game_rosters",        "WNBA Game Rosters Data (from ESPN)")
create_release("espn_wnba_officials",           "WNBA Officials Data (from ESPN)")
