#!/usr/bin/env Rscript
# Serialize the Python-built parquet datasets to .rds and upload rds-only.
#
# R does ZERO reshaping here: the published .rds is byte-derived from the
# parity-passed parquet, so wehoop::load_wnba_* keeps its rds contract while
# Python (wnba_data_build) owns the reshape and publishes the parquet/csv
# assets itself.
#
# WNBA delta vs the WBB sibling: 12 datasets (draft is annual -- it is included
# here and simply skipped on days its parquet wasn't rebuilt), plus the two
# schedule extras (wnba_schedule_master + wnba_games_in_data_repo) that
# espn_wnba_03 shipped as rds+csv+parquet. Python writes their parquet/csv and
# uploads those; this script adds the rds.
#
# Usage: Rscript R/serialize_rds.R -s 2025 -e 2025 [--no-upload]
suppressPackageStartupMessages({
  library(arrow)
  library(glue)
  library(optparse)
  library(purrr)
})

option_list <- list(
  make_option(
    c("-s", "--start_year"),
    action = "store",
    default = wehoop::most_recent_wnba_season(),
    type = "integer"
  ),
  make_option(
    c("-e", "--end_year"),
    action = "store",
    default = wehoop::most_recent_wnba_season(),
    type = "integer"
  ),
  make_option(
    "--no-upload",
    action = "store_true",
    default = FALSE,
    dest = "no_upload",
    help = "serialize locally, skip the release upload"
  ),
  make_option(
    "--dataset",
    action = "store",
    default = "all",
    type = "character",
    help = paste(
      "serialize a single dataset (e.g. 'draft') instead of all 12.",
      "The annual draft workflow uses this so it doesn't re-upload the rds",
      "of every other dataset whose parquet happens to be in the tree."
    )
  )
)
opt <- parse_args(OptionParser(option_list = option_list))

retry_rate <- purrr::rate_backoff(pause_base = 1, pause_min = 1, max_times = 5)
any_failed <- FALSE

save_rds <- function(df, stem, tag, ds, pkg_fn, out_path) {
  df <- wehoop:::make_wehoop_data(
    df,
    glue("ESPN WNBA {ds} from wehoop data repository"),
    Sys.time()
  )
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  saveRDS(df, out_path)
  if (!opt$no_upload) {
    purrr::insistently(
      sportsdataversedata::sportsdataverse_save,
      rate = retry_rate,
      quiet = FALSE
    )(
      data_frame = df,
      file_name = stem,
      sportsdataverse_type = glue("{ds} data"),
      release_tag = tag,
      pkg_function = pkg_fn,
      file_types = c("rds"),
      .token = Sys.getenv("GITHUB_PAT")
    )
  }
  invisible(TRUE)
}

# dataset dir | file stem | release tag | pkg_function
# Mirrors wnba_data_build.config.REGISTRY exactly (tags are load-bearing).
T_ <- "espn_wnba_"
DATASETS <- list(
  list("pbp",                 "play_by_play",        paste0(T_, "pbp"),                 "wehoop::load_wnba_pbp()"),
  list("schedules",           "wnba_schedule",       paste0(T_, "schedules"),           "wehoop::load_wnba_schedule()"),
  list("shots",               "shots",               paste0(T_, "shots"),               "wehoop::load_wnba_shots()"),
  list("team_box",            "team_box",            paste0(T_, "team_boxscores"),      "wehoop::load_wnba_team_box()"),
  list("player_box",          "player_box",          paste0(T_, "player_boxscores"),    "wehoop::load_wnba_player_box()"),
  list("rosters",             "rosters",             paste0(T_, "rosters"),             "wehoop::load_wnba_rosters()"),
  list("player_season_stats", "player_season_stats", paste0(T_, "player_season_stats"), "wehoop::load_wnba_player_stats()"),
  list("team_season_stats",   "team_season_stats",   paste0(T_, "team_season_stats"),   "wehoop::load_wnba_team_stats()"),
  list("standings",           "standings",           paste0(T_, "standings"),           "wehoop::load_wnba_standings()"),
  list("game_rosters",        "game_rosters",        paste0(T_, "game_rosters"),        "wehoop::load_wnba_game_rosters()"),
  list("officials",           "officials",           paste0(T_, "officials"),           "wehoop::load_wnba_officials()"),
  list("draft",               "draft",               paste0(T_, "draft"),               "wehoop::load_wnba_draft()")
)

# The two full-history schedule extras. Not season-scoped: Python rewrites them
# whenever the schedules dataset is rebuilt, so serialize them once per run.
EXTRAS <- list(
  list("wnba_schedule_master",     "wehoop::load_wnba_schedule()"),
  list("wnba_games_in_data_repo",  "wehoop::load_wnba_games()")
)

if (opt$dataset != "all") {
  keep <- purrr::keep(DATASETS, ~ .x[[1]] == opt$dataset)
  if (length(keep) == 0) {
    stop(glue("unknown --dataset '{opt$dataset}'"))
  }
  DATASETS <- keep
  # The extras belong to the schedules dataset only.
  if (opt$dataset != "schedules") EXTRAS <- list()
}

for (y in opt$s:opt$e) {
  for (d in DATASETS) {
    ds <- d[[1]]
    stem <- d[[2]]
    tag <- d[[3]]
    pkg_fn <- d[[4]]
    pq <- glue("wnba/{ds}/parquet/{stem}_{y}.parquet")
    if (!file.exists(pq)) {
      cli::cli_alert_info("{Sys.time()}: no parquet for {ds} {y}; skipping rds")
      next
    }
    ok <- tryCatch(
      {
        save_rds(
          arrow::read_parquet(pq),
          glue("{stem}_{y}"),
          tag,
          ds,
          pkg_fn,
          glue("wnba/{ds}/rds/{stem}_{y}.rds")
        )
      },
      error = function(e) {
        cli::cli_alert_warning(
          "{Sys.time()}: rds serialize failed for {ds} {y}: {e$message}"
        )
        FALSE
      }
    )
    if (!ok) any_failed <- TRUE
  }
}

for (x in EXTRAS) {
  stem <- x[[1]]
  pkg_fn <- x[[2]]
  pq <- glue("wnba/schedules/{stem}.parquet")
  if (!file.exists(pq)) {
    cli::cli_alert_info("{Sys.time()}: no parquet for {stem}; skipping rds")
    next
  }
  ok <- tryCatch(
    {
      save_rds(
        arrow::read_parquet(pq),
        stem,
        paste0(T_, "schedules"),
        "schedule",
        pkg_fn,
        glue("wnba/schedules/rds/{stem}.rds")
      )
    },
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: rds serialize failed for {stem}: {e$message}"
      )
      FALSE
    }
  )
  if (!ok) any_failed <- TRUE
}

if (any_failed) quit(status = 1)
