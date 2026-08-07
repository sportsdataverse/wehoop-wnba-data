#!/usr/bin/env Rscript

gcol <- gc()

suppressPackageStartupMessages({
  library(optparse)
  library(dplyr)
})

# Sourced up-front: upsert_manifest_row() is called inside the season loop.
source(file.path("R", "manifest_upload_helper.R"))

# Stage 16 -- player_core. Twin of python/espn_wnba_16_player_core_creation.py.
#
# Athlete identity + bio for the athletes who appeared in a season. Until
# 2026-08-03 this dataset had NO R producer: it was the one released dataset
# only the Python pipeline could build, which is a parity gap under the
# dual-pipeline policy. Closing it needed the projection to exist in wehoop
# first (wehoop::espn_basketball_player_core), because this repo's rule is that
# ESPN JSON parsing lives in wehoop, not here.
#
# What the season partition MEANS: "the athletes who appeared in season Y, with
# their CURRENT bio". The season dimension is participation -- it is NOT the
# bio's vintage. ESPN overwrites height/weight/jersey in place, so era-correct
# bio is not obtainable from any ESPN endpoint, and current_team_id is the
# athlete's team TODAY, not their season team (that lives in player_box).

option_list <- list(
  make_option(c("-s", "--start_year"),
    type = "integer", default = NULL,
    help = "Start season (end year, e.g. 2026 = 2025-26)"
  ),
  make_option(c("-e", "--end_year"),
    type = "integer", default = NULL,
    help = "End season (end year); defaults to --start_year"
  )
)
opt <- parse_args(OptionParser(option_list = option_list))
if (is.null(opt$start_year)) stop("--start_year is required")
if (is.null(opt$end_year)) opt$end_year <- opt$start_year

years_vec <- opt$start_year:opt$end_year

# Athlete-keyed raw payloads: nba/player_core/json/{athlete_id}.json. The
# core-v2 athlete resource takes no season parameter, so the tree is FLAT --
# one file per athlete, serving every season they played.
raw_base <- "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/player_core/json"

# --- helpers ---------------------------------------------------------------

# "Who played in season Y" comes from the already-released player_box. Only the
# ID SET is needed, not identity columns -- player_core *is* the identity
# source, so unlike player_season_stats there is no identity lookup to graft on.
season_athlete_ids <- function(season) {
  pb <- tryCatch(
    wehoop::load_wnba_player_box(seasons = season),
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: could not load player_box for {season}: {e$message}"
      )
      NULL
    }
  )
  if (is.null(pb) || nrow(pb) == 0) return(integer())
  ids <- suppressWarnings(as.integer(as.data.frame(pb)[["athlete_id"]]))
  sort(unique(ids[!is.na(ids)]))
}

parse_one_athlete <- function(athlete_id, raw_base) {
  payload <- tryCatch(
    jsonlite::fromJSON(
      glue::glue("{raw_base}/{athlete_id}.json"),
      simplifyVector = FALSE
    ),
    error = function(e) NULL
  )
  if (is.null(payload)) return(NULL)
  out <- tryCatch(
    wehoop::espn_basketball_player_core(payload, athlete_id = athlete_id),
    error = function(e) NULL
  )
  if (is.null(out) || nrow(out) == 0) return(NULL)
  out
}

# --- main loop -------------------------------------------------------------

build_season_player_core <- function(y) {
  athlete_ids <- season_athlete_ids(y)
  if (length(athlete_ids) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no player_box athletes for {y}; skipping player_core"
    )
    return(invisible(NULL))
  }

  cli::cli_progress_step(
    msg = "Compiling {y} ESPN WNBA player core ({length(athlete_ids)} athletes)",
    msg_done = "Compiled {y} ESPN WNBA player core!"
  )

  future::plan("multisession")
  core <- furrr::future_map_dfr(
    athlete_ids,
    function(a) parse_one_athlete(a, raw_base),
    .options = furrr::furrr_options(seed = TRUE)
  )

  if (is.null(core) || nrow(core) == 0) {
    cli::cli_alert_warning("{Sys.time()}: no player_core rows parsed for {y}")
    return(invisible(NULL))
  }

  # The projection is a pure athlete record and deliberately takes no season --
  # a core record is not season data. The season belongs to the PARTITION, so
  # it is stamped here, keeping the released frame self-describing when seasons
  # are concatenated. Column order matches the python twin: season first.
  core <- core %>%
    dplyr::distinct() %>%
    dplyr::mutate(season = as.integer(y)) %>%
    dplyr::relocate(season) %>%
    wehoop:::make_wehoop_data(
      "ESPN WNBA Player Core from hoopR data repository",
      Sys.time()
    )

  for (d in c("wnba/player_core", "wnba/player_core/rds", "wnba/player_core/parquet")) {
    if (!dir.exists(d)) dir.create(d, recursive = TRUE)
  }

  saveRDS(core, glue::glue("wnba/player_core/rds/player_core_{y}.rds"))
  arrow::write_parquet(
    core,
    glue::glue("wnba/player_core/parquet/player_core_{y}.parquet")
  )

  cli::cli_progress_step(
    msg = "Updating {y} ESPN WNBA Player Core GitHub Release",
    msg_done = "Updated {y} ESPN WNBA Player Core GitHub Release!"
  )

  retry_rate <- purrr::rate_backoff(pause_base = 1, pause_min = 1, max_times = 5)
  purrr::insistently(
    sportsdataversedata::sportsdataverse_save,
    rate = retry_rate,
    quiet = FALSE
  )(
    data_frame = core,
    file_name = glue::glue("player_core_{y}"),
    sportsdataverse_type = "player core data",
    release_tag = "espn_wnba_player_core",
    # No hoopR loader ships for this dataset yet -- the python registry records
    # the same. Point at the release rather than inventing a function name.
    pkg_function = "sportsdataverse-data release espn_wnba_player_core",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  manifest_path <- "wnba/player_core/wnba_player_core_in_data_repo.csv"
  manifest_row <- tibble::tibble(
    season           = as.integer(y),
    row_count        = as.integer(nrow(core)),
    generated_at_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    source_endpoint  = glue::glue("{raw_base}/<athlete_id>.json")
  )
  # One row per season; see upsert_manifest_row() in
  # R/manifest_upload_helper.R.
  upsert_manifest_row(manifest_path, manifest_row, y)

  rm(core)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, function(y) {
  tryCatch(
    build_season_player_core(y),
    error = function(e) {
      cli::cli_alert_danger(
        "{Sys.time()}: player_core season {y} failed: {e$message}"
      )
    }
  )
})
tictoc::toc()

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
