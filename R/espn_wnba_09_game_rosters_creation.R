#!/usr/bin/env Rscript
rm(list = ls())
gcol <- gc()

suppressPackageStartupMessages(suppressMessages(library(dplyr)))
suppressPackageStartupMessages(suppressMessages(library(magrittr)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite)))
suppressPackageStartupMessages(suppressMessages(library(purrr)))
suppressPackageStartupMessages(suppressMessages(library(progressr)))
suppressPackageStartupMessages(suppressMessages(library(data.table)))
suppressPackageStartupMessages(suppressMessages(library(arrow)))
suppressPackageStartupMessages(suppressMessages(library(glue)))
suppressPackageStartupMessages(suppressMessages(library(optparse)))
suppressPackageStartupMessages(suppressMessages(library(tibble)))
suppressPackageStartupMessages(suppressMessages(library(tidyr)))
suppressPackageStartupMessages(suppressMessages(library(rlang)))

option_list <- list(
  make_option(
    c("-s", "--start_year"),
    action = "store",
    default = wehoop:::most_recent_wnba_season(),
    type = "integer",
    help = "Start year of the seasons to process"
  ),
  make_option(
    c("-e", "--end_year"),
    action = "store",
    default = wehoop:::most_recent_wnba_season(),
    type = "integer",
    help = "End year of the seasons to process"
  )
)
opt <- parse_args(OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- opt$s:opt$e

raw_base <- "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/game_rosters/json"
sched_base <- "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/schedules/rds"

# --- helpers ---------------------------------------------------------------

safe_chr <- function(x) {
  if (is.null(x)) return(NA_character_)
  if (length(x) == 0) return(NA_character_)
  as.character(x[[1]])
}

safe_int <- function(x) {
  if (is.null(x)) return(NA_integer_)
  if (length(x) == 0) return(NA_integer_)
  suppressWarnings(as.integer(x[[1]]))
}

`%|%` <- function(a, b) {
  if (is.null(a) || length(a) == 0) return(b)
  if (is.na(a) || !nzchar(a)) return(b)
  a
}

list_game_ids <- function(season) {
  url <- glue::glue("{sched_base}/wnba_schedule_{season}.rds")
  sched <- tryCatch(
    wehoop:::rds_from_url(url),
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: could not load schedule for {season}: {e$message}"
      )
      NULL
    }
  )
  if (is.null(sched) || nrow(sched) == 0) return(character())
  if (!"game_id" %in% colnames(sched)) return(character())
  ids <- as.character(unique(sched$game_id))
  ids[!is.na(ids) & nzchar(ids)]
}

parse_one_athlete <- function(season, game_id, team_block, athlete) {
  team_id <- safe_int(team_block[["team"]][["id"]] %||% team_block[["id"]])
  team_slug <- safe_chr(
    team_block[["team"]][["slug"]] %||% team_block[["slug"]]
  )
  team_abbr <- safe_chr(
    team_block[["team"]][["abbreviation"]] %||%
      team_block[["abbreviation"]]
  )
  team_display_name <- safe_chr(
    team_block[["team"]][["displayName"]] %||%
      team_block[["displayName"]]
  )
  home_away <- safe_chr(team_block[["homeAway"]])

  ath <- athlete[["athlete"]] %||% athlete

  position <- safe_chr(
    athlete[["position"]][["abbreviation"]] %||%
      ath[["position"]][["abbreviation"]] %||%
      ath[["position"]][["name"]]
  )

  jersey <- safe_chr(athlete[["jersey"]] %||% ath[["jersey"]])

  starter <- athlete[["starter"]] %||% ath[["starter"]]
  if (is.null(starter)) starter <- NA
  starter <- as.logical(starter)

  did_not_play <- athlete[["didNotPlay"]] %||% ath[["didNotPlay"]]
  if (is.null(did_not_play)) did_not_play <- NA
  did_not_play <- as.logical(did_not_play)

  active <- athlete[["active"]] %||% ath[["active"]]
  if (is.null(active)) active <- NA
  active <- as.logical(active)

  ejected <- athlete[["ejected"]] %||% ath[["ejected"]]
  if (is.null(ejected)) ejected <- NA
  ejected <- as.logical(ejected)

  reason <- safe_chr(athlete[["reason"]] %||% ath[["reason"]])

  tibble::tibble(
    season = as.integer(season),
    game_id = as.character(game_id),
    team_id = team_id,
    team_slug = team_slug,
    team_abbreviation = team_abbr,
    team_display_name = team_display_name,
    home_away = home_away,
    athlete_id = safe_int(ath[["id"]]),
    athlete_uid = safe_chr(ath[["uid"]]),
    athlete_guid = safe_chr(ath[["guid"]]),
    athlete_display_name = safe_chr(ath[["displayName"]]),
    athlete_short_name = safe_chr(ath[["shortName"]]),
    athlete_first_name = safe_chr(ath[["firstName"]]),
    athlete_last_name = safe_chr(ath[["lastName"]]),
    athlete_jersey = jersey,
    athlete_position = position,
    athlete_headshot = safe_chr(
      ath[["headshot"]][["href"]] %||% ath[["headshot"]]
    ),
    starter = starter,
    did_not_play = did_not_play,
    active = active,
    ejected = ejected,
    reason = reason
  )
}

parse_one_game <- function(season, game_id) {
  url <- glue::glue("{raw_base}/{game_id}.json")
  raw <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = FALSE),
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: skip game_rosters {season}/{game_id}: {e$message}"
      )
      NULL
    }
  )
  if (is.null(raw)) return(NULL)

  rosters <- raw[["rosters"]] %||%
    raw[["teams"]] %||%
    raw[["boxscore"]][["players"]] %||%
    list()
  if (length(rosters) == 0) return(NULL)

  purrr::map_dfr(rosters, function(team_block) {
    athletes <- team_block[["roster"]] %||%
      team_block[["athletes"]] %||%
      list()
    if (length(athletes) == 0) return(NULL)
    purrr::map_dfr(athletes, function(a) {
      tryCatch(
        parse_one_athlete(season, game_id, team_block, a),
        error = function(e) {
          cli::cli_alert_warning(
            "{Sys.time()}: athlete parse issue in {game_id}: {e$message}"
          )
          NULL
        }
      )
    })
  })
}

write_manifest_row <- function(season, row_count, source_endpoint) {
  manifest_path <- "wnba/game_rosters/wnba_game_rosters_in_data_repo.csv"
  ifelse(
    !dir.exists(file.path("wnba/game_rosters")),
    dir.create(file.path("wnba/game_rosters"), recursive = TRUE),
    FALSE
  )
  row <- tibble::tibble(
    season = as.integer(season),
    row_count = as.integer(row_count),
    generated_at_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    source_endpoint = source_endpoint
  )
  if (file.exists(manifest_path)) {
    data.table::fwrite(row, manifest_path, append = TRUE)
  } else {
    data.table::fwrite(row, manifest_path)
  }
  invisible(NULL)
}

# --- main loop -------------------------------------------------------------

build_season_game_rosters <- function(y) {
  game_ids <- list_game_ids(y)
  if (length(game_ids) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no games listed for {y}; skipping game_rosters"
    )
    return(invisible(NULL))
  }

  cli::cli_progress_step(
    msg = "Compiling {y} ESPN WNBA game rosters ({length(game_ids)} games)",
    msg_done = "Compiled {y} ESPN WNBA game rosters!"
  )

  rosters_df <- purrr::map_dfr(game_ids, function(g) {
    tryCatch(
      parse_one_game(y, g),
      error = function(e) {
        cli::cli_alert_warning(
          "{Sys.time()}: game_rosters issue for {g}: {e$message}"
        )
        NULL
      }
    )
  })

  if (is.null(rosters_df) || nrow(rosters_df) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no game_rosters rows parsed for {y}"
    )
    return(invisible(NULL))
  }

  rosters_df <- rosters_df %>%
    dplyr::distinct() %>%
    wehoop:::make_wehoop_data(
      "ESPN WNBA Game Rosters from wehoop data repository",
      Sys.time()
    )

  ifelse(
    !dir.exists(file.path("wnba/game_rosters")),
    dir.create(file.path("wnba/game_rosters"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/game_rosters/rds")),
    dir.create(file.path("wnba/game_rosters/rds"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/game_rosters/parquet")),
    dir.create(file.path("wnba/game_rosters/parquet"), recursive = TRUE),
    FALSE
  )

  saveRDS(
    rosters_df,
    glue::glue("wnba/game_rosters/rds/game_rosters_{y}.rds")
  )
  arrow::write_parquet(
    rosters_df,
    glue::glue("wnba/game_rosters/parquet/game_rosters_{y}.parquet")
  )

  cli::cli_progress_step(
    msg = "Updating {y} ESPN WNBA Game Rosters GitHub Release",
    msg_done = "Updated {y} ESPN WNBA Game Rosters GitHub Release!"
  )

  retry_rate <- purrr::rate_backoff(
    pause_base = 1,
    pause_min = 1,
    max_times = 5
  )
  purrr::insistently(
    sportsdataversedata::sportsdataverse_save,
    rate = retry_rate,
    quiet = FALSE
  )(
    data_frame = rosters_df,
    file_name = glue::glue("game_rosters_{y}"),
    sportsdataverse_type = "game rosters data",
    release_tag = "espn_wnba_game_rosters",
    pkg_function = "wehoop::load_wnba_pbp()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  write_manifest_row(
    season = y,
    row_count = nrow(rosters_df),
    source_endpoint = glue::glue("{raw_base}/<game_id>.json")
  )

  rm(rosters_df)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, function(y) {
  tryCatch(
    build_season_game_rosters(y),
    error = function(e) {
      cli::cli_alert_danger(
        "{Sys.time()}: game_rosters season {y} failed: {e$message}"
      )
    }
  )
})
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
tryCatch({
  source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
  upload_wnba_manifest(
    manifest_path        = "wnba/game_rosters/wnba_game_rosters_in_data_repo.csv",
    release_tag          = "espn_wnba_game_rosters",
    file_name            = "wnba_game_rosters_in_data_repo",
    sportsdataverse_type = "game rosters manifest",
    pkg_function         = "wehoop::load_wnba_game_rosters_manifest()"
  )
}, error = function(e) {
  cli::cli_alert_warning(
    "{Sys.time()}: game_rosters manifest upload failed (non-fatal): {e$message}"
  )
})

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
