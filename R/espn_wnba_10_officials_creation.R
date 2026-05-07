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

raw_base <- "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/officials/json"
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

parse_one_official <- function(season, game_id, official) {
  position <- official[["position"]]
  pos_name <- safe_chr(
    if (is.list(position)) position[["name"]] else position
  )
  pos_display <- safe_chr(
    if (is.list(position)) position[["displayName"]] else NULL
  )

  tibble::tibble(
    season = as.integer(season),
    game_id = as.character(game_id),
    official_id = safe_int(official[["id"]]),
    official_uid = safe_chr(official[["uid"]]),
    official_full_name = safe_chr(
      official[["fullName"]] %||% official[["displayName"]]
    ),
    official_display_name = safe_chr(official[["displayName"]]),
    official_first_name = safe_chr(official[["firstName"]]),
    official_last_name = safe_chr(official[["lastName"]]),
    official_order = safe_int(official[["order"]]),
    position_name = pos_name,
    position_display_name = pos_display
  )
}

parse_one_game <- function(season, game_id) {
  url <- glue::glue("{raw_base}/{game_id}.json")
  raw <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = FALSE),
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: skip officials {season}/{game_id}: {e$message}"
      )
      NULL
    }
  )
  if (is.null(raw)) return(NULL)

  officials <- raw[["officials"]] %||%
    raw[["items"]] %||%
    raw[["gameInfo"]][["officials"]] %||%
    list()
  if (length(officials) == 0) return(NULL)

  purrr::map_dfr(officials, function(o) {
    tryCatch(
      parse_one_official(season, game_id, o),
      error = function(e) {
        cli::cli_alert_warning(
          "{Sys.time()}: official parse issue in {game_id}: {e$message}"
        )
        NULL
      }
    )
  })
}

write_manifest_row <- function(season, row_count, source_endpoint) {
  manifest_path <- "wnba/officials/wnba_officials_in_data_repo.csv"
  ifelse(
    !dir.exists(file.path("wnba/officials")),
    dir.create(file.path("wnba/officials"), recursive = TRUE),
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

build_season_officials <- function(y) {
  game_ids <- list_game_ids(y)
  if (length(game_ids) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no games listed for {y}; skipping officials"
    )
    return(invisible(NULL))
  }

  cli::cli_progress_step(
    msg = "Compiling {y} ESPN WNBA officials ({length(game_ids)} games)",
    msg_done = "Compiled {y} ESPN WNBA officials!"
  )

  officials_df <- purrr::map_dfr(game_ids, function(g) {
    tryCatch(
      parse_one_game(y, g),
      error = function(e) {
        cli::cli_alert_warning(
          "{Sys.time()}: officials issue for {g}: {e$message}"
        )
        NULL
      }
    )
  })

  if (is.null(officials_df) || nrow(officials_df) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no officials rows parsed for {y}"
    )
    return(invisible(NULL))
  }

  officials_df <- officials_df %>%
    dplyr::distinct() %>%
    wehoop:::make_wehoop_data(
      "ESPN WNBA Officials from wehoop data repository",
      Sys.time()
    )

  ifelse(
    !dir.exists(file.path("wnba/officials")),
    dir.create(file.path("wnba/officials"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/officials/rds")),
    dir.create(file.path("wnba/officials/rds"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/officials/parquet")),
    dir.create(file.path("wnba/officials/parquet"), recursive = TRUE),
    FALSE
  )

  saveRDS(
    officials_df,
    glue::glue("wnba/officials/rds/officials_{y}.rds")
  )
  arrow::write_parquet(
    officials_df,
    glue::glue("wnba/officials/parquet/officials_{y}.parquet")
  )

  cli::cli_progress_step(
    msg = "Updating {y} ESPN WNBA Officials GitHub Release",
    msg_done = "Updated {y} ESPN WNBA Officials GitHub Release!"
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
    data_frame = officials_df,
    file_name = glue::glue("officials_{y}"),
    sportsdataverse_type = "officials data",
    release_tag = "espn_wnba_officials",
    pkg_function = "wehoop::load_wnba_pbp()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  write_manifest_row(
    season = y,
    row_count = nrow(officials_df),
    source_endpoint = glue::glue("{raw_base}/<game_id>.json")
  )

  rm(officials_df)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, function(y) {
  tryCatch(
    build_season_officials(y),
    error = function(e) {
      cli::cli_alert_danger(
        "{Sys.time()}: officials season {y} failed: {e$message}"
      )
    }
  )
})
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
tryCatch({
  source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
  upload_wnba_manifest(
    manifest_path        = "wnba/officials/wnba_officials_in_data_repo.csv",
    release_tag          = "espn_wnba_officials",
    file_name            = "wnba_officials_in_data_repo",
    sportsdataverse_type = "officials manifest",
    pkg_function         = "wehoop::load_wnba_officials_manifest()"
  )
}, error = function(e) {
  cli::cli_alert_warning(
    "{Sys.time()}: officials manifest upload failed (non-fatal): {e$message}"
  )
})

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
