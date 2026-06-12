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

# --- main loop -------------------------------------------------------------

build_season_team_crosswalk <- function(y) {
  cli::cli_progress_step(
    msg = "Compiling {y} WNBA team crosswalk",
    msg_done = "Compiled {y} WNBA team crosswalk!"
  )

  team_crosswalk <- tryCatch(
    wehoop::wnba_team_crosswalk(season = y),
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: skip team crosswalk {y}: {e$message}"
      )
      NULL
    }
  )

  if (is.null(team_crosswalk) || nrow(team_crosswalk) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no team crosswalk rows for {y}"
    )
    return(invisible(NULL))
  }

  ifelse(
    !dir.exists(file.path("wnba/crosswalk")),
    dir.create(file.path("wnba/crosswalk"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/crosswalk/rds")),
    dir.create(file.path("wnba/crosswalk/rds"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/crosswalk/parquet")),
    dir.create(file.path("wnba/crosswalk/parquet"), recursive = TRUE),
    FALSE
  )

  saveRDS(
    team_crosswalk,
    glue::glue("wnba/crosswalk/rds/wnba_team_crosswalk_{y}.rds")
  )
  arrow::write_parquet(
    team_crosswalk,
    glue::glue("wnba/crosswalk/parquet/wnba_team_crosswalk_{y}.parquet"),
    compression = "zstd",
    compression_level = 22
  )

  cli::cli_progress_step(
    msg = "Updating {y} WNBA Team Crosswalk GitHub Release",
    msg_done = "Updated {y} WNBA Team Crosswalk GitHub Release!"
  )

  retry_rate <- purrr::rate_backoff(
    pause_base = 1,
    pause_min = 60,
    max_times = 10
  )
  purrr::insistently(
    sportsdataversedata::sportsdataverse_save,
    rate = retry_rate,
    quiet = FALSE
  )(
    data_frame = team_crosswalk,
    file_name = glue::glue("wnba_team_crosswalk_{y}"),
    sportsdataverse_type = "team crosswalk data",
    release_tag = "wnba_crosswalk",
    pkg_function = "wehoop::wnba_team_crosswalk()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  # --- Manifest row append --------------------------------------------------
  manifest_path <- "wnba/crosswalk/wnba_team_crosswalk_in_data_repo.csv"
  manifest_row <- tibble::tibble(
    season           = as.integer(y),
    row_count        = as.integer(nrow(team_crosswalk)),
    generated_at_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    source_endpoint  = "wehoop::wnba_team_crosswalk()"
  )
  if (file.exists(manifest_path)) {
    data.table::fwrite(manifest_row, manifest_path, append = TRUE)
  } else {
    data.table::fwrite(manifest_row, manifest_path)
  }

  rm(team_crosswalk)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, function(y) {
  tryCatch(
    build_season_team_crosswalk(y),
    error = function(e) {
      cli::cli_alert_danger(
        "{Sys.time()}: team crosswalk season {y} failed: {e$message}"
      )
    }
  )
})
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
tryCatch({
  source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
  upload_wnba_manifest(
    manifest_path        = "wnba/crosswalk/wnba_team_crosswalk_in_data_repo.csv",
    release_tag          = "wnba_crosswalk",
    file_name            = "wnba_team_crosswalk_in_data_repo",
    sportsdataverse_type = "team crosswalk manifest",
    pkg_function         = "wehoop::wnba_team_crosswalk()"
  )
}, error = function(e) {
  cli::cli_alert_warning(
    "{Sys.time()}: team crosswalk manifest upload failed (non-fatal): {e$message}"
  )
})

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
