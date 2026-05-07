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

raw_base <- "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/standings/json"

# --- helpers ---------------------------------------------------------------

safe_chr <- function(x) {
  if (is.null(x)) return(NA_character_)
  if (length(x) == 0) return(NA_character_)
  as.character(x[[1]])
}

`%|%` <- function(a, b) {
  if (is.null(a) || length(a) == 0) return(b)
  if (is.na(a) || !nzchar(a)) return(b)
  a
}

# WNBA standings payload: similar shape to WBB. Top-level may carry
# `children[]` (conferences East/West) each with `standings.entries[]`, or a
# single flat `standings.entries[]`.
parse_one_entry <- function(season, group_meta, entry) {
  team <- entry[["team"]] %||% list()
  stats <- entry[["stats"]] %||% list()
  if (length(stats) == 0) return(NULL)

  stat_name <- purrr::map_chr(stats, ~ safe_chr(.x[["name"]]))
  stat_display_name <- purrr::map_chr(stats, ~ safe_chr(.x[["displayName"]]))
  stat_short_display_name <- purrr::map_chr(
    stats,
    ~ safe_chr(.x[["shortDisplayName"]])
  )
  stat_description <- purrr::map_chr(stats, ~ safe_chr(.x[["description"]]))
  stat_abbreviation <- purrr::map_chr(stats, ~ safe_chr(.x[["abbreviation"]]))
  stat_type <- purrr::map_chr(stats, ~ safe_chr(.x[["type"]]))
  display_value <- purrr::map_chr(stats, ~ safe_chr(.x[["displayValue"]]))
  value_chr <- purrr::map_chr(stats, ~ safe_chr(.x[["value"]]))
  value <- suppressWarnings(as.numeric(value_chr))

  tibble::tibble(
    season = season,
    group_id = group_meta$group_id,
    group_name = group_meta$group_name,
    group_abbreviation = group_meta$group_abbreviation,
    group_short_name = group_meta$group_short_name,
    team_id = suppressWarnings(as.integer(safe_chr(team[["id"]]))),
    team_uid = safe_chr(team[["uid"]]),
    team_slug = safe_chr(team[["slug"]]),
    team_location = safe_chr(team[["location"]]),
    team_name = safe_chr(team[["name"]]),
    team_abbreviation = safe_chr(team[["abbreviation"]]),
    team_display_name = safe_chr(team[["displayName"]]),
    team_short_display_name = safe_chr(team[["shortDisplayName"]]),
    team_color = safe_chr(team[["color"]]),
    team_alternate_color = safe_chr(team[["alternateColor"]]),
    team_logo = safe_chr(
      team[["logo"]] %||% purrr::pluck(team, "logos", 1, "href")
    ),
    stat_name = stat_name,
    stat_display_name = stat_display_name,
    stat_short_display_name = stat_short_display_name,
    stat_description = stat_description,
    stat_abbreviation = stat_abbreviation,
    stat_type = stat_type,
    display_value = display_value,
    value = value
  )
}

walk_groups <- function(season, node, parent_meta = NULL) {
  group_meta <- list(
    group_id = safe_chr(node[["id"]]),
    group_name = safe_chr(node[["name"]]) %|% safe_chr(node[["displayName"]]),
    group_abbreviation = safe_chr(node[["abbreviation"]]),
    group_short_name = safe_chr(node[["shortName"]])
  )
  if (is.null(group_meta$group_id) && !is.null(parent_meta)) {
    group_meta <- parent_meta
  }

  pieces <- list()

  entries <- node[["standings"]][["entries"]] %||%
    node[["entries"]] %||%
    list()
  if (length(entries) > 0) {
    pieces <- c(pieces, list(purrr::map_dfr(entries, function(e) {
      parse_one_entry(season, group_meta, e)
    })))
  }

  children <- node[["children"]] %||% node[["groups"]] %||% list()
  if (length(children) > 0) {
    pieces <- c(pieces, purrr::map(children, function(ch) {
      walk_groups(season, ch, group_meta)
    }))
  }

  do.call(rbind, pieces)
}

write_manifest_row <- function(season, row_count, source_endpoint) {
  manifest_path <- "wnba/standings/wnba_standings_in_data_repo.csv"
  ifelse(
    !dir.exists(file.path("wnba/standings")),
    dir.create(file.path("wnba/standings"), recursive = TRUE),
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

build_season_standings <- function(y) {
  url <- glue::glue("{raw_base}/{y}.json")

  raw <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = FALSE),
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: skip standings {y}: {e$message}"
      )
      NULL
    }
  )
  if (is.null(raw)) return(invisible(NULL))

  cli::cli_progress_step(
    msg = "Compiling {y} ESPN WNBA standings",
    msg_done = "Compiled {y} ESPN WNBA standings!"
  )

  standings <- walk_groups(y, raw)

  if (is.null(standings) || nrow(standings) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no standings rows parsed for {y}"
    )
    return(invisible(NULL))
  }

  standings <- standings %>%
    dplyr::as_tibble() %>%
    dplyr::distinct() %>%
    wehoop:::make_wehoop_data(
      "ESPN WNBA Standings from wehoop data repository",
      Sys.time()
    )

  ifelse(
    !dir.exists(file.path("wnba/standings")),
    dir.create(file.path("wnba/standings"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/standings/rds")),
    dir.create(file.path("wnba/standings/rds"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/standings/parquet")),
    dir.create(file.path("wnba/standings/parquet"), recursive = TRUE),
    FALSE
  )

  saveRDS(standings, glue::glue("wnba/standings/rds/standings_{y}.rds"))
  arrow::write_parquet(
    standings,
    glue::glue("wnba/standings/parquet/standings_{y}.parquet"),
    compression = "zstd",
    compression_level = 22
  )

  cli::cli_progress_step(
    msg = "Updating {y} ESPN WNBA Standings GitHub Release",
    msg_done = "Updated {y} ESPN WNBA Standings GitHub Release!"
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
    data_frame = standings,
    file_name = glue::glue("standings_{y}"),
    sportsdataverse_type = "standings data",
    release_tag = "espn_wnba_standings",
    pkg_function = "wehoop::load_wnba_standings()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  write_manifest_row(
    season = y,
    row_count = nrow(standings),
    source_endpoint = glue::glue("{raw_base}/{y}.json")
  )

  rm(standings)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, function(y) {
  tryCatch(
    build_season_standings(y),
    error = function(e) {
      cli::cli_alert_danger(
        "{Sys.time()}: standings season {y} failed: {e$message}"
      )
    }
  )
})
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
tryCatch({
  source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
  upload_wnba_manifest(
    manifest_path        = "wnba/standings/wnba_standings_in_data_repo.csv",
    release_tag          = "espn_wnba_standings",
    file_name            = "wnba_standings_in_data_repo",
    sportsdataverse_type = "standings manifest",
    pkg_function         = "wehoop::load_wnba_standings_manifest()"
  )
}, error = function(e) {
  cli::cli_alert_warning(
    "{Sys.time()}: standings manifest upload failed (non-fatal): {e$message}"
  )
})

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
