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

raw_base <- "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/team_stats/json"
gh_api_base <- "https://api.github.com/repos/sportsdataverse/wehoop-wnba-raw/contents/wnba/team_stats/json"

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

list_team_ids <- function(season) {
  api_url <- glue::glue("{gh_api_base}/{season}")
  pat <- Sys.getenv("GITHUB_PAT")
  resp <- tryCatch(
    expr = {
      if (nzchar(pat)) {
        jsonlite::fromJSON(
          httr::content(
            httr::GET(
              api_url,
              httr::add_headers(Authorization = paste("token", pat))
            ),
            as = "text",
            encoding = "UTF-8"
          ),
          simplifyDataFrame = TRUE
        )
      } else {
        jsonlite::fromJSON(api_url, simplifyDataFrame = TRUE)
      }
    },
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: Could not list team_stats for {season}: {e$message}"
      )
      NULL
    }
  )
  if (is.null(resp) || length(resp) == 0) return(integer())
  if (!is.data.frame(resp) || !"name" %in% colnames(resp)) return(integer())
  ids <- sub("\\.json$", "", resp$name)
  suppressWarnings(as.integer(ids[grepl("^[0-9]+$", ids)]))
}

parse_one_category <- function(season, team_id, team_meta, category) {
  totals <- category[["totals"]] %||% category[["values"]] %||% list()
  if (length(totals) == 0) return(NULL)

  labels <- unlist(category[["labels"]] %||% list(), use.names = FALSE)
  names_ <- unlist(category[["names"]] %||% list(), use.names = FALSE)
  display_names <- unlist(
    category[["displayNames"]] %||% list(),
    use.names = FALSE
  )
  descriptions <- unlist(
    category[["descriptions"]] %||% list(),
    use.names = FALSE
  )

  vals <- as.character(unlist(totals, use.names = FALSE))
  n <- length(vals)
  if (n == 0) return(NULL)

  pad <- function(x, n) {
    if (length(x) == 0) return(rep(NA_character_, n))
    if (length(x) >= n) return(as.character(x[seq_len(n)]))
    c(as.character(x), rep(NA_character_, n - length(x)))
  }

  cat_name <- safe_chr(category[["name"]]) %|%
    safe_chr(category[["displayName"]]) %|%
    NA_character_

  num_val <- suppressWarnings(as.numeric(vals))

  tibble::tibble(
    season = season,
    team_id = as.integer(team_id),
    team_slug = team_meta$team_slug,
    team_abbreviation = team_meta$team_abbreviation,
    team_display_name = team_meta$team_display_name,
    team_short_display_name = team_meta$team_short_display_name,
    team_color = team_meta$team_color,
    team_alternate_color = team_meta$team_alternate_color,
    team_logo = team_meta$team_logo,
    category = cat_name,
    stat_label = pad(labels, n),
    stat_name = pad(names_, n),
    stat_display_name = pad(display_names, n),
    stat_description = pad(descriptions, n),
    display_value = vals,
    value = num_val
  )
}

extract_team_meta <- function(raw) {
  team <- raw[["team"]] %||% raw[["requestedTeam"]] %||% list()
  list(
    team_slug = safe_chr(team[["slug"]]),
    team_abbreviation = safe_chr(team[["abbreviation"]]),
    team_display_name = safe_chr(team[["displayName"]]),
    team_short_display_name = safe_chr(team[["shortDisplayName"]]),
    team_color = safe_chr(team[["color"]]),
    team_alternate_color = safe_chr(team[["alternateColor"]]),
    team_logo = safe_chr(
      team[["logo"]] %||% purrr::pluck(team, "logos", 1, "href")
    )
  )
}

parse_one_team <- function(season, team_id) {
  url <- glue::glue("{raw_base}/{season}/{team_id}.json")
  raw <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = FALSE),
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: skip team_stats {season}/{team_id}: {e$message}"
      )
      NULL
    }
  )
  if (is.null(raw)) return(NULL)

  categories <- raw[["categories"]] %||%
    raw[["statCategories"]] %||%
    raw[["splits"]][["categories"]] %||%
    raw[["results"]][["stats"]][["categories"]] %||%
    list()
  if (length(categories) == 0) return(NULL)

  meta <- extract_team_meta(raw)

  purrr::map_dfr(categories, function(cat) {
    parse_one_category(season, team_id, meta, cat)
  })
}

write_manifest_row <- function(season, row_count, source_endpoint) {
  manifest_path <- "wnba/team_season_stats/wnba_team_season_stats_in_data_repo.csv"
  ifelse(
    !dir.exists(file.path("wnba/team_season_stats")),
    dir.create(file.path("wnba/team_season_stats"), recursive = TRUE),
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

build_season_team_stats <- function(y) {
  team_ids <- list_team_ids(y)
  if (length(team_ids) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no team_stats in raw repo for {y}; skipping"
    )
    return(invisible(NULL))
  }

  cli::cli_progress_step(
    msg = "Compiling {y} ESPN WNBA team season stats ({length(team_ids)} teams)",
    msg_done = "Compiled {y} ESPN WNBA team season stats!"
  )

  stats <- purrr::map_dfr(team_ids, function(t) {
    tryCatch(
      parse_one_team(y, t),
      error = function(e) {
        cli::cli_alert_warning(
          "{Sys.time()}: team_stats issue for {t}: {e$message}"
        )
        NULL
      }
    )
  })

  if (nrow(stats) == 0) {
    cli::cli_alert_warning(
      "{Sys.time()}: no team_season_stats rows parsed for {y}"
    )
    return(invisible(NULL))
  }

  stats <- stats %>%
    dplyr::distinct() %>%
    wehoop:::make_wehoop_data(
      "ESPN WNBA Team Season Stats from wehoop data repository",
      Sys.time()
    )

  ifelse(
    !dir.exists(file.path("wnba/team_season_stats")),
    dir.create(file.path("wnba/team_season_stats"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/team_season_stats/rds")),
    dir.create(file.path("wnba/team_season_stats/rds"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/team_season_stats/parquet")),
    dir.create(
      file.path("wnba/team_season_stats/parquet"),
      recursive = TRUE
    ),
    FALSE
  )

  saveRDS(
    stats,
    glue::glue("wnba/team_season_stats/rds/team_season_stats_{y}.rds")
  )
  arrow::write_parquet(
    stats,
    glue::glue(
      "wnba/team_season_stats/parquet/team_season_stats_{y}.parquet"
    ),
    compression = "zstd",
    compression_level = 22
  )

  cli::cli_progress_step(
    msg = "Updating {y} ESPN WNBA Team Season Stats GitHub Release",
    msg_done = "Updated {y} ESPN WNBA Team Season Stats GitHub Release!"
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
    data_frame = stats,
    file_name = glue::glue("team_season_stats_{y}"),
    sportsdataverse_type = "team season stats data",
    release_tag = "espn_wnba_team_season_stats",
    pkg_function = "wehoop::load_wnba_team_stats()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  write_manifest_row(
    season = y,
    row_count = nrow(stats),
    source_endpoint = glue::glue("{raw_base}/{y}/<team_id>.json")
  )

  rm(stats)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, function(y) {
  tryCatch(
    build_season_team_stats(y),
    error = function(e) {
      cli::cli_alert_danger(
        "{Sys.time()}: team_season_stats season {y} failed: {e$message}"
      )
    }
  )
})
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
tryCatch({
  source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
  upload_wnba_manifest(
    manifest_path        = "wnba/team_season_stats/wnba_team_season_stats_in_data_repo.csv",
    release_tag          = "espn_wnba_team_season_stats",
    file_name            = "wnba_team_season_stats_in_data_repo",
    sportsdataverse_type = "team season stats manifest",
    pkg_function         = "wehoop::load_wnba_team_stats_manifest()"
  )
}, error = function(e) {
  cli::cli_alert_warning(
    "{Sys.time()}: team_season_stats manifest upload failed (non-fatal): {e$message}"
  )
})

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
