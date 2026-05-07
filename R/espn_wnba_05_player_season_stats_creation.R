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

raw_base <- "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/player_season_stats/json"
gh_api_base <- "https://api.github.com/repos/sportsdataverse/wehoop-wnba-raw/contents/wnba/player_season_stats/json"

# --- helpers ---------------------------------------------------------------

safe_chr <- function(x) {
  if (is.null(x)) return(NA_character_)
  if (length(x) == 0) return(NA_character_)
  as.character(x[[1]])
}

# Coalesce-style for character scalars (NA / "" treated as missing)
`%|%` <- function(a, b) {
  if (is.null(a) || length(a) == 0) return(b)
  if (is.na(a) || !nzchar(a)) return(b)
  a
}

list_athlete_ids <- function(season) {
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
      message(glue::glue(
        "{Sys.time()}: Could not list player_season_stats for {season}: {e$message}"
      ))
      NULL
    }
  )
  if (is.null(resp) || length(resp) == 0) return(integer())
  if (!is.data.frame(resp) || !"name" %in% colnames(resp)) return(integer())
  ids <- sub("\\.json$", "", resp$name)
  suppressWarnings(as.integer(ids[grepl("^[0-9]+$", ids)]))
}

parse_one_category <- function(season, athlete_id, athlete_meta, category) {
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
    athlete_id = as.integer(athlete_id),
    athlete_display_name = athlete_meta$display_name,
    athlete_first_name = athlete_meta$first_name,
    athlete_last_name = athlete_meta$last_name,
    athlete_position_abbreviation = athlete_meta$position_abbreviation,
    athlete_jersey = athlete_meta$jersey,
    team_id = athlete_meta$team_id,
    team_display_name = athlete_meta$team_display_name,
    category = cat_name,
    stat_label = pad(labels, n),
    stat_name = pad(names_, n),
    stat_display_name = pad(display_names, n),
    stat_description = pad(descriptions, n),
    display_value = vals,
    value = num_val
  )
}

extract_athlete_meta <- function(raw) {
  athlete <- raw[["athlete"]] %||% raw[["requestedAthlete"]] %||% list()
  team <- raw[["team"]] %||%
    purrr::pluck(athlete, "team") %||%
    list()
  list(
    display_name = safe_chr(athlete[["displayName"]]) %|%
      safe_chr(athlete[["fullName"]]),
    first_name = safe_chr(athlete[["firstName"]]),
    last_name = safe_chr(athlete[["lastName"]]),
    position_abbreviation = safe_chr(
      athlete[["position"]][["abbreviation"]]
    ),
    jersey = safe_chr(athlete[["jersey"]]),
    team_id = suppressWarnings(as.integer(safe_chr(team[["id"]]))),
    team_display_name = safe_chr(team[["displayName"]])
  )
}

parse_one_athlete <- function(season, athlete_id) {
  url <- glue::glue("{raw_base}/{season}/{athlete_id}.json")
  raw <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = FALSE),
    error = function(e) {
      message(glue::glue(
        "{Sys.time()}: skip player_season_stats {season}/{athlete_id}: {e$message}"
      ))
      NULL
    }
  )
  if (is.null(raw)) return(NULL)

  categories <- raw[["categories"]] %||%
    raw[["statCategories"]] %||%
    raw[["splits"]][["categories"]] %||%
    list()
  if (length(categories) == 0) return(NULL)

  meta <- extract_athlete_meta(raw)

  purrr::map_dfr(categories, function(cat) {
    parse_one_category(season, athlete_id, meta, cat)
  })
}

# --- main loop -------------------------------------------------------------

build_season_player_stats <- function(y) {
  athlete_ids <- list_athlete_ids(y)
  if (length(athlete_ids) == 0) {
    message(glue::glue(
      "{Sys.time()}: no player_season_stats in raw repo for {y}; skipping"
    ))
    return(invisible(NULL))
  }

  cli::cli_progress_step(
    msg = "Compiling {y} ESPN WNBA player season stats ({length(athlete_ids)} athletes)",
    msg_done = "Compiled {y} ESPN WNBA player season stats!"
  )

  future::plan("multisession")
  stats <- furrr::future_map_dfr(
    athlete_ids,
    function(a) {
      tryCatch(
        parse_one_athlete(y, a),
        error = function(e) {
          message(glue::glue(
            "{Sys.time()}: player_season_stats issue for {a}: {e$message}"
          ))
          NULL
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE)
  )

  if (nrow(stats) == 0) {
    message(glue::glue(
      "{Sys.time()}: no player_season_stats rows parsed for {y}"
    ))
    return(invisible(NULL))
  }

  stats <- stats %>%
    dplyr::distinct() %>%
    wehoop:::make_wehoop_data(
      "ESPN WNBA Player Season Stats from wehoop data repository",
      Sys.time()
    )

  ifelse(
    !dir.exists(file.path("wnba/player_season_stats")),
    dir.create(file.path("wnba/player_season_stats"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/player_season_stats/rds")),
    dir.create(file.path("wnba/player_season_stats/rds"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/player_season_stats/parquet")),
    dir.create(
      file.path("wnba/player_season_stats/parquet"),
      recursive = TRUE
    ),
    FALSE
  )

  saveRDS(
    stats,
    glue::glue("wnba/player_season_stats/rds/player_season_stats_{y}.rds")
  )
  arrow::write_parquet(
    stats,
    glue::glue(
      "wnba/player_season_stats/parquet/player_season_stats_{y}.parquet"
    ),
    compression = "zstd",
    compression_level = 22
  )

  cli::cli_progress_step(
    msg = "Updating {y} ESPN WNBA Player Season Stats GitHub Release",
    msg_done = "Updated {y} ESPN WNBA Player Season Stats GitHub Release!"
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
    data_frame = stats,
    file_name = glue::glue("player_season_stats_{y}"),
    sportsdataverse_type = "player season stats data",
    release_tag = "espn_wnba_player_season_stats",
    pkg_function = "wehoop::load_wnba_player_stats()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  # --- Manifest row append --------------------------------------------------
  manifest_path <- "wnba/player_season_stats/wnba_player_season_stats_in_data_repo.csv"
  manifest_row <- tibble::tibble(
    season           = as.integer(y),
    row_count        = as.integer(nrow(stats)),
    generated_at_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    source_endpoint  = glue::glue("{raw_base}/{y}/<athlete_id>.json")
  )
  if (file.exists(manifest_path)) {
    data.table::fwrite(manifest_row, manifest_path, append = TRUE)
  } else {
    data.table::fwrite(manifest_row, manifest_path)
  }

  rm(stats)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, build_season_player_stats)
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
tryCatch({
  source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
  upload_wnba_manifest(
    manifest_path        = "wnba/player_season_stats/wnba_player_season_stats_in_data_repo.csv",
    release_tag          = "espn_wnba_player_season_stats",
    file_name            = "wnba_player_season_stats_in_data_repo",
    sportsdataverse_type = "player season stats manifest",
    pkg_function         = "wehoop::load_wnba_player_stats_manifest()"
  )
}, error = function(e) {
  cli::cli_alert_warning(
    "{Sys.time()}: player_season_stats manifest upload failed (non-fatal): {e$message}"
  )
})

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
