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

raw_base <- "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/team_rosters/json"
gh_api_base <- "https://api.github.com/repos/sportsdataverse/wehoop-wnba-raw/contents/wnba/team_rosters/json"

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
      message(glue::glue(
        "{Sys.time()}: Could not list team_rosters for {season}: {e$message}"
      ))
      NULL
    }
  )
  if (is.null(resp) || length(resp) == 0) return(integer())
  if (!is.data.frame(resp) || !"name" %in% colnames(resp)) return(integer())
  ids <- sub("\\.json$", "", resp$name)
  suppressWarnings(as.integer(ids[grepl("^[0-9]+$", ids)]))
}

flatten_athletes <- function(raw) {
  athletes <- raw[["athletes"]] %||% raw[["items"]] %||% list()
  if (length(athletes) == 0) return(list())
  has_buckets <- is.list(athletes[[1]]) && !is.null(athletes[[1]][["items"]])
  if (has_buckets) {
    return(purrr::flatten(purrr::map(athletes, "items")))
  }
  athletes
}

parse_one_team <- function(season, team_id) {
  url <- glue::glue("{raw_base}/{season}/{team_id}.json")
  raw <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = FALSE),
    error = function(e) {
      message(glue::glue(
        "{Sys.time()}: skip rosters {season}/{team_id}: {e$message}"
      ))
      NULL
    }
  )
  if (is.null(raw)) return(NULL)

  flat <- flatten_athletes(raw)
  if (length(flat) == 0) return(NULL)

  team_meta <- raw[["team"]] %||% list()

  tibble::tibble(
    season = season,
    team_id = as.integer(team_id),
    team_slug = safe_chr(team_meta[["slug"]]),
    team_abbreviation = safe_chr(team_meta[["abbreviation"]]),
    team_display_name = safe_chr(team_meta[["displayName"]]),
    team_short_display_name = safe_chr(team_meta[["shortDisplayName"]]),
    team_color = safe_chr(team_meta[["color"]]),
    team_alternate_color = safe_chr(team_meta[["alternateColor"]]),
    team_logo = safe_chr(
      team_meta[["logo"]] %||%
        purrr::pluck(team_meta, "logos", 1, "href")
    ),
    athlete_id = purrr::map_chr(flat, ~ safe_chr(.x[["id"]])),
    uid = purrr::map_chr(flat, ~ safe_chr(.x[["uid"]])),
    guid = purrr::map_chr(flat, ~ safe_chr(.x[["guid"]])),
    full_name = purrr::map_chr(flat, function(p) {
      safe_chr(p[["fullName"]]) %|% safe_chr(p[["displayName"]])
    }),
    display_name = purrr::map_chr(flat, ~ safe_chr(.x[["displayName"]])),
    short_name = purrr::map_chr(flat, ~ safe_chr(.x[["shortName"]])),
    first_name = purrr::map_chr(flat, ~ safe_chr(.x[["firstName"]])),
    last_name = purrr::map_chr(flat, ~ safe_chr(.x[["lastName"]])),
    jersey = purrr::map_chr(flat, ~ safe_chr(.x[["jersey"]])),
    position_abbreviation = purrr::map_chr(
      flat,
      ~ safe_chr(.x[["position"]][["abbreviation"]])
    ),
    position_name = purrr::map_chr(
      flat,
      ~ safe_chr(.x[["position"]][["displayName"]])
    ),
    position_id = purrr::map_chr(
      flat,
      ~ safe_chr(.x[["position"]][["id"]])
    ),
    height = purrr::map_chr(flat, function(p) {
      safe_chr(p[["displayHeight"]]) %|% safe_chr(p[["height"]])
    }),
    weight = purrr::map_chr(flat, function(p) {
      safe_chr(p[["displayWeight"]]) %|% safe_chr(p[["weight"]])
    }),
    age = purrr::map_chr(flat, ~ safe_chr(.x[["age"]])),
    date_of_birth = purrr::map_chr(flat, ~ safe_chr(.x[["dateOfBirth"]])),
    birth_place_city = purrr::map_chr(
      flat,
      ~ safe_chr(.x[["birthPlace"]][["city"]])
    ),
    birth_place_state = purrr::map_chr(
      flat,
      ~ safe_chr(.x[["birthPlace"]][["state"]])
    ),
    birth_place_country = purrr::map_chr(
      flat,
      ~ safe_chr(.x[["birthPlace"]][["country"]])
    ),
    experience_years = purrr::map_chr(
      flat,
      ~ safe_chr(.x[["experience"]][["years"]])
    ),
    experience_display_value = purrr::map_chr(
      flat,
      ~ safe_chr(.x[["experience"]][["displayValue"]])
    ),
    headshot_href = purrr::map_chr(
      flat,
      ~ safe_chr(.x[["headshot"]][["href"]])
    ),
    headshot_alt = purrr::map_chr(
      flat,
      ~ safe_chr(.x[["headshot"]][["alt"]])
    ),
    link_web = purrr::map_chr(flat, function(p) {
      links <- p[["links"]]
      if (is.null(links) || length(links) == 0) return(NA_character_)
      safe_chr(links[[1]][["href"]])
    }),
    status_id = purrr::map_chr(flat, ~ safe_chr(.x[["status"]][["id"]])),
    status_name = purrr::map_chr(flat, ~ safe_chr(.x[["status"]][["name"]])),
    status_type = purrr::map_chr(flat, ~ safe_chr(.x[["status"]][["type"]]))
  )
}

# --- main loop -------------------------------------------------------------

build_season_rosters <- function(y) {
  team_ids <- list_team_ids(y)
  if (length(team_ids) == 0) {
    message(glue::glue(
      "{Sys.time()}: no team rosters in raw repo for {y}; skipping"
    ))
    return(invisible(NULL))
  }

  cli::cli_progress_step(
    msg = "Compiling {y} ESPN WNBA rosters ({length(team_ids)} teams)",
    msg_done = "Compiled {y} ESPN WNBA rosters!"
  )

  rosters <- purrr::map_dfr(team_ids, function(t) {
    parse_one_team(y, t)
  })

  if (nrow(rosters) == 0) {
    message(glue::glue("{Sys.time()}: no roster rows parsed for {y}"))
    return(invisible(NULL))
  }

  rosters <- rosters %>%
    dplyr::distinct() %>%
    wehoop:::make_wehoop_data(
      "ESPN WNBA Rosters from wehoop data repository",
      Sys.time()
    )

  ifelse(
    !dir.exists(file.path("wnba/rosters")),
    dir.create(file.path("wnba/rosters"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/rosters/rds")),
    dir.create(file.path("wnba/rosters/rds"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/rosters/parquet")),
    dir.create(file.path("wnba/rosters/parquet"), recursive = TRUE),
    FALSE
  )

  saveRDS(rosters, glue::glue("wnba/rosters/rds/rosters_{y}.rds"))
  arrow::write_parquet(
    rosters,
    glue::glue("wnba/rosters/parquet/rosters_{y}.parquet"),
    compression = "zstd",
    compression_level = 22
  )

  cli::cli_progress_step(
    msg = "Updating {y} ESPN WNBA Rosters GitHub Release",
    msg_done = "Updated {y} ESPN WNBA Rosters GitHub Release!"
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
    data_frame = rosters,
    file_name = glue::glue("rosters_{y}"),
    sportsdataverse_type = "rosters data",
    release_tag = "espn_wnba_rosters",
    pkg_function = "wehoop::load_wnba_rosters()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  # --- Manifest row append --------------------------------------------------
  manifest_path <- "wnba/rosters/wnba_rosters_in_data_repo.csv"
  manifest_row <- tibble::tibble(
    season           = as.integer(y),
    row_count        = as.integer(nrow(rosters)),
    generated_at_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    source_endpoint  = glue::glue("{raw_base}/{y}/<team_id>.json")
  )
  if (file.exists(manifest_path)) {
    data.table::fwrite(manifest_row, manifest_path, append = TRUE)
  } else {
    data.table::fwrite(manifest_row, manifest_path)
  }

  rm(rosters)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, build_season_rosters)
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
tryCatch({
  source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
  upload_wnba_manifest(
    manifest_path        = "wnba/rosters/wnba_rosters_in_data_repo.csv",
    release_tag          = "espn_wnba_rosters",
    file_name            = "wnba_rosters_in_data_repo",
    sportsdataverse_type = "rosters manifest",
    pkg_function         = "wehoop::load_wnba_rosters_manifest()"
  )
}, error = function(e) {
  cli::cli_alert_warning(
    "{Sys.time()}: rosters manifest upload failed (non-fatal): {e$message}"
  )
})

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
