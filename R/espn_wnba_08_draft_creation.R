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

raw_base <- "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/draft/json"

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

# ESPN WNBA draft payload: top-level usually `rounds[]`, each with `picks[]`.
# Each pick carries `overall`, `pick`, `round`, plus nested `athlete{}` and
# `team{}` blocks. Some payloads flatten to `picks[]` at the top level.
parse_one_pick <- function(season, round_meta, pick) {
  athlete <- pick[["athlete"]] %||% list()
  team <- pick[["team"]] %||% list()
  college <- athlete[["college"]] %||%
    pick[["college"]] %||%
    list()

  tibble::tibble(
    season = season,
    round = suppressWarnings(as.integer(
      safe_chr(pick[["round"]]) %|% round_meta$round_number
    )),
    round_display_name = round_meta$round_display_name,
    pick = suppressWarnings(as.integer(safe_chr(pick[["pick"]]))),
    overall_pick = suppressWarnings(as.integer(
      safe_chr(pick[["overall"]]) %|% safe_chr(pick[["overallPick"]])
    )),
    pick_traded = safe_chr(pick[["traded"]]),
    pick_notes = safe_chr(pick[["notes"]]) %|% safe_chr(pick[["note"]]),
    athlete_id = suppressWarnings(as.integer(safe_chr(athlete[["id"]]))),
    athlete_uid = safe_chr(athlete[["uid"]]),
    athlete_guid = safe_chr(athlete[["guid"]]),
    athlete_first_name = safe_chr(athlete[["firstName"]]),
    athlete_last_name = safe_chr(athlete[["lastName"]]),
    athlete_full_name = safe_chr(athlete[["fullName"]]) %|%
      safe_chr(athlete[["displayName"]]),
    athlete_display_name = safe_chr(athlete[["displayName"]]),
    athlete_short_name = safe_chr(athlete[["shortName"]]),
    athlete_height = safe_chr(athlete[["displayHeight"]]) %|%
      safe_chr(athlete[["height"]]),
    athlete_weight = safe_chr(athlete[["displayWeight"]]) %|%
      safe_chr(athlete[["weight"]]),
    athlete_position_abbreviation = safe_chr(
      athlete[["position"]][["abbreviation"]]
    ),
    athlete_position_name = safe_chr(
      athlete[["position"]][["displayName"]]
    ),
    athlete_headshot_href = safe_chr(athlete[["headshot"]][["href"]]),
    college_id = suppressWarnings(as.integer(safe_chr(college[["id"]]))),
    college_name = safe_chr(college[["name"]]) %|%
      safe_chr(college[["displayName"]]),
    college_short_name = safe_chr(college[["shortName"]]),
    college_abbreviation = safe_chr(college[["abbreviation"]]),
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
    )
  )
}

write_manifest_row <- function(season, row_count, source_endpoint) {
  manifest_path <- "wnba/draft/wnba_draft_in_data_repo.csv"
  ifelse(
    !dir.exists(file.path("wnba/draft")),
    dir.create(file.path("wnba/draft"), recursive = TRUE),
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

build_season_draft <- function(y) {
  url <- glue::glue("{raw_base}/{y}.json")

  raw <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = FALSE),
    error = function(e) {
      cli::cli_alert_warning(
        "{Sys.time()}: skip draft {y}: {e$message}"
      )
      NULL
    }
  )
  if (is.null(raw)) return(invisible(NULL))

  cli::cli_progress_step(
    msg = "Compiling {y} ESPN WNBA draft",
    msg_done = "Compiled {y} ESPN WNBA draft!"
  )

  pieces <- list()
  rounds <- raw[["rounds"]] %||% list()
  if (length(rounds) > 0) {
    for (r in rounds) {
      rmeta <- list(
        round_number = safe_chr(r[["number"]]) %|% safe_chr(r[["round"]]),
        round_display_name = safe_chr(r[["displayName"]]) %|%
          safe_chr(r[["name"]])
      )
      picks <- r[["picks"]] %||% list()
      if (length(picks) > 0) {
        pieces <- c(pieces, list(purrr::map_dfr(picks, function(p) {
          parse_one_pick(y, rmeta, p)
        })))
      }
    }
  }

  flat_picks <- raw[["picks"]] %||% list()
  if (length(flat_picks) > 0) {
    rmeta <- list(round_number = NA_character_, round_display_name = NA_character_)
    pieces <- c(pieces, list(purrr::map_dfr(flat_picks, function(p) {
      parse_one_pick(y, rmeta, p)
    })))
  }

  draft <- if (length(pieces) > 0) {
    do.call(rbind, pieces)
  } else {
    NULL
  }

  if (is.null(draft) || nrow(draft) == 0) {
    cli::cli_alert_warning("{Sys.time()}: no draft picks parsed for {y}")
    return(invisible(NULL))
  }

  draft <- draft %>%
    dplyr::as_tibble() %>%
    dplyr::distinct() %>%
    dplyr::arrange(dplyr::across(dplyr::any_of(c("overall_pick", "round", "pick")))) %>%
    wehoop:::make_wehoop_data(
      "ESPN WNBA Draft from wehoop data repository",
      Sys.time()
    )

  ifelse(
    !dir.exists(file.path("wnba/draft")),
    dir.create(file.path("wnba/draft"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/draft/rds")),
    dir.create(file.path("wnba/draft/rds"), recursive = TRUE),
    FALSE
  )
  ifelse(
    !dir.exists(file.path("wnba/draft/parquet")),
    dir.create(file.path("wnba/draft/parquet"), recursive = TRUE),
    FALSE
  )

  saveRDS(draft, glue::glue("wnba/draft/rds/draft_{y}.rds"))
  arrow::write_parquet(
    draft,
    glue::glue("wnba/draft/parquet/draft_{y}.parquet"),
    compression = "zstd",
    compression_level = 22
  )

  cli::cli_progress_step(
    msg = "Updating {y} ESPN WNBA Draft GitHub Release",
    msg_done = "Updated {y} ESPN WNBA Draft GitHub Release!"
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
    data_frame = draft,
    file_name = glue::glue("draft_{y}"),
    sportsdataverse_type = "draft data",
    release_tag = "espn_wnba_draft",
    pkg_function = "wehoop::load_wnba_draft()",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  write_manifest_row(
    season = y,
    row_count = nrow(draft),
    source_endpoint = glue::glue("{raw_base}/{y}.json")
  )

  rm(draft)
  gc()
  invisible(NULL)
}

tictoc::tic()
purrr::walk(years_vec, function(y) {
  tryCatch(
    build_season_draft(y),
    error = function(e) {
      cli::cli_alert_danger(
        "{Sys.time()}: draft season {y} failed: {e$message}"
      )
    }
  )
})
tictoc::toc()

# --- Manifest upload (idempotent -- overwrites release asset on each run) ----
tryCatch({
  source(file.path("R", "manifest_upload_helper.R"), local = TRUE)
  upload_wnba_manifest(
    manifest_path        = "wnba/draft/wnba_draft_in_data_repo.csv",
    release_tag          = "espn_wnba_draft",
    file_name            = "wnba_draft_in_data_repo",
    sportsdataverse_type = "draft manifest",
    pkg_function         = "wehoop::load_wnba_draft_manifest()"
  )
}, error = function(e) {
  cli::cli_alert_warning(
    "{Sys.time()}: draft manifest upload failed (non-fatal): {e$message}"
  )
})

cli::cli_progress_message("")
rm(years_vec)
gcol <- gc()
