rm(list = ls())
gcol <- gc()
lib_path <- Sys.getenv("R_LIBS")
if (!requireNamespace("pacman", quietly = TRUE)) {
  install.packages("pacman", lib = Sys.getenv("R_LIBS"), repos = "http://cran.us.r-project.org")
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(qs, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(optparse, lib.loc = lib_path)))

option_list <- list(
  make_option(c("-s", "--start_year"),
              action = "store",
              default = wehoop:::most_recent_wnba_season(),
              type = "integer",
              help = "Start year of the seasons to process"),
  make_option(c("-e", "--end_year"),
              action = "store",
              default = wehoop:::most_recent_wnba_season(),
              type = "integer",
              help = "End year of the seasons to process")
)
opt <- parse_args(OptionParser(option_list = option_list))
options(stringsAsFactors = FALSE)
options(scipen = 999)
years_vec <- opt$s:opt$e
# --- compile into player_box_{year}.parquet ---------

wnba_player_box_games <- function(y) {
  espn_df <- data.frame()
  player_box_list <- list.files(path = glue::glue("wnba/json/final/"))
  sched <- data.table::fread(paste0("wnba/schedules/csv/wnba_schedule_", y, ".csv"))
  player_box_game_ids <- as.integer(gsub(".json", "", player_box_list))
  season_player_box_list <- sched %>%
    dplyr::filter(.data$game_id %in% player_box_game_ids) %>%
    dplyr::pull("game_id")

  cli::cli_progress_step(msg = "Compiling {y} ESPN WNBA Player Boxscores ({length(season_player_box_list)} games)",
                         msg_done = "Compiled {y} ESPN WNBA Player Boxscores!")

  future::plan("multisession")
  espn_df <- furrr::future_map_dfr(season_player_box_list, function(x) {
    tryCatch(
      expr = {
        resp <- glue::glue("wnba/json/final/{x}.json")
        player_box_score <- wehoop:::helper_espn_wnba_player_box(resp)
        return(player_box_score)
      },
      error = function(e) {
         message(glue::glue("{Sys.time()}: Player box score data for {x} issue!"))
      }
    )
  }, .options = furrr::furrr_options(seed = TRUE))

  cli::cli_progress_step(msg = "Updating {y} ESPN WNBA Player Boxscores GitHub Release",
                         msg_done = "Updated {y} ESPN WNBA Player Boxscores GitHub Release!")

  if (nrow(espn_df) > 1) {

    espn_df <- espn_df %>%
      dplyr::arrange(dplyr::desc(.data$game_date)) %>%
      wehoop:::make_wehoop_data("ESPN WNBA Player Boxscores from wehoop data repository", Sys.time())

    ifelse(!dir.exists(file.path("wnba/player_box")), dir.create(file.path("wnba/player_box")), FALSE)

    ifelse(!dir.exists(file.path("wnba/player_box/csv")), dir.create(file.path("wnba/player_box/csv")), FALSE)
    data.table::fwrite(espn_df, file = paste0("wnba/player_box/csv/player_box_", y, ".csv.gz"))

    ifelse(!dir.exists(file.path("wnba/player_box/rds")), dir.create(file.path("wnba/player_box/rds")), FALSE)
    saveRDS(espn_df, glue::glue("wnba/player_box/rds/player_box_{y}.rds"))

    ifelse(!dir.exists(file.path("wnba/player_box/parquet")), dir.create(file.path("wnba/player_box/parquet")), FALSE)
    arrow::write_parquet(espn_df, glue::glue("wnba/player_box/parquet/player_box_{y}.parquet"))

    sportsdataversedata::sportsdataverse_save(
      data_frame = espn_df,
      file_name =  glue::glue("player_box_{y}"),
      sportsdataverse_type = "player boxscores data",
      release_tag = "espn_wnba_player_boxscores",
      file_types = c("rds", "csv", "parquet"),
      .token = Sys.getenv("GITHUB_PAT")
    )
  }

  sched <- sched %>%
    dplyr::mutate(
      game_id = as.integer(.data$id),
      id = as.integer(.data$id),
      status_display_clock = as.character(.data$status_display_clock),
      game_date_time = lubridate::ymd_hm(substr(.data$date, 1, nchar(.data$date) - 1)) %>%
        lubridate::with_tz(tzone = "America/New_York"),
      game_date = as.Date(substr(.data$game_date_time, 1, 10)))

  if (nrow(espn_df) > 0) {

    sched <- sched %>%
      dplyr::mutate(
        player_box = ifelse(.data$game_id %in% unique(espn_df$game_id), TRUE, FALSE))

  } else {

    sched$player_box <- FALSE

  }

  final_sched <- sched %>%
    dplyr::distinct() %>%
    dplyr::arrange(dplyr::desc(.data$date))

  final_sched <- final_sched %>%
    wehoop:::make_wehoop_data("ESPN WNBA Schedule from wehoop data repository", Sys.time())

  sportsdataversedata::sportsdataverse_save(
    data_frame = final_sched,
    file_name =  glue::glue("wnba_schedule_{y}"),
    sportsdataverse_type = "schedule data",
    release_tag = "espn_wnba_schedules",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )

  data.table::fwrite(final_sched, paste0("wnba/schedules/csv/wnba_schedule_", y, ".csv"))
  saveRDS(final_sched, glue::glue("wnba/schedules/rds/wnba_schedule_{y}.rds"))
  arrow::write_parquet(final_sched, glue::glue("wnba/schedules/parquet/wnba_schedule_{y}.parquet"))
  rm(sched)
  rm(final_sched)

  rm(espn_df)
  gc()
  return(NULL)
}

all_games <- purrr::map(years_vec, function(y) {
  wnba_player_box_games(y)
  return(NULL)
})


cli::cli_progress_step(msg = "Compiling ESPN WNBA master schedule",
                       msg_done = "ESPN WNBA master schedule compiled and written to disk")

sched_list <- list.files(path = glue::glue("wnba/schedules/csv/"))
sched_g <-  purrr::map_dfr(sched_list, function(x) {
  sched <- data.table::fread(paste0("wnba/schedules/csv/", x)) %>%
    dplyr::mutate(
      status_display_clock = as.character(.data$status_display_clock)
    )
  return(sched)
})

sched_g <- sched_g %>%
  wehoop:::make_wehoop_data("ESPN WNBA Schedule from wehoop data repository", Sys.time())

data.table::fwrite(sched_g %>%
                     dplyr::arrange(dplyr::desc(.data$date)), "wnba/wnba_schedule_master.csv")
data.table::fwrite(sched_g %>%
                     dplyr::filter(.data$PBP == TRUE) %>%
                     dplyr::arrange(dplyr::desc(.data$date)), "wnba/wnba_games_in_data_repo.csv")

arrow::write_parquet(sched_g %>%
                       dplyr::arrange(dplyr::desc(.data$date)), glue::glue("wnba/wnba_schedule_master.parquet"))
arrow::write_parquet(sched_g %>%
                       dplyr::filter(.data$PBP == TRUE) %>%
                       dplyr::arrange(dplyr::desc(.data$date)), "wnba/wnba_games_in_data_repo.parquet")

cli::cli_progress_message("")

rm(all_games)
rm(sched_g)
rm(sched_list)
rm(years_vec)
gcol <- gc()