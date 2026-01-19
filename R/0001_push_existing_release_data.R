lib_path <- Sys.getenv("R_LIBS")
if (!requireNamespace("pacman", quietly = TRUE)){
  install.packages("pacman",lib=Sys.getenv("R_LIBS"), repos="http://cran.us.r-project.org")
}
suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(magrittr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(progressr, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(data.table, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(arrow, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc=lib_path)))
suppressPackageStartupMessages(suppressMessages(library(optparse, lib.loc=lib_path)))



sched_list <- list.files(path = glue::glue("wbb/schedules/rds/"))
sched_g <-  purrr::map(sched_list, function(x) {
  sched <- readRDS(paste0("wbb/schedules/rds/", x)) %>%
    dplyr::mutate(
      id = as.integer(.data$id),
      game_id = as.integer(.data$game_id),
      status_display_clock = as.character(.data$status_display_clock)
    )

  sched <- sched %>%
    wehoop:::make_wehoop_data("ESPN WBB Schedule from wehoop data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = sched,
    file_name =  glue::glue("wbb_schedule_{y}"),
    sportsdataverse_type = "schedule data",
    release_tag = "espn_womens_college_basketball_schedules",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(sched_g)

pbp_list <- list.files(path = glue::glue("wbb/pbp/rds/"))
pbp_g <-  purrr::map(pbp_list, function(x) {
  pbp <- readRDS(paste0("wbb/pbp/rds/", x))

  pbp <- pbp %>%
      wehoop:::make_wehoop_data("ESPN WBB Play-by-Play from wehoop data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = pbp,
    file_name =  glue::glue("play_by_play_{y}"),
    sportsdataverse_type = "Play-by-Play data",
    release_tag = "espn_womens_college_basketball_pbp",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(pbp_g)

team_box_list <- list.files(path = glue::glue("wbb/team_box/rds/"))
team_box_g <-  purrr::map(team_box_list, function(x) {
  team_box <- readRDS(paste0("wbb/team_box/rds/", x))
  team_box <- team_box %>%
      wehoop:::make_wehoop_data("ESPN WBB Team Boxscores from wehoop data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = team_box,
    file_name =  glue::glue("team_box_{y}"),
    sportsdataverse_type = "Team Boxscores data",
    release_tag = "espn_womens_college_basketball_team_boxscores",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})

rm(team_box_g)

player_box_list <- list.files(path = glue::glue("wbb/player_box/rds/"))
player_box_g <-  purrr::map(player_box_list, function(x) {
  player_box <- readRDS(paste0("wbb/player_box/rds/", x))
  player_box <- player_box %>%
      wehoop:::make_wehoop_data("ESPN WBB Player Boxscores from wehoop data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = player_box,
    file_name =  glue::glue("player_box_{y}"),
    sportsdataverse_type = "Player Boxscores data",
    release_tag = "espn_womens_college_basketball_player_boxscores",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})

rm(player_box_g)

sched_list <- list.files(path = glue::glue("wnba/schedules/rds/"))
sched_g <-  purrr::map(sched_list, function(x) {
  sched <- readRDS(paste0("wnba/schedules/rds/", x)) %>%
    dplyr::mutate(
      id = as.integer(.data$id),
      game_id = as.integer(.data$game_id),
      status_display_clock = as.character(.data$status_display_clock)
    )

  sched <- sched %>%
    wehoop:::make_wehoop_data("ESPN WNBA Schedule from wehoop data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = sched,
    file_name =  glue::glue("wnba_schedule_{y}"),
    sportsdataverse_type = "schedule data",
    release_tag = "espn_wnba_schedules",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(sched_g)

pbp_list <- list.files(path = glue::glue("wnba/pbp/rds/"))
pbp_g <-  purrr::map(pbp_list, function(x) {
  pbp <- readRDS(paste0("wnba/pbp/rds/", x))

  pbp <- pbp %>%
      wehoop:::make_wehoop_data("ESPN WNBA Play-by-Play from wehoop data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = pbp,
    file_name =  glue::glue("play_by_play_{y}"),
    sportsdataverse_type = "Play-by-Play data",
    release_tag = "espn_wnba_pbp",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})
rm(pbp_g)

team_box_list <- list.files(path = glue::glue("wnba/team_box/rds/"))
team_box_g <-  purrr::map(team_box_list, function(x) {
  team_box <- readRDS(paste0("wnba/team_box/rds/", x))
  team_box <- team_box %>%
      wehoop:::make_wehoop_data("ESPN WNBA Team Boxscores from wehoop data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = team_box,
    file_name =  glue::glue("team_box_{y}"),
    sportsdataverse_type = "Team Boxscores data",
    release_tag = "espn_wnba_team_boxscores",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})

rm(team_box_g)

player_box_list <- list.files(path = glue::glue("wnba/player_box/rds/"))
player_box_g <-  purrr::map(player_box_list, function(x) {
  player_box <- readRDS(paste0("wnba/player_box/rds/", x))
  player_box <- player_box %>%
      wehoop:::make_wehoop_data("ESPN WNBA Player Boxscores from wehoop data repository", Sys.time())
  y <- stringr::str_extract(x, "\\d+")
  sportsdataversedata::sportsdataverse_save(
    data_frame = player_box,
    file_name =  glue::glue("player_box_{y}"),
    sportsdataverse_type = "Player Boxscores data",
    release_tag = "espn_wnba_player_boxscores",
    file_types = c("rds", "csv", "parquet"),
    .token = Sys.getenv("GITHUB_PAT")
  )
})

rm(player_box_g)
