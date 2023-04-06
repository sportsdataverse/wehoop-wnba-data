lib_path <- Sys.getenv("R_LIBS")

suppressPackageStartupMessages(suppressMessages(library(dplyr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(httr, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(jsonlite, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(glue, lib.loc = lib_path)))
suppressPackageStartupMessages(suppressMessages(library(purrr, lib.loc = lib_path)))
get_proxy_bonanza_ips <- function(
    api_key = Sys.getenv("PROXY_BONANZA_KEY"),
    user_package = Sys.getenv("PROXY_BONANZA_USERPKG")){
  res <- httr::RETRY(
    "GET",
    glue::glue("https://proxybonanza.com/api/v1/userpackages/{user_package}.json"),
    httr::add_headers(Authorization = paste(api_key))) %>%
    httr::content(as = "text", encoding = "UTF-8")

  resp <- res %>%
    jsonlite::fromJSON() %>%
    purrr::pluck("data")

  login <- resp$login
  password <- resp$password
  ips <- resp$ippacks

  ips$login <- login
  ips$password <- password
  proxies <- ips %>%
    dplyr::select("ip","port_http","login", "password")
  return(proxies)
}

select_proxy <- function(proxies = get_proxy_bonanza_ips()) {
  proxy <- sample(proxies$ip, 1)          # pick a random proxy from the list above
  proxy_selected <- proxies %>%
    dplyr::filter(.data$ip == proxy)
  my_proxy <- httr::use_proxy(url = proxy_selected$ip,
                              port = proxy_selected$port,
                              username = proxy_selected$login,
                              password = proxy_selected$password)
  return(my_proxy)
}


rejoin_schedules <- function(df){
  df <- df %>%
    dplyr::mutate(
      HOME_AWAY = ifelse(stringr::str_detect(.data$MATCHUP,"@"),"AWAY","HOME")) %>%
    dplyr::select(-.data$WL,.data$MATCHUP)
  away_df <- df %>%
    dplyr::filter(.data$HOME_AWAY == "AWAY") %>%
    dplyr::select(-.data$HOME_AWAY) %>%
    dplyr::select(.data$SEASON_ID, .data$GAME_ID, .data$GAME_DATE, .data$MATCHUP, tidyr::everything())
  colnames(away_df)[5:ncol(away_df)]<-paste0("AWAY_", colnames(away_df)[5:ncol(away_df)])
  home_df <- df %>%
    dplyr::filter(.data$HOME_AWAY == "HOME") %>%
    dplyr::select(-.data$HOME_AWAY, -.data$MATCHUP) %>%
    dplyr::select(.data$SEASON_ID, .data$GAME_ID, .data$GAME_DATE,  tidyr::everything())
  colnames(home_df)[4:ncol(home_df)]<-paste0("HOME_", colnames(home_df)[4:ncol(home_df)])
  sched_df <- away_df %>%
    dplyr::left_join(home_df, by=c("GAME_ID", "SEASON_ID", "GAME_DATE"))
  return(sched_df)
}
