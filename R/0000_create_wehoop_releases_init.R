
#--- ESPN WBB Data -----
piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_womens_college_basketball_schedules",
  name = "espn_womens_college_basketball_schedules",
  body = "NCAA Women's College Basketball Schedules Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_womens_college_basketball_team_boxscores",
  name = "espn_womens_college_basketball_team_boxscores",
  body = "NCAA Women's College Basketball Team Boxscores Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_womens_college_basketball_player_boxscores",
  name = "espn_womens_college_basketball_player_boxscores",
  body = "NCAA Women's College Basketball Player Boxscores Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)


piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_womens_college_basketball_pbp",
  name = "espn_womens_college_basketball_pbp",
  body = "NCAA Women's College Basketball Play-by-Play Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)

#--- ESPN WNBA Data -----

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_wnba_schedules",
  name = "espn_wnba_schedules",
  body = "WNBA Schedules Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_wnba_team_boxscores",
  name = "espn_wnba_team_boxscores",
  body = "WNBA Team Boxscores Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_wnba_player_boxscores",
  name = "espn_wnba_player_boxscores",
  body = "WNBA Player Boxscores Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)


piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "espn_wnba_pbp",
  name = "espn_wnba_pbp",
  body = "WNBA Play-by-Play Data (from ESPN)",
  .token = Sys.getenv("GITHUB_PAT")
)


#--- WNBA Stats Data -----

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "wnba_stats_schedules",
  name = "wnba_stats_schedules",
  body = "WNBA Schedules Data (from stats.wnba.com)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "wnba_stats_team_boxscores",
  name = "wnba_stats_team_boxscores",
  body = "WNBA Team Boxscores Data (from stats.wnba.com)",
  .token = Sys.getenv("GITHUB_PAT")
)

piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "wnba_stats_player_boxscores",
  name = "wnba_stats_player_boxscores",
  body = "WNBA Player Boxscores Data (from stats.wnba.com)",
  .token = Sys.getenv("GITHUB_PAT")
)


piggyback::pb_release_create(
  repo = "sportsdataverse/sportsdataverse-data",
  tag = "wnba_stats_pbp",
  name = "wnba_stats_pbp",
  body = "WNBA Play-by-Play Data (from stats.wnba.com)",
  .token = Sys.getenv("GITHUB_PAT")
)
