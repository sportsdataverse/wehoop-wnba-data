# wehoop-wnba-data

```mermaid
  graph LR;
    A[wehoop-wnba-raw]-->B[wehoop-wnba-data];
    B[wehoop-wnba-data]-->C1[espn_wnba_pbp];
    B[wehoop-wnba-data]-->C2[espn_wnba_team_boxscores];
    B[wehoop-wnba-data]-->C3[espn_wnba_player_boxscores];

```

## wehoop ESPN WNBA workflow diagram

```mermaid
flowchart TB;
    subgraph A[wehoop-wnba-raw];
        direction TB;
        A1[python/scrape_wnba_schedules.py]-->A2[python/scrape_wnba_json.py];
    end;

    subgraph B[wehoop-wnba-data];
        direction TB;
        B1[R/espn_wnba_01_pbp_creation.R]-->B2[R/espn_wnba_02_team_box_creation.R];
        B2[R/espn_wnba_02_team_box_creation.R]-->B3[R/espn_wnba_03_player_box_creation.R];
    end;

    subgraph C[sportsdataverse Releases];
        direction TB;
        C1[espn_wnba_pbp];
        C2[espn_wnba_team_boxscores];
        C3[espn_wnba_player_boxscores];
    end;

    A-->B;
    B-->C1;
    B-->C2;
    B-->C3;

```

## Women's Basketball Data Releases

[ESPN Women's College Basketball Schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_schedules)

[ESPN Women's College Basketball PBP](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_pbp)

[ESPN Women's College Basketball Team Boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_team_boxscores)

[ESPN Women's College Basketball Player Boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_player_boxscores)

[ESPN WNBA Schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_schedules)

[ESPN WNBA PBP](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_pbp)

[ESPN WNBA Team Boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_boxscores)

[ESPN WNBA Player Boxscores](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_boxscores)


## Data Repositories

[wehoop-wnba-raw data repository (source: ESPN)](https://github.com/sportsdataverse/wehoop-wnba-raw)

[wehoop-wnba-data repository (source: ESPN)](https://github.com/sportsdataverse/wehoop-wnba-data)

[wehoop-wnba-stats-data Repo (source: NBA Stats)](https://github.com/sportsdataverse/wehoop-wnba-stats-data)

[wehoop-wbb-raw data repository (source: ESPN)](https://github.com/sportsdataverse/wehoop-wbb-raw)

[wehoop-wbb-data repository (source: ESPN)](https://github.com/sportsdataverse/wehoop-wbb-data)
