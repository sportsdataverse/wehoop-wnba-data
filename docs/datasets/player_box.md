# `player_box`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_03_player_box_creation.py`](../../python/espn_wnba_03_player_box_creation.py) |
| **Release tag** | [`espn_wnba_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_boxscores) |
| **File stem** | `player_box_{season}.{parquet,csv,rds}` |
| **Seasons built** | — |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_wnba.yml` — cron 07:00 UTC in season, plus `repository_dispatch` from `wehoop-wnba-raw`. Runs `scripts/daily_wnba_data_processor.sh` (Python build, crosswalks included). Draft refreshes annually via `annual_wnba_draft.yml`; rosters additionally refresh weekly via `weekly_wnba.yml`.

## Columns

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game id (Int64 canonical join key across pbp, box, rosters, officials). |
| `season` | Int64 | Season year (the year the season ends in; WNBA seasons are single-year). |
| `season_type` | Int64 | ESPN season type (2 regular season, 3 postseason). |
| `game_date` | Date | Game date (America/New_York). |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game tip-off datetime (America/New_York). |
| `athlete_id` | Int64 | ESPN athlete id for the row's player. |
| `athlete_display_name` | String | Player display name. |
| `team_id` | Int64 | ESPN team id for the row's team. |
| `team_name` | String | Team nickname. |
| `team_location` | String | Team market/location. |
| `team_short_display_name` | String | Team short display name. |
| `minutes` | Float64 | Minutes played. |
| `field_goals_made` | Int64 | Field goals made. |
| `field_goals_attempted` | Int64 | Field goals attempted. |
| `three_point_field_goals_made` | Int64 | Three-point field goals made. |
| `three_point_field_goals_attempted` | Int64 | Three-point field goals attempted. |
| `free_throws_made` | Int64 | Free throws made. |
| `free_throws_attempted` | Int64 | Free throws attempted. |
| `offensive_rebounds` | Int64 | Offensive rebounds. |
| `defensive_rebounds` | Int64 | Defensive rebounds. |
| `rebounds` | Int64 | Total rebounds. |
| `assists` | Int64 | Assists. |
| `steals` | Int64 | Steals. |
| `blocks` | Int64 | Blocked shots. |
| `turnovers` | Int64 | Turnovers. |
| `fouls` | Int64 | Personal fouls. |
| `plus_minus` | String | Plus-minus while on the floor. |
| `points` | Int64 | Points scored. |
| `starter` | Boolean | True when the player started the game. |
| `ejected` | Boolean | True when the player was ejected. |
| `did_not_play` | Boolean | True when the player was on the roster but did not play. |
| `reason` | String | Reason string attached to the row (e.g. DNP reason). |
| `active` | Boolean | True when the athlete is currently active. |
| `athlete_jersey` | String | Player jersey number. |
| `athlete_short_name` | String | Player short name (`F. Lastname`). |
| `athlete_headshot_href` | String | Player headshot URL. |
| `athlete_position_name` | String | Player position name. |
| `athlete_position_abbreviation` | String | Player position abbreviation. |
| `team_display_name` | String | Team full display name. |
| `team_uid` | String | ESPN universal id string for the team. |
| `team_slug` | String | URL-safe ESPN slug for the team. |
| `team_logo` | String | Team logo URL. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_color` | String | Team primary color (hex). |
| `team_alternate_color` | String | Team alternate color (hex). |
| `home_away` | String | Whether the row team was home or away. |
| `team_winner` | Boolean | True when the row team won the game. |
| `team_score` | Int64 | Row team's score after the play (or team total in box frames). |
| `opponent_team_id` | Int64 | ESPN team id of the opposing team. |
| `opponent_team_name` | String | Opponent team nickname. |
| `opponent_team_location` | String | Opponent team market/location. |
| `opponent_team_display_name` | String | Opponent team full display name. |
| `opponent_team_abbreviation` | String | Opponent team abbreviation. |
| `opponent_team_logo` | String | Opponent logo URL. |
| `opponent_team_color` | String | Opponent primary color (hex). |
| `opponent_team_alternate_color` | String | Opponent alternate color (hex). |
| `opponent_team_score` | Int64 | Opponent final score. |

## Coverage

_No build manifest yet._
