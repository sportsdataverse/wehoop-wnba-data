# `team_box`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_02_team_box_creation.py`](../../python/espn_wnba_02_team_box_creation.py) |
| **Release tag** | [`espn_wnba_team_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_boxscores) |
| **File stem** | `team_box_{season}.{parquet,csv,rds}` |
| **Seasons built** | — |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_wnba.yml` — cron 07:00 UTC in season, plus `repository_dispatch` from `wehoop-wnba-raw`. Runs `scripts/daily_wnba_data_processor.sh` (Python build + the R crosswalk tail). Draft refreshes annually via `annual_wnba_draft.yml`; rosters additionally refresh weekly via `weekly_wnba.yml`.

## Columns

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game id (Int64 canonical join key across pbp, box, rosters, officials). |
| `season` | Int64 | Season year (the year the season ends in; WNBA seasons are single-year). |
| `season_type` | Int64 | ESPN season type (2 regular season, 3 postseason). |
| `game_date` | Date | Game date (America/New_York). |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game tip-off datetime (America/New_York). |
| `team_id` | Int64 | ESPN team id for the row's team. |
| `team_uid` | String | ESPN universal id string for the team. |
| `team_slug` | String | URL-safe ESPN slug for the team. |
| `team_location` | String | Team market/location. |
| `team_name` | String | Team nickname. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team full display name. |
| `team_short_display_name` | String | Team short display name. |
| `team_color` | String | Team primary color (hex). |
| `team_alternate_color` | String | Team alternate color (hex). |
| `team_logo` | String | Team logo URL. |
| `team_home_away` | String | Whether the row team was home or away. |
| `team_score` | Int64 | Row team's score after the play (or team total in box frames). |
| `team_winner` | Boolean | True when the row team won the game. |
| `assists` | Int64 | Assists. |
| `blocks` | Int64 | Blocked shots. |
| `defensive_rebounds` | Int64 | Defensive rebounds. |
| `fast_break_points` | String | Fast-break points. |
| `field_goal_pct` | Float64 | Field goal percentage. |
| `field_goals_made` | Int64 | Field goals made. |
| `field_goals_attempted` | Int64 | Field goals attempted. |
| `flagrant_fouls` | Int64 | Flagrant fouls. |
| `fouls` | Int64 | Personal fouls. |
| `free_throw_pct` | Float64 | Free throw percentage. |
| `free_throws_made` | Int64 | Free throws made. |
| `free_throws_attempted` | Int64 | Free throws attempted. |
| `largest_lead` | String | Largest lead held. |
| `offensive_rebounds` | Int64 | Offensive rebounds. |
| `points_in_paint` | String | Points in the paint. |
| `steals` | Int64 | Steals. |
| `team_turnovers` | Int64 | Team (non-individual) turnovers. |
| `technical_fouls` | Int64 | Technical fouls. |
| `three_point_field_goal_pct` | Float64 | Three-point field goal percentage. |
| `three_point_field_goals_made` | Int64 | Three-point field goals made. |
| `three_point_field_goals_attempted` | Int64 | Three-point field goals attempted. |
| `total_rebounds` | Int64 | Total rebounds. |
| `total_technical_fouls` | Int64 | Team plus individual technical fouls. |
| `total_turnovers` | Int64 | Team plus individual turnovers. |
| `turnover_points` | String | Points scored off opponent turnovers. |
| `turnovers` | Int64 | Turnovers. |
| `opponent_team_id` | Int64 | ESPN team id of the opposing team. |
| `opponent_team_uid` | String | Opponent ESPN universal id. |
| `opponent_team_slug` | String | Opponent URL-safe slug. |
| `opponent_team_location` | String | Opponent team market/location. |
| `opponent_team_name` | String | Opponent team nickname. |
| `opponent_team_abbreviation` | String | Opponent team abbreviation. |
| `opponent_team_display_name` | String | Opponent team full display name. |
| `opponent_team_short_display_name` | String | Opponent team short display name. |
| `opponent_team_color` | String | Opponent primary color (hex). |
| `opponent_team_alternate_color` | String | Opponent alternate color (hex). |
| `opponent_team_logo` | String | Opponent logo URL. |
| `opponent_team_score` | Int64 | Opponent final score. |

## Coverage

_No build manifest yet._
