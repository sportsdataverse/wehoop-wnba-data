# `pbp`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_01_pbp_creation.py`](../../python/espn_wnba_01_pbp_creation.py) |
| **Release tag** | [`espn_wnba_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_pbp) |
| **File stem** | `play_by_play_{season}.{parquet,csv,rds}` |
| **Seasons built** | — |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_wnba.yml` — cron 07:00 UTC in season, plus `repository_dispatch` from `wehoop-wnba-raw`. Runs `scripts/daily_wnba_data_processor.sh` (Python build + the R crosswalk tail). Draft refreshes annually via `annual_wnba_draft.yml`; rosters additionally refresh weekly via `weekly_wnba.yml`.

## Columns

| col_name | type | description |
|---|---|---|
| `game_play_number` | Int64 | Running play number within the game (1-based). |
| `id` | Float64 | ESPN id of the row's entity (play id in pbp; game id in the schedule frame). |
| `sequence_number` | Int64 | ESPN play sequence number. |
| `type_id` | Int64 | ESPN season/competition type id (2 regular season, 3 postseason). |
| `type_text` | String | Play type name (`Jump Shot`, `Substitution`, ...). |
| `text` | String | Play narrative text. |
| `away_score` | Int64 | Away team final score. |
| `home_score` | Int64 | Home team final score. |
| `period_number` | Int64 | Period number (1-4, 5+ = overtime). |
| `period_display_value` | String | Period display string (`1st Quarter`, `OT`). |
| `clock_display_value` | String | Game clock display at the play (`7:42`). |
| `scoring_play` | Boolean | True when the play scored points. |
| `score_value` | Int64 | Points scored on the play (0-3). |
| `team_id` | Int64 | ESPN team id for the row's team. |
| `athlete_id_1` | Int64 | ESPN athlete id of the primary player credited on the play. |
| `athlete_id_2` | Int64 | ESPN athlete id of the secondary player on the play (e.g. assister, fouled player). |
| `athlete_id_3` | Int64 | ESPN athlete id of the tertiary player on the play. |
| `wallclock` | String | Wall-clock timestamp of the play. |
| `shooting_play` | Boolean | True for shot attempts (including free throws). |
| `coordinate_x_raw` | Float64 | Shot x coordinate as shipped by ESPN. |
| `coordinate_y_raw` | Float64 | Shot y coordinate as shipped by ESPN. |
| `points_attempted` | Int64 | Shot-attempt points available on the row (box metadata). |
| `short_description` | String | Short description of the row's entity. |
| `game_id` | Int64 | ESPN game id (Int64 canonical join key across pbp, box, rosters, officials). |
| `season` | Int64 | Season year (the year the season ends in; WNBA seasons are single-year). |
| `season_type` | Int64 | ESPN season type (2 regular season, 3 postseason). |
| `home_team_id` | Int64 | ESPN team id of the home team. |
| `home_team_name` | String | Home team nickname. |
| `home_team_mascot` | String | Home team mascot. |
| `home_team_abbrev` | String | Home team abbreviation. |
| `home_team_name_alt` | String | Alternate home team name. |
| `away_team_id` | Int64 | ESPN team id of the away team. |
| `away_team_name` | String | Away team nickname. |
| `away_team_mascot` | String | Away team mascot. |
| `away_team_abbrev` | String | Away team abbreviation. |
| `away_team_name_alt` | String | Alternate away team name. |
| `game_spread` | Float64 | Pregame point spread (positive = home favored magnitude). |
| `home_favorite` | Boolean | True when the home team was the pregame favorite. |
| `game_spread_available` | Boolean | True when a real pregame spread was found (not a default). |
| `home_team_spread` | Float64 | Spread from the home team's perspective. |
| `qtr` | Int64 | Quarter number (1-4; overtime numbered onward). |
| `time` | String | Game clock display string at the play. |
| `clock_minutes` | Int64 | Game clock minutes component. |
| `clock_seconds` | Float64 | Game clock seconds component. |
| `home_timeout_called` | Boolean | True when the home team called timeout on the play. |
| `away_timeout_called` | Boolean | True when the away team called timeout on the play. |
| `half` | Int64 | Half of the game (1-2), derived from the quarter. |
| `game_half` | Int64 | Half of the game the play occurred in. |
| `lead_qtr` | Int64 | Next play's quarter (edge marker for period transitions). |
| `lead_half` | Int64 | Next play's half. |
| `start_quarter_seconds_remaining` | Float64 | Seconds remaining in the quarter when the play began. |
| `start_half_seconds_remaining` | Float64 | Seconds remaining in the half when the play began. |
| `start_game_seconds_remaining` | Float64 | Seconds remaining in the game when the play began. |
| `end_quarter_seconds_remaining` | Float64 | Seconds remaining in the quarter when the play ended. |
| `end_half_seconds_remaining` | Float64 | Seconds remaining in the half when the play ended. |
| `end_game_seconds_remaining` | Float64 | Seconds remaining in the game when the play ended. |
| `period` | Int64 | Period number (1-4, 5+ = overtime). |
| `lag_qtr` | Int64 | Previous play's quarter (edge marker for period transitions). |
| `lag_half` | Int64 | Previous play's half. |
| `coordinate_x` | Float64 | Shot x coordinate, court-normalized. |
| `coordinate_y` | Float64 | Shot y coordinate, court-normalized. |
| `game_date` | Date | Game date (America/New_York). |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game tip-off datetime (America/New_York). |
| `type_abbreviation` | String | Season/competition type abbreviation. |

## Coverage

_No build manifest yet._
