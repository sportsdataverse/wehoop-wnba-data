# `schedules`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_14_schedules_creation.py`](../../python/espn_wnba_14_schedules_creation.py) |
| **Release tag** | [`espn_wnba_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_schedules) |
| **File stem** | `wnba_schedule_{season}.{parquet,csv,rds}` |
| **Seasons built** | — |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_wnba.yml` — cron 07:00 UTC in season, plus `repository_dispatch` from `wehoop-wnba-raw`. Runs `scripts/daily_wnba_data_processor.sh` (Python build + the R crosswalk tail). Draft refreshes annually via `annual_wnba_draft.yml`; rosters additionally refresh weekly via `weekly_wnba.yml`.

## Columns

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN id of the row's entity (play id in pbp; game id in the schedule frame). |
| `uid` | String | ESPN universal id string (`s:40~l:59~...`). |
| `date` | String | Raw ESPN date string for the event. |
| `attendance` | Float64 | Announced attendance. |
| `time_valid` | Boolean | True when ESPN considers the scheduled tip time reliable. |
| `neutral_site` | Boolean | True when played at a neutral site. |
| `conference_competition` | Boolean | True when both teams belong to the same conference (schedule flag). |
| `play_by_play_available` | Boolean | True when ESPN has play-by-play for the game. |
| `recent` | Boolean | ESPN "recent" schedule flag. |
| `start_date` | String | Scheduled start datetime string from the ESPN schedule feed. |
| `broadcast` | String | Primary broadcast string for the game. |
| `highlights` | String | ESPN highlights payload (stringified). |
| `notes_type` | String | ESPN event note type (e.g. `event`). |
| `notes_headline` | String | ESPN event note headline (e.g. `WNBA Finals`). |
| `broadcast_market` | String | Broadcast market type (`national`, `home`, `away`). |
| `broadcast_name` | String | Broadcasting network name. |
| `type_id` | Int64 | ESPN season/competition type id (2 regular season, 3 postseason). |
| `type_abbreviation` | String | Season/competition type abbreviation. |
| `venue_id` | Int64 | ESPN venue id where the game was played. |
| `venue_full_name` | String | Venue name. |
| `venue_address_city` | String | Venue city. |
| `venue_address_state` | String | Venue state. |
| `venue_indoor` | Boolean | True for indoor venues. |
| `status_clock` | Float64 | Game clock (seconds) at the status snapshot. |
| `status_display_clock` | String | Game clock display string at the status snapshot. |
| `status_period` | Float64 | Period number at the status snapshot. |
| `status_type_id` | Int64 | ESPN game-status type id (schedule frame). |
| `status_type_name` | String | ESPN status name (`STATUS_FINAL`, ...). |
| `status_type_state` | String | ESPN status state (`pre`, `in`, `post`). |
| `status_type_completed` | Boolean | True when the game is complete. |
| `status_type_description` | String | Human status description (`Final`, `Scheduled`). |
| `status_type_detail` | String | Status detail string (`Final`, `Sat, June 7th at 1:00 PM EDT`). |
| `status_type_short_detail` | String | Short status detail string. |
| `format_regulation_periods` | Float64 | Number of regulation periods (4 for the WNBA). |
| `home_id` | Int64 | ESPN team id of the home team (schedule frame). |
| `home_uid` | String | ESPN universal id string for the home team. |
| `home_location` | String | Home team market/location. |
| `home_name` | String | Home team nickname. |
| `home_abbreviation` | String | Home team abbreviation. |
| `home_display_name` | String | Home team full display name. |
| `home_short_display_name` | String | Home team short display name. |
| `home_color` | String | Home team primary color (hex). |
| `home_alternate_color` | String | Home team alternate color (hex). |
| `home_is_active` | Boolean | True when the home team is an active franchise. |
| `home_venue_id` | Int64 | ESPN home team's registered venue id. |
| `home_logo` | String | Home team logo URL. |
| `home_score` | Int64 | Home team final score. |
| `home_winner` | Boolean | True when the home team won. |
| `home_linescores` | String | Home team per-period linescores (stringified list). |
| `home_records` | String | Home team record strings (stringified list). |
| `away_id` | Int64 | ESPN team id of the away team (schedule frame). |
| `away_uid` | String | ESPN universal id string for the away team. |
| `away_location` | String | Away team market/location. |
| `away_name` | String | Away team nickname. |
| `away_abbreviation` | String | Away team abbreviation. |
| `away_display_name` | String | Away team full display name. |
| `away_short_display_name` | String | Away team short display name. |
| `away_color` | String | Away team primary color (hex). |
| `away_alternate_color` | String | Away team alternate color (hex). |
| `away_is_active` | Boolean | True when the away team is an active franchise. |
| `away_venue_id` | Int64 | ESPN away team's registered venue id. |
| `away_logo` | String | Away team logo URL. |
| `away_score` | Int64 | Away team final score. |
| `away_winner` | Boolean | True when the away team won. |
| `away_linescores` | String | Away team per-period linescores (stringified list). |
| `away_records` | String | Away team record strings (stringified list). |
| `game_id` | Int64 | ESPN game id (Int64 canonical join key across pbp, box, rosters, officials). |
| `season` | Int64 | Season year (the year the season ends in; WNBA seasons are single-year). |
| `season_type` | Int64 | ESPN season type (2 regular season, 3 postseason). |
| `status_type_alt_detail` | String | Alternate status detail string. |
| `game_json` | Boolean | Capture flag - the raw repo holds the game summary JSON. |
| `game_json_url` | String | Raw-repo URL of the game summary JSON. |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game tip-off datetime (America/New_York). |
| `game_date` | Date | Game date (America/New_York). |
| `PBP` | Boolean | Capture flag - the raw repo holds a pbp JSON for this game. |
| `team_box` | Boolean | Capture flag - the raw repo holds team box data for this game. |
| `player_box` | Boolean | Capture flag - the raw repo holds player box data for this game. |

## Coverage

_No build manifest yet._
