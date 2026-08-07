# `player_core`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_16_player_core_creation.py`](../../python/espn_wnba_16_player_core_creation.py) |
| **Release tag** | [`espn_wnba_player_core`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_core) |
| **File stem** | `player_core_{season}.{parquet,csv,rds}` |
| **Seasons built** | — |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_wnba.yml` — cron 07:00 UTC in season, plus `repository_dispatch` from `wehoop-wnba-raw`. Runs `scripts/daily_wnba_data_processor.sh` (Python build + the R crosswalk tail). Draft refreshes annually via `annual_wnba_draft.yml`; rosters additionally refresh weekly via `weekly_wnba.yml`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year (the year the season ends in; WNBA seasons are single-year). |
| `athlete_id` | Int64 | ESPN athlete id for the row's player. |
| `guid` | String | ESPN GUID for the entity. |
| `uid` | String | ESPN universal id string (`s:40~l:59~...`). |
| `slug` | String | URL-safe ESPN slug for the entity. |
| `type` | String | Row type from the source feed (e.g. official assignment type). |
| `first_name` | String | First name. |
| `last_name` | String | Last name. |
| `full_name` | String | Player full name. |
| `display_name` | String | Display name for the row's entity. |
| `short_name` | String | Short name for the row's entity. |
| `height` | Float64 | Height in inches. |
| `display_height` | String | Height display string (`6' 2"`). |
| `weight` | Float64 | Weight in pounds. |
| `display_weight` | String | Weight display string (`190 lbs`). |
| `age` | Int64 | Age in years at capture time. |
| `date_of_birth` | String | Date of birth. |
| `birth_city` | String | Birth city. |
| `birth_state` | String | Birth state/province. |
| `birth_country` | String | Birth country. |
| `jersey` | String | Jersey number. |
| `position_id` | Int64 | ESPN position id. |
| `position_name` | String | Position name. |
| `position_abbreviation` | String | Position abbreviation. |
| `position_display_name` | String | Position display name. |
| `college_id` | Int64 | ESPN college/team id of the athlete's college (draft frame). |
| `current_team_id` | Int64 | ESPN team id of the athlete's current team (player_core snapshot). |
| `headshot_href` | String | Headshot URL. |
| `experience_years` | Int64 | Years of professional experience. |
| `status_id` | Int64 | ESPN game-status id. |
| `status_name` | String |  |
| `status_type` | String |  |
| `draft_year` | Int64 | Year the player was drafted. |
| `draft_round` | Int64 | Round the player was drafted in. |
| `draft_selection` | Int64 | Overall selection number of the player's draft pick. |
| `active` | Boolean | True when the athlete is currently active. |

## Coverage

_No build manifest yet._
