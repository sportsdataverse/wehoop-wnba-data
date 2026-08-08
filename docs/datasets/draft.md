# `draft`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_08_draft_creation.py`](../../python/espn_wnba_08_draft_creation.py) |
| **Release tag** | [`espn_wnba_draft`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_draft) |
| **File stem** | `draft_{season}.{parquet,csv,rds}` |
| **Seasons built** | 2026 (1 season) |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_wnba.yml` — cron 07:00 UTC in season, plus `repository_dispatch` from `wehoop-wnba-raw`. Runs `scripts/daily_wnba_data_processor.sh` (Python build, crosswalks included). Draft refreshes annually via `annual_wnba_draft.yml`; rosters additionally refresh weekly via `weekly_wnba.yml`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year (the year the season ends in; WNBA seasons are single-year). |
| `round` | Int64 | Draft round number. |
| `round_display_name` | String | Draft round display string (`Round 1`). |
| `pick` | Int64 | Pick number within the round. |
| `overall_pick` | Int64 | Overall pick number across rounds. |
| `pick_traded` | String | True when the pick was traded. |
| `pick_notes` | String | Notes attached to the pick (trades, forfeits). |
| `athlete_id` | Int64 | ESPN athlete id for the row's player. |
| `athlete_uid` | String | ESPN universal id string for the athlete. |
| `athlete_guid` | String | ESPN GUID for the athlete. |
| `athlete_first_name` | String | Player first name. |
| `athlete_last_name` | String | Player last name. |
| `athlete_full_name` | String | Player full name. |
| `athlete_display_name` | String | Player display name. |
| `athlete_short_name` | String | Player short name (`F. Lastname`). |
| `athlete_height` | String | Player height in inches. |
| `athlete_weight` | String | Player weight in pounds. |
| `athlete_position_abbreviation` | String | Player position abbreviation. |
| `athlete_position_name` | String | Player position name. |
| `athlete_headshot_href` | String | Player headshot URL. |
| `college_id` | Int64 | ESPN college/team id of the athlete's college (draft frame). |
| `college_name` | String | Athlete's college name. |
| `college_short_name` | String | Athlete's college short name. |
| `college_abbreviation` | String | Athlete's college abbreviation. |
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

## Coverage

| season | rows | built (UTC) |
|---:|---:|---|
| 2026 | 45 | 2026-05-30 07:00:05 UTC |
