# `team_crosswalk`

| | |
|---|---|
| **Builder** | [`R/wnba_11_team_crosswalk_creation.R`](../../R/wnba_11_team_crosswalk_creation.R) |
| **Release tag** | [`wnba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) |
| **File stem** | `wnba_team_crosswalk_{season}.{parquet,csv,rds}` |
| **Seasons built** | 2026 (1 season) |
| **Last published** | — (newest release asset) |
| **Tag created** | — |
| **Release assets** | — |

## Automation

`.github/workflows/daily_wnba.yml` — cron 07:00 UTC in season, plus `repository_dispatch` from `wehoop-wnba-raw`. Runs `scripts/daily_wnba_data_processor.sh` (Python build + the R crosswalk tail). Draft refreshes annually via `annual_wnba_draft.yml`; rosters additionally refresh weekly via `weekly_wnba.yml`.

## Columns

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year (the year the season ends in; WNBA seasons are single-year). |
| `espn_team_id` | Int64 | ESPN team id side of the crosswalk row. |
| `espn_abbreviation` | String | ESPN team abbreviation. |
| `espn_display_name` | String | ESPN display name (team or athlete) on the crosswalk row. |
| `espn_short_name` | String | ESPN short name. |
| `espn_location` | String | ESPN team market/location string. |
| `espn_mascot` | String | ESPN team mascot/nickname. |
| `wnba_team_id` | String | stats.wnba.com team id. |
| `wnba_team_tricode` | String | stats.wnba.com three-letter team code. |
| `wnba_team_name` | String | stats.wnba.com team nickname. |
| `wnba_team_city` | String | stats.wnba.com team city. |
| `wnba_team_slug` | String | stats.wnba.com team slug. |
| `fox_team_id` | String | Fox Sports team id. |
| `fox_team_name` | String | Fox Sports team name. |
| `yahoo_team_id` | String | Yahoo Sports team id. |
| `yahoo_team_abbreviation` | String | Yahoo Sports team abbreviation. |
| `yahoo_team_name` | String | Yahoo Sports team name. |
| `match_method` | String | How the crosswalk row was matched (exact id, name+date heuristic, etc.). |
| `match_confidence` | Float64 | Crosswalk match confidence score (1.0 = exact key match). |

## Coverage

| season | rows | built (UTC) |
|---:|---:|---|
| 2026 | 15 | 2026-06-13 04:48:04 UTC |
