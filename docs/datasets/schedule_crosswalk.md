# `schedule_crosswalk`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_12_schedule_crosswalk_creation.py`](../../python/espn_wnba_12_schedule_crosswalk_creation.py) |
| **Release tag** | [`wnba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) |
| **File stem** | `wnba_schedule_crosswalk_{season}.{parquet,csv,rds}` |
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
| `season_type` | String | ESPN season type (2 regular season, 3 postseason). |
| `game_date` | Date | Game date (America/New_York). |
| `home_espn_team_id` | Int64 | ESPN team id of the home team on the crosswalk row. |
| `away_espn_team_id` | Int64 | ESPN team id of the away team on the crosswalk row. |
| `espn_game_id` | String | ESPN game id side of the crosswalk row. |
| `wnba_game_id` | String | stats.wnba.com game id side of the crosswalk row. |
| `wnba_game_code` | String | stats.wnba.com game code (`YYYYMMDD/AWYHOM`). |
| `wnba_home_team_id` | String | stats.wnba.com team id of the home team. |
| `wnba_away_team_id` | String | stats.wnba.com team id of the away team. |
| `fox_game_id` | String | Fox Sports game id side of the crosswalk row. |
| `fox_home_team_id` | String | Fox Sports team id of the home team. |
| `fox_away_team_id` | String | Fox Sports team id of the away team. |
| `yahoo_game_id` | String | Yahoo Sports game id side of the crosswalk row. |
| `match_method` | String | How the crosswalk row was matched (exact id, name+date heuristic, etc.). |
| `match_confidence` | Float64 | Crosswalk match confidence score (1.0 = exact key match). |

## Coverage

| season | rows | built (UTC) |
|---:|---:|---|
| 2026 | 355 | 2026-06-13 04:51:06 UTC |
