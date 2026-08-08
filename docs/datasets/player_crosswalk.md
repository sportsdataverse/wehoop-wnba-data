# `player_crosswalk`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_13_player_crosswalk_creation.py`](../../python/espn_wnba_13_player_crosswalk_creation.py) |
| **Release tag** | [`wnba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) |
| **File stem** | `wnba_player_crosswalk_{season}.{parquet,csv,rds}` |
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
| `espn_team_id` | Int64 | ESPN team id side of the crosswalk row. |
| `team_abbreviation` | String | Team abbreviation. |
| `player_name` | String | Player display name. |
| `espn_athlete_id` | String | ESPN athlete id side of the crosswalk row. |
| `espn_full_name` | String | ESPN full athlete name. |
| `espn_jersey` | String | Jersey number as listed by ESPN. |
| `espn_position` | String | Position abbreviation as listed by ESPN. |
| `wnba_player_id` | String | stats.wnba.com player id. |
| `wnba_player_name` | String | stats.wnba.com player display name. |
| `wnba_jersey_num` | String | Jersey number as listed by stats.wnba.com. |
| `wnba_position` | String | Position as listed by stats.wnba.com. |
| `fox_athlete_id` | String | Fox Sports athlete id. |
| `fox_player` | String | Fox Sports player display name. |
| `fox_jersey` | String | Jersey number as listed by Fox Sports. |
| `fox_position_group` | String | Position group as listed by Fox Sports. |
| `yahoo_player_id` | String | Yahoo Sports player id. |
| `yahoo_player_name` | String | Yahoo Sports player display name. |
| `match_method` | String | How the crosswalk row was matched (exact id, name+date heuristic, etc.). |
| `match_confidence` | Float64 | Crosswalk match confidence score (1.0 = exact key match). |
| `match_keys` | String | Keys the crosswalk row was matched on. |

## Coverage

| season | rows | built (UTC) |
|---:|---:|---|
| 2026 | 203 | 2026-06-13 04:57:29 UTC |
