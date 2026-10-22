# `schedule_crosswalk`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_12_schedule_crosswalk_creation.py`](../../python/espn_wnba_12_schedule_crosswalk_creation.py) |
| **Release tag** | [`wnba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) |
| **File stem** | `wnba_schedule_crosswalk_{season}.{parquet,csv,rds}` |
| **Seasons built** | 2002–2026 (25 seasons) |
| **Last published** | 2026-10-20 (newest release asset) |
| **Tag created** | 2026-06-13 |
| **Release assets** | 232 |

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
| 2002 | 274 | 2026-10-11 16:43:25 UTC |
| 2003 | 258 | 2026-10-11 16:45:11 UTC |
| 2004 | 241 | 2026-10-11 16:47:12 UTC |
| 2005 | 239 | 2026-10-11 16:48:53 UTC |
| 2006 | 258 | 2026-10-11 16:50:41 UTC |
| 2007 | 243 | 2026-10-11 16:52:29 UTC |
| 2008 | 259 | 2026-10-11 16:54:20 UTC |
| 2009 | 243 | 2026-10-11 16:56:08 UTC |
| 2010 | 221 | 2026-10-11 16:57:59 UTC |
| 2011 | 225 | 2026-10-11 16:59:51 UTC |
| 2012 | 223 | 2026-10-11 17:01:42 UTC |
| 2013 | 222 | 2026-10-11 17:03:32 UTC |
| 2014 | 223 | 2026-10-11 17:05:31 UTC |
| 2015 | 226 | 2026-10-11 17:07:15 UTC |
| 2016 | 220 | 2026-10-11 17:09:06 UTC |
| 2017 | 220 | 2026-10-11 17:10:54 UTC |
| 2018 | 222 | 2026-10-11 17:12:47 UTC |
| 2019 | 221 | 2026-10-11 17:14:42 UTC |
| 2020 | 147 | 2026-10-11 17:16:29 UTC |
| 2021 | 211 | 2026-10-11 17:18:16 UTC |
| 2022 | 241 | 2026-10-11 17:20:14 UTC |
| 2023 | 263 | 2026-10-11 17:21:51 UTC |
| 2024 | 264 | 2026-10-11 17:23:40 UTC |
| 2025 | 312 | 2026-10-11 17:25:38 UTC |
| 2026 | 356 | 2026-10-21 15:15:44 UTC |
