# `team_crosswalk`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_11_team_crosswalk_creation.py`](../../python/espn_wnba_11_team_crosswalk_creation.py) |
| **Release tag** | [`wnba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) |
| **File stem** | `wnba_team_crosswalk_{season}.{parquet,csv,rds}` |
| **Seasons built** | 2002–2026 (25 seasons) |
| **Last published** | 2026-10-12 (newest release asset) |
| **Tag created** | 2026-06-13 |
| **Release assets** | 232 |

## Automation

`.github/workflows/daily_wnba.yml` — cron 07:00 UTC in season, plus `repository_dispatch` from `wehoop-wnba-raw`. Runs `scripts/daily_wnba_data_processor.sh` (Python build, crosswalks included). Draft refreshes annually via `annual_wnba_draft.yml`; rosters additionally refresh weekly via `weekly_wnba.yml`.

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
| 2002 | 16 | 2026-10-11 20:38:02 UTC |
| 2003 | 14 | 2026-10-11 20:38:43 UTC |
| 2004 | 13 | 2026-10-11 20:39:28 UTC |
| 2005 | 13 | 2026-10-11 20:40:13 UTC |
| 2006 | 14 | 2026-10-11 20:40:58 UTC |
| 2007 | 13 | 2026-10-11 20:41:43 UTC |
| 2008 | 14 | 2026-10-11 20:42:28 UTC |
| 2009 | 13 | 2026-10-11 20:43:13 UTC |
| 2010 | 12 | 2026-10-11 20:43:58 UTC |
| 2011 | 12 | 2026-10-11 20:44:42 UTC |
| 2012 | 12 | 2026-10-11 20:45:32 UTC |
| 2013 | 12 | 2026-10-11 20:46:14 UTC |
| 2014 | 12 | 2026-10-11 20:46:58 UTC |
| 2015 | 12 | 2026-10-11 20:47:43 UTC |
| 2016 | 12 | 2026-10-11 20:48:29 UTC |
| 2017 | 12 | 2026-10-11 20:49:13 UTC |
| 2018 | 12 | 2026-10-11 20:49:59 UTC |
| 2019 | 12 | 2026-10-11 20:50:43 UTC |
| 2020 | 12 | 2026-10-11 20:51:29 UTC |
| 2021 | 12 | 2026-10-11 20:52:14 UTC |
| 2022 | 12 | 2026-10-11 20:52:58 UTC |
| 2023 | 12 | 2026-10-11 20:53:43 UTC |
| 2024 | 12 | 2026-10-11 20:54:29 UTC |
| 2025 | 13 | 2026-10-11 20:55:14 UTC |
| 2026 | 15 | 2026-10-11 20:56:10 UTC |
