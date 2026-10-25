# `player_crosswalk`

| | |
|---|---|
| **Builder** | [`python/espn_wnba_13_player_crosswalk_creation.py`](../../python/espn_wnba_13_player_crosswalk_creation.py) |
| **Release tag** | [`wnba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) |
| **File stem** | `wnba_player_crosswalk_{season}.{parquet,csv,rds}` |
| **Seasons built** | 2002–2026 (25 seasons) |
| **Last published** | 2026-10-22 (newest release asset) |
| **Tag created** | 2026-06-13 |
| **Release assets** | 232 |

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
| 2002 | 226 | 2026-10-11 18:32:01 UTC |
| 2003 | 185 | 2026-10-11 18:33:43 UTC |
| 2004 | 173 | 2026-10-11 18:35:31 UTC |
| 2005 | 206 | 2026-10-11 18:37:22 UTC |
| 2006 | 191 | 2026-10-11 18:39:11 UTC |
| 2007 | 183 | 2026-10-11 18:41:00 UTC |
| 2008 | 212 | 2026-10-11 18:42:52 UTC |
| 2009 | 170 | 2026-10-11 18:44:43 UTC |
| 2010 | 154 | 2026-10-11 18:46:33 UTC |
| 2011 | 154 | 2026-10-11 18:48:21 UTC |
| 2012 | 158 | 2026-10-11 18:50:11 UTC |
| 2013 | 156 | 2026-10-11 18:52:00 UTC |
| 2014 | 158 | 2026-10-11 18:53:53 UTC |
| 2015 | 173 | 2026-10-11 18:55:42 UTC |
| 2016 | 171 | 2026-10-11 18:57:30 UTC |
| 2017 | 175 | 2026-10-11 18:59:21 UTC |
| 2018 | 169 | 2026-10-11 19:01:13 UTC |
| 2019 | 170 | 2026-10-11 19:03:02 UTC |
| 2020 | 163 | 2026-10-11 19:04:51 UTC |
| 2021 | 175 | 2026-10-11 19:06:41 UTC |
| 2022 | 186 | 2026-10-11 19:08:34 UTC |
| 2023 | 176 | 2026-10-11 19:10:26 UTC |
| 2024 | 171 | 2026-10-11 19:12:10 UTC |
| 2025 | 212 | 2026-10-11 19:14:01 UTC |
| 2026 | 269 | 2026-10-24 15:15:49 UTC |
