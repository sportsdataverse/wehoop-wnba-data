# CLAUDE.md — wehoop-wnba-data

R compiler (`DESCRIPTION` package `wehoop.wnba`, not on CRAN) that reshapes
per-game ESPN WNBA JSON from the paired
[wehoop-wnba-raw](https://github.com/sportsdataverse/wehoop-wnba-raw) into
season-level parquet/csv/rds, then uploads them as GitHub Releases on
`sportsdataverse/sportsdataverse-data`. The `wehoop` package's `load_wnba_*()`
loaders read those releases via piggyback URLs.

Pipeline: `ESPN -> wehoop-wnba-raw --push--> wehoop-wnba-data [HERE] --release--> sportsdataverse-data --> wehoop`.

## Commands (verified)

Driven by `scripts/daily_wnba_R_processor.sh` (getopts `-s -e`; loops seasons,
runs each creation script, commits + pushes). Reads raw JSON from
`raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw`, not a local clone.

```sh
bash scripts/daily_wnba_R_processor.sh -s 2025 -e 2025   # full daily compile
Rscript R/espn_wnba_01_pbp_creation.R -s 2025 -e 2025     # any single creation script
```

Daily SCRIPTS array runs in order: `espn_wnba_01_pbp` (also writes schedules +
the `shots` filtered subset), `_02_team_box`, `_03_player_box`, `_04_rosters`,
`_05_player_season_stats`, `_06_team_season_stats`, `_07_standings`,
`_09_game_rosters`, `_10_officials`, then `wnba_11_team_crosswalk`,
`wnba_12_schedule_crosswalk`, `wnba_13_player_crosswalk`.
`espn_wnba_08_draft_creation.R` is NOT in the daily flow — draft runs annually.
One-time bootstraps: `R/0000_create_wehoop_releases_init.R` (creates release
tags idempotently), `R/0001_push_existing_release_data.R`. `R/run_summary.R`
writes a CI summary.

`GITHUB_PAT` is required for uploads (CI injects `secrets.SDV_GH_TOKEN`).

## Outputs

Local committed output under `wnba/<dataset>/{rds,csv,parquet}/`; each script
also uploads to its release tag on `sportsdataverse-data` (asset shape
`<tag>/<file_prefix>_<season>.{rds,csv,parquet}`):

`espn_wnba_schedules`, `espn_wnba_pbp`, `espn_wnba_shots` (derived),
`espn_wnba_team_boxscores`, `espn_wnba_player_boxscores`, `espn_wnba_rosters`,
`espn_wnba_player_season_stats`, `espn_wnba_team_season_stats`,
`espn_wnba_standings`, `espn_wnba_draft` (annual), `espn_wnba_game_rosters`,
`espn_wnba_officials` — one tag per creation script, read by `wehoop::load_wnba_*()`.

## CI

- `.github/workflows/daily_wnba.yml` — cron (in-season windows, `0 7 UTC`) +
  `repository_dispatch` type `daily_wnba_data` (fired by the raw repo) +
  `workflow_dispatch`. Extracts years from the dispatch commit message
  (`Start:`/`End:` regex), defaulting to `wehoop::most_recent_wnba_season()`.
- `.github/workflows/annual_wnba_draft.yml` — draft pipeline (ESPN
  `espn_wnba_08_draft_creation.R` + WNBA Stats API `wnba_stats_07_draft.R`);
  triggered by `repository_dispatch` type `annual_wnba_draft` from the raw repo.
- `.github/workflows/weekly_wnba.yml` — Sunday `0 6 UTC` roster refresh.

## Gotchas

- Daily CI commit subject `"WNBA Data Update (Start: <yr> End: <yr>)"` is load-bearing — don't restyle.
- Schedules + shots are emitted inside `espn_wnba_01_pbp_creation.R`; don't add a separate schedule/shots script — extend `01`.
- Release tags + the `<tag>/<prefix>_<season>` asset shape are load-bearing for `wehoop::load_wnba_*()`; renaming a tag or reorganizing `wnba/` is a breaking change.
- `DESCRIPTION` `Remotes:` pins `wehoop` + `sportsdataverse-data` + `piggyback`; license is CC BY 4.0 (data-repo convention), not MIT.
- Never add AI co-author trailers to commits. Use Conventional Commits (`feat(compile):`, `fix(pbp):`, `ci:`).
