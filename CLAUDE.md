# CLAUDE.md — wehoop-wnba-data

R compiler (`DESCRIPTION` package `wehoop.wnba`, not on CRAN) that reshapes
per-game ESPN WNBA JSON from the paired
[wehoop-wnba-raw](https://github.com/sportsdataverse/wehoop-wnba-raw) into
season-level parquet/csv/rds, then uploads them as GitHub Releases on
`sportsdataverse/sportsdataverse-data`. The `wehoop` package's `load_wnba_*()`
loaders read those releases via piggyback URLs.

Pipeline: `ESPN -> wehoop-wnba-raw --push--> wehoop-wnba-data [HERE] --release--> sportsdataverse-data --> wehoop`.

## Commands (verified)

Driven by the single entrypoint `scripts/daily_wnba_data_processor.sh`
(getopts `-s -e -l`; loops seasons, builds each dataset, commits + pushes).
`-l python` is the default (`wnba_data_build`); `-l R` is the retained
rollback over `espn_wnba_01`…`10`. `scripts/daily_wnba_R_processor.sh` is a
deprecation shim that execs it with `-l R`. Reads raw JSON from
`raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw`, not a local clone.

```sh
bash scripts/daily_wnba_data_processor.sh -s 2025 -e 2025        # full daily compile (python)
bash scripts/daily_wnba_data_processor.sh -s 2025 -e 2025 -l R   # R rollback
Rscript R/espn_wnba_01_pbp_creation.R -s 2025 -e 2025     # any single creation script
```

Daily SCRIPTS array runs in order: `espn_wnba_01_pbp` (also writes schedules +
the `shots` filtered subset), `_02_team_box`, `_03_player_box`, `_04_rosters`,
`_05_player_season_stats`, `_06_team_season_stats`, `_07_standings`,
`_09_game_rosters`, `_10_officials`, then `wnba_11_team_crosswalk`,
`wnba_12_schedule_crosswalk`, `wnba_13_player_crosswalk`.
`espn_wnba_08_draft_creation.R` is NOT in the daily flow — draft runs annually.
One-time bootstraps live in `ops/init/` (run from the repo root):
`0000_create_wehoop_releases_init.R` (creates release tags idempotently),
`0001_push_existing_release_data.R`. `R/run_summary.R` writes a CI summary.

`GITHUB_PAT` is required for uploads (CI injects `secrets.SDV_GH_TOKEN`).

**Every `R/espn_wnba_*_creation.R` stage publishes to the LIVE release when it
runs — there is no dry-run flag.** Running one locally to inspect its output
overwrites production; that is how the 2025 pbp/shots/team_box assets were
overwritten on 2026-08-07. Blanking `GITHUB_PAT` is not a workaround (the save
is wrapped in `insistently(pause_min = 60, max_times = 10)`, so it retries for
~10 minutes and only then fails). Use `ops/_r_no_publish.R`, which replaces the
publisher before sourcing the stage and aborts if that swap fails.

`ops/output_parity.sh -d <dataset> -s <season>` is the weekly R↔Python **output**
parity check (`.github/workflows/weekly_output_parity.yml`, Mondays 09:00 UTC).
`tests/test_r_python_parity.py` proves the two chains declare the same datasets;
this proves they produce the same values. It rebuilds both sides into temp dirs
— the chains share one output path and clobber each other, so the checked-in
tree only ever holds whichever ran last — and runs the R chain from stage 01,
because the stages feed each other (02 reads stage 01's `schedules/rds`).

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
- `.github/workflows/annual_wnba_draft.yml` — the **ESPN** draft only, built by
  `wnba_data_build --dataset draft --publish` (`espn_wnba_08_draft_creation.R`
  is the R fallback); triggered by `repository_dispatch` type
  `annual_wnba_draft` from the raw repo. The WNBA **Stats API** draft is not
  built here: it belongs to `wehoop-wnba-stats-data`'s own
  `annual_wnba_stats_draft.yml`.
- `.github/workflows/weekly_wnba.yml` — Sunday `0 6 UTC` roster refresh.
- `.github/workflows/tests.yml` — offline gate on every push/PR: pytest
  (release-parity + R↔Python stage-parity suites), ruff, `bash -n`, and the
  generated-docs drift check (`python -m wnba_data_build.docs --check
  --no-live`). Its sparse checkout MUST include `R/` or the parity gate finds
  zero R stages and reds out main.
- `.github/workflows/orphan_scripts.yml` — shared gate: every script in
  `scripts/`/`ops/` must be referenced by a runbook, workflow, or other script.

## Generated docs

`docs/datasets/*.md` + the README/CLAUDE `<!-- BEGIN GENERATED: datasets -->
| Script | Dataset | Release tag | Last published |
|---|---|---|---|
| [`python/espn_wnba_01_pbp_creation.py`](python/espn_wnba_01_pbp_creation.py) | [`pbp`](docs/datasets/pbp.md) | [`espn_wnba_pbp`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_pbp) | — |
| [`python/espn_wnba_02_team_box_creation.py`](python/espn_wnba_02_team_box_creation.py) | [`team_box`](docs/datasets/team_box.md) | [`espn_wnba_team_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_boxscores) | — |
| [`python/espn_wnba_03_player_box_creation.py`](python/espn_wnba_03_player_box_creation.py) | [`player_box`](docs/datasets/player_box.md) | [`espn_wnba_player_boxscores`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_boxscores) | — |
| [`python/espn_wnba_04_rosters_creation.py`](python/espn_wnba_04_rosters_creation.py) | [`rosters`](docs/datasets/rosters.md) | [`espn_wnba_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_rosters) | — |
| [`python/espn_wnba_05_player_season_stats_creation.py`](python/espn_wnba_05_player_season_stats_creation.py) | [`player_season_stats`](docs/datasets/player_season_stats.md) | [`espn_wnba_player_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_season_stats) | — |
| [`python/espn_wnba_06_team_season_stats_creation.py`](python/espn_wnba_06_team_season_stats_creation.py) | [`team_season_stats`](docs/datasets/team_season_stats.md) | [`espn_wnba_team_season_stats`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_team_season_stats) | — |
| [`python/espn_wnba_07_standings_creation.py`](python/espn_wnba_07_standings_creation.py) | [`standings`](docs/datasets/standings.md) | [`espn_wnba_standings`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_standings) | — |
| [`python/espn_wnba_08_draft_creation.py`](python/espn_wnba_08_draft_creation.py) | [`draft`](docs/datasets/draft.md) | [`espn_wnba_draft`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_draft) | — |
| [`python/espn_wnba_09_game_rosters_creation.py`](python/espn_wnba_09_game_rosters_creation.py) | [`game_rosters`](docs/datasets/game_rosters.md) | [`espn_wnba_game_rosters`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_game_rosters) | — |
| [`python/espn_wnba_10_officials_creation.py`](python/espn_wnba_10_officials_creation.py) | [`officials`](docs/datasets/officials.md) | [`espn_wnba_officials`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_officials) | — |
| [`R/wnba_11_team_crosswalk_creation.R`](R/wnba_11_team_crosswalk_creation.R) | [`team_crosswalk`](docs/datasets/team_crosswalk.md) | [`wnba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) | — |
| [`R/wnba_12_schedule_crosswalk_creation.R`](R/wnba_12_schedule_crosswalk_creation.R) | [`schedule_crosswalk`](docs/datasets/schedule_crosswalk.md) | [`wnba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) | — |
| [`R/wnba_13_player_crosswalk_creation.R`](R/wnba_13_player_crosswalk_creation.R) | [`player_crosswalk`](docs/datasets/player_crosswalk.md) | [`wnba_crosswalk`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) | — |
| [`python/espn_wnba_14_schedules_creation.py`](python/espn_wnba_14_schedules_creation.py) | [`schedules`](docs/datasets/schedules.md) | [`espn_wnba_schedules`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_schedules) | — |
| [`python/espn_wnba_15_shots_creation.py`](python/espn_wnba_15_shots_creation.py) | [`shots`](docs/datasets/shots.md) | [`espn_wnba_shots`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_shots) | — |
| [`python/espn_wnba_16_player_core_creation.py`](python/espn_wnba_16_player_core_creation.py) | [`player_core`](docs/datasets/player_core.md) | [`espn_wnba_player_core`](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_player_core) | — |
<!-- END GENERATED: datasets -->
