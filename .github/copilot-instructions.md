# wehoop-wnba-data Copilot Instructions

## Project Context

This repo is the R-side parser stage for the WNBA. It reads raw ESPN
JSON from
`https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/...`,
compiles per-season parquet/rds/csv artifacts under `wnba/<dataset>/`,
and uploads them to release tags on `sportsdataverse/sportsdataverse-data`
that the public `wehoop` R package reads via its `load_wnba_*()` loaders.

Pipeline: `ESPN -> wehoop-wnba-raw -> wehoop-wnba-data [HERE] -> sportsdataverse-data -> wehoop`.

Package: `wehoop.wnba` (v0.0.1, License: CC BY 4.0).

Do not confuse with `wehoop-wnba-stats-data` — that's the parser for
the WNBA Stats API source, which lives in a separate pipeline.

## Repository Workflow

- Branch from `main`; `main` is the default and release branch.
- The cron entry point is `.github/workflows/daily_wnba.yml`, which fires
  on `repository_dispatch: [daily_wnba_data]` (from the raw repo's push
  trigger) and on a `0 7 UTC` cron gated to in-season windows.
- Don't reorganize the `wnba/` output tree without aligning `wehoop`'s
  `load_wnba_*()` URL builders — release assets are
  `<release_tag>/<file_prefix>_<season>.{rds,csv,parquet}`.
- ESPN parsing bugs go upstream in `wehoop` (`R/espn_wnba_*.R`); this
  repo should stay a thin compile-and-upload layer.

## Build & Development Commands

The 11 daily datasets are built by **Python** (`python/wnba_data_build`, a
parity-validated port of the `espn_wnba_01..10` scripts — every dataset is
tested against the published release asset it must reproduce). R is retained
for the three crosswalks, the `.rds` serialization (`R/serialize_rds.R` —
`wehoop::load_wnba_*()` reads rds), and `run_summary.R`. Draft is annual
(`annual_wnba_draft.yml`), not part of the daily run.

```sh
# Current entry point (Python + the R tail), per season:
bash scripts/daily_wnba_data_processor.sh -s 2025 -e 2025

# Build one dataset directly (--publish uploads; --dry-run doesn't):
cd python && uv run python -m wnba_data_build --dataset pbp --base ../wnba -s 2025 -e 2025 --dry-run
uv run pytest                                # the release-parity suite

# Serialize the Python parquet to .rds (optionally one dataset):
Rscript R/serialize_rds.R -s 2025 -e 2025 [--dataset draft] [--no-upload]

# Legacy all-R path, retained as a fallback:
bash scripts/daily_wnba_R_processor.sh -s 2025 -e 2025

# Run any creation script directly:
Rscript R/espn_wnba_01_pbp_creation.R              -s 2025 -e 2025
Rscript R/espn_wnba_02_team_box_creation.R         -s 2025 -e 2025
Rscript R/espn_wnba_03_player_box_creation.R       -s 2025 -e 2025
Rscript R/espn_wnba_04_rosters_creation.R          -s 2025 -e 2025
Rscript R/espn_wnba_05_player_season_stats_creation.R -s 2025 -e 2025
Rscript R/espn_wnba_06_team_season_stats_creation.R   -s 2025 -e 2025
Rscript R/espn_wnba_07_standings_creation.R        -s 2025 -e 2025
Rscript R/espn_wnba_08_draft_creation.R            -s 2025 -e 2025
Rscript R/espn_wnba_09_game_rosters_creation.R     -s 2025 -e 2025
Rscript R/espn_wnba_10_officials_creation.R        -s 2025 -e 2025

# One-time / on-add: ensure every release tag exists
Rscript R/0000_create_wehoop_releases_init.R
```

`GITHUB_PAT` must be exported (or provided by `secrets.GITHUB_TOKEN` in
CI) for `piggyback` release uploads. Bare invocations default to
`wehoop:::most_recent_wnba_season()` for both year bounds.

Output paths and release tags (see `CLAUDE.md` for the full table):

- `wnba/pbp/{rds,csv,parquet}/play_by_play_{year}.{ext}` → `espn_wnba_pbp`
- `wnba/schedules/{rds,csv,parquet}/wnba_schedule_{year}.{ext}` → `espn_wnba_schedules`
- `wnba/shots/{rds,csv,parquet}/shots_{year}.{ext}` → `espn_wnba_shots`
- `wnba/team_box/...` → `espn_wnba_team_boxscores`
- `wnba/player_box/...` → `espn_wnba_player_boxscores`
- `wnba/rosters/...`, `wnba/player_season_stats/...`,
  `wnba/team_season_stats/...`, `wnba/standings/...`,
  `wnba/game_rosters/...`, `wnba/officials/...` → matching `espn_wnba_*` tags

## Code Style

- Follow tidyverse style: `snake_case`, 2-space indent, `%>%` pipes.
- Use `wehoop:::*` internals for ESPN parsing (`espn_wnba_pbp`,
  `rds_from_url`, `most_recent_wnba_season`); don't reimplement
  ESPN-shape logic here.
- Always tag outputs with `wehoop:::make_wehoop_data(<desc>, Sys.time())`
  before saving — downstream loaders rely on the `wehoop_data` class.
- Three formats per dataset: `saveRDS()`, `data.table::fwrite()`,
  `arrow::write_parquet()`.
- Use `purrr::insistently()` + `purrr::rate_backoff()` around
  `sportsdataversedata::sportsdataverse_save()` for release uploads —
  GitHub releases 502 intermittently.
- `cli::cli_progress_step()` for stage messaging, `progressr` for
  per-game progress bars, `glue::glue()` for path templating.
- `future::plan("multisession")` + `furrr::future_map_dfr()` for the
  per-game iteration loops.

## Daily Umbrella Workflow

`.github/workflows/daily_wnba.yml` runs
`scripts/daily_wnba_data_processor.sh`, which builds the 11 daily datasets
with `wnba_data_build`, then runs the R crosswalks and `serialize_rds.R`,
and commits the cumulative output. Triggers:

- `repository_dispatch: [daily_wnba_data]` — fired by
  `wehoop-wnba-raw`'s `wehoop_wnba_data_trigger.yml` when it pushes a
  scrape commit. The 2-hour offset behind raw's `0 5 UTC` cron lets the
  scrape land before we read it.
- `schedule:` cron `0 7 UTC` daily, gated to in-season month/day
  windows (late October, November–December, January–June, early July).
- `workflow_dispatch:` accepts optional `start_year`/`end_year` inputs.

Draft is annual: `.github/workflows/annual_wnba_draft.yml` fires off the raw
repo's `wehoop_wnba_draft_trigger.yml` dispatch (and an April cron), builds
the draft dataset with `wnba_data_build --dataset draft`, and serializes its
rds with `serialize_rds.R --dataset draft`. `R/espn_wnba_08_draft_creation.R`
is retained as the R fallback.

## Cross-Repo References

- Upstream raw cache: <https://github.com/sportsdataverse/wehoop-wnba-raw>
- Downstream releases: <https://github.com/sportsdataverse/sportsdataverse-data>
- Public R API: <https://github.com/sportsdataverse/wehoop>
- Sister WBB parser: <https://github.com/sportsdataverse/wehoop-wbb-data>
- Shared conventions: <https://github.com/sportsdataverse/wehoop/blob/main/CLAUDE.md>

## Conventional Commits

Use `type(scope): description`. Common types: `feat`, `fix`, `chore`,
`ci`, `docs`, `refactor`. Use `type!:` or a `BREAKING CHANGE:` footer
for breaking changes.

Daily automated umbrella commits keep the legacy subject
`"WNBA Data Update (Start: YYYY End: YYYY)"` — downstream tooling
parses years from this format, so don't change it.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
