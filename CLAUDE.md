# CLAUDE.md — wehoop-wnba-data Development Guide

## Repo Overview

`wehoop-wnba-data` is the R-side parser for ESPN WNBA play-by-play and
ancillary data. It reads raw ESPN JSON committed by
[`wehoop-wnba-raw`](https://github.com/sportsdataverse/wehoop-wnba-raw)
over `raw.githubusercontent.com`, compiles per-season parquet/rds/csv
artifacts under `wnba/<dataset>/`, and uploads them as release assets
on [`sportsdataverse/sportsdataverse-data`](https://github.com/sportsdataverse/sportsdataverse-data)
under the `espn_wnba_*` tags. Those releases are what
`wehoop::load_wnba_*()` reads at runtime — this repo IS the build step
between the raw scrape and the public R API.

Package name (from `DESCRIPTION`): `wehoop.wnba` (v0.0.1, License: CC BY 4.0).

## Pipeline Position

```
ESPN APIs --[python]--> wehoop-wnba-raw
                              | repository_dispatch (daily_wnba_data)
                              v
                       wehoop-wnba-data [HERE] --[piggyback]--> sportsdataverse-data
                                                                       | release assets
                                                                       v
                                                                 wehoop::load_wnba_*()
```

The parser is triggered by `.github/workflows/daily_wnba.yml`, which
fires on:

- `repository_dispatch: [daily_wnba_data]` (from wehoop-wnba-raw's push trigger)
- `schedule:` cron at `0 7 UTC` daily, gated to in-season month/day windows
  (late October, November–December, January–June, early July) — the
  2-hour offset behind raw's `0 5 UTC` lets the scrape land first
- `workflow_dispatch:` with optional `start_year` / `end_year` inputs

## Release Tags Owned by This Repo

The parsers in `R/espn_wnba_*_creation.R` upload to these tags on
`sportsdataverse/sportsdataverse-data`. The init script
`R/0000_create_wehoop_releases_init.R` creates them idempotently — re-run
it any time you add a new `espn_wnba_*_creation.R` parser.

| Release tag                       | Source script                                  | What it contains |
|-----------------------------------|------------------------------------------------|------------------|
| `espn_wnba_schedules`             | `R/espn_wnba_01_pbp_creation.R` (sidecar)      | Per-season schedule frames |
| `espn_wnba_pbp`                   | `R/espn_wnba_01_pbp_creation.R`                | Per-season play-by-play |
| `espn_wnba_shots`                 | `R/espn_wnba_01_pbp_creation.R` (derived)      | Filtered `shooting_play == TRUE` rows |
| `espn_wnba_team_boxscores`        | `R/espn_wnba_02_team_box_creation.R`           | Per-season team box scores |
| `espn_wnba_player_boxscores`      | `R/espn_wnba_03_player_box_creation.R`         | Per-season player box scores |
| `espn_wnba_rosters`               | `R/espn_wnba_04_rosters_creation.R`            | Per-season team rosters |
| `espn_wnba_player_season_stats`   | `R/espn_wnba_05_player_season_stats_creation.R`| Per-athlete season stats |
| `espn_wnba_team_season_stats`     | `R/espn_wnba_06_team_season_stats_creation.R`  | Per-team season stats |
| `espn_wnba_standings`             | `R/espn_wnba_07_standings_creation.R`          | Per-season standings |
| `espn_wnba_draft`                 | `R/espn_wnba_08_draft_creation.R`              | Per-season draft results (annual) |
| `espn_wnba_game_rosters`          | `R/espn_wnba_09_game_rosters_creation.R`       | Per-game rosters |
| `espn_wnba_officials`             | `R/espn_wnba_10_officials_creation.R`          | Per-game officials |

Each script writes locally to `wnba/<dataset>/{rds,csv,parquet}/...` and
also uploads to its release via `sportsdataversedata::sportsdataverse_save()`
wrapped in `purrr::insistently()` with a `purrr::rate_backoff()` policy.

## Build & Development Commands

The legacy shell entry point is `scripts/daily_wnba_R_processor.sh`, which
runs the first three creation scripts and commits the output. The
in-repo cron workflow `.github/workflows/daily_wnba.yml` is the
authoritative daily flow and runs all `R/espn_wnba_*_creation.R` scripts
in numbered order.

```sh
# Legacy local entry point — runs scripts 01–03 for a year range
bash scripts/daily_wnba_R_processor.sh -s 2025 -e 2025 -r false

# Or run any creation script directly with --start_year/--end_year:
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

# One-time / on-add: ensure all release tags exist on sportsdataverse-data
Rscript R/0000_create_wehoop_releases_init.R

# One-off: backfill release assets from existing local files
Rscript R/0001_push_existing_release_data.R
```

Defaults: when `--start_year`/`--end_year` are omitted, scripts fall back
to `wehoop:::most_recent_wnba_season()` for both bounds (single-season
run). `GITHUB_PAT` must be exported for `piggyback`/`sportsdataversedata`
release uploads to authenticate.

## Project Structure

```
R/
  0000_create_wehoop_releases_init.R         # Create release tags on sportsdataverse-data
  0001_push_existing_release_data.R          # Backfill release assets from local wnba/
  espn_wnba_01_pbp_creation.R                # PBP + schedules + shots
  espn_wnba_02_team_box_creation.R           # Team boxscores
  espn_wnba_03_player_box_creation.R         # Player boxscores
  espn_wnba_04_rosters_creation.R            # Team rosters
  espn_wnba_05_player_season_stats_creation.R# Per-athlete season stats
  espn_wnba_06_team_season_stats_creation.R  # Per-team season stats
  espn_wnba_07_standings_creation.R          # Standings
  espn_wnba_08_draft_creation.R              # Draft (annual)
  espn_wnba_09_game_rosters_creation.R       # Per-game rosters
  espn_wnba_10_officials_creation.R          # Per-game officials
  manifest_upload_helper.R                   # Shared manifest CSV append helper
  minify_json_folders.R                      # JSON minification helper
scripts/
  daily_wnba_R_processor.sh                  # Legacy shell entry point (scripts 01–03)
wnba/                                        # Committed build output, mirrored to releases
  pbp/{rds,csv,parquet}/
  schedules/{rds,csv,parquet}/
  shots/{rds,csv,parquet}/
  team_box/{rds,csv,parquet}/
  player_box/{rds,csv,parquet}/
  rosters/{rds,csv,parquet}/
  player_season_stats/{rds,csv,parquet}/
  team_season_stats/{rds,csv,parquet}/
  standings/{rds,csv,parquet}/
  game_rosters/{rds,csv,parquet}/
  officials/{rds,csv,parquet}/
.github/workflows/
  daily_wnba.yml                             # Cron + repository_dispatch entry point
```

## Conventions

- **Read from raw via `raw.githubusercontent.com`** — every creation
  script pulls input from
  `https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/...`.
  Don't re-hit ESPN here; the raw repo is the only HTTP boundary.
- **Parsing lives in `wehoop`** — the creation scripts call
  `wehoop:::*` internals (`espn_wnba_pbp`, `most_recent_wnba_season`,
  `rds_from_url`, etc.). Schema/ESPN-shape fixes belong in `wehoop`'s
  `R/espn_*.R`, not here.
- **Three file formats per dataset**: `rds`, `csv`, `parquet`, written
  via `saveRDS()`, `data.table::fwrite()`, and `arrow::write_parquet()`.
  Release uploads use `file_types = c("rds", "csv", "parquet")`.
- **Tag output with `make_wehoop_data()`** before saving:
  `wehoop:::make_wehoop_data("<description>", Sys.time())` sets the
  `wehoop_data` class and attaches `wehoop_timestamp`/`wehoop_type`
  attributes that downstream loaders rely on.
- **Parallelism via `future`/`furrr`** — creation scripts call
  `future::plan("multisession")` then `furrr::future_map_dfr()` over
  the per-season game ID list.
- **`progressr` for live progress**, `cli::cli_progress_step()` for
  message scaffolding, `glue::glue()` for paths.
- **Optparse for CLI** — every numbered creation script defines
  `-s/--start_year` and `-e/--end_year` with `wehoop:::most_recent_wnba_season()`
  defaults so a bare `Rscript R/espn_wnba_NN_*.R` does the right thing
  for the current season.
- **Manifest CSVs**: each creation script appends a per-season row to
  `wnba/<dataset>/wnba_<dataset>_in_data_repo.csv` for downstream
  consumers (e.g., wehoop's loaders) to detect what's been built.

## Cross-Repo References

- Upstream raw cache (input): <https://github.com/sportsdataverse/wehoop-wnba-raw>
- Downstream releases (output assets): <https://github.com/sportsdataverse/sportsdataverse-data>
- Public R API that consumes the releases: <https://github.com/sportsdataverse/wehoop>
- Sister parser (same shape, NCAA WBB): <https://github.com/sportsdataverse/wehoop-wbb-data>
- Sister parser (WNBA Stats API source): <https://github.com/sportsdataverse/wehoop-wnba-stats-data>
- Shared conventions for the wehoop family: <https://github.com/sportsdataverse/wehoop/blob/main/CLAUDE.md>

## Project-Specific Gotchas

- **Commit message format is load-bearing** for daily umbrella commits.
  The legacy shell script uses
  `"WNBA Data Update (Start: ${i} End: ${i})"`. Downstream automation
  parses the years from this subject — keep the format if you switch
  the parser to GitHub Actions.
- **Schedules ride along inside `espn_wnba_01_pbp_creation.R`** — that
  script writes `wnba/schedules/...` and uploads to the
  `espn_wnba_schedules` tag in addition to the PBP outputs. Don't add a
  separate `espn_wnba_*_schedules_creation.R`; extend the pbp script if
  the schedule pipeline grows.
- **Shots are derived inside the pbp script**, not scraped — they're
  the `dplyr::filter(.data$shooting_play == TRUE)` subset of PBP and
  upload to the `espn_wnba_shots` tag. Re-running `01_pbp_creation.R`
  refreshes shots automatically.
- **Don't reorganize `wnba/`** without aligning `wehoop`'s
  `load_wnba_*()` URL builders — the release assets are
  `<release_tag>/<file_prefix>_<season>.{rds,csv,parquet}`, and the
  loaders hardcode that shape.
- **`piggyback::pb_release_create()` line-wraps the "already exists"
  error** depending on tag length / cli width — the init script
  collapses whitespace before matching so we don't miss the wrapped
  variant. Don't simplify that error handler.
- **`GITHUB_PAT` is required** for release uploads. The workflow
  injects `secrets.GITHUB_TOKEN`; locally export
  `GITHUB_PAT=<token-with-repo-scope>` before running creation scripts
  end-to-end.

## Commit Convention

Daily automation uses the legacy umbrella subject (load-bearing for
downstream parsing):

```
WNBA Data Update (Start: 2025 End: 2025)
```

For manual / human-authored commits in this repo, use
[Conventional Commits](https://www.conventionalcommits.org/) with a
parser-aware scope when useful:

```
feat(pbp): add shot-distance column to espn_wnba_01_pbp_creation.R
fix(rosters): handle missing jersey number from ESPN payload
chore(release-init): register espn_wnba_game_rosters tag
ci: bump R version pin in daily_wnba.yml
```

Use `type!:` or a `BREAKING CHANGE:` footer for breaking changes. Split
unrelated work into separate commits for reviewability.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
