# wehoop-wnba-data Copilot Instructions

## Project Context

This R repo compiles per-season WNBA tidy tables (play-by-play, team box,
player box) from JSON cached in `wehoop-wnba-raw` and uploads them as
releases on `sportsdataverse-data` via
`sportsdataversedata::sportsdataverse_save()`. The `wehoop` R package
loads from those releases.

Pipeline: `wehoop-wnba-raw -> wehoop-wnba-data [HERE] -> sportsdataverse-data -> wehoop`.

## Repository Workflow

- Branch from `main`; `main` is the default and release branch.
- CI entry point: `.github/workflows/daily_wnba.yml` calls `scripts/daily_wnba_R_processor.sh`, which loops seasons and runs the three creation scripts in order.
- Triggers: cron, `repository_dispatch: daily_wnba_data` (fired by `wehoop-wnba-raw`), and manual `workflow_dispatch` with `start_year`/`end_year` inputs.
- Run the creation scripts in order (`01_pbp` → `02_team_box` → `03_player_box`); steps 02/03 depend on outputs from 01.

## Build & Development Commands

```sh
bash scripts/daily_wnba_R_processor.sh -s 2025 -e 2025
```

Or invoke individual scripts:

```sh
Rscript R/espn_wnba_01_pbp_creation.R        -s 2025 -e 2025
Rscript R/espn_wnba_02_team_box_creation.R   -s 2025 -e 2025
Rscript R/espn_wnba_03_player_box_creation.R -s 2025 -e 2025
```

Output: per-season artifacts under `wnba/{pbp,team_box,player_box,schedules}/` (PBP CSVs are gzipped — `play_by_play_{year}.csv.gz`), then uploaded via `sportsdataverse_save(file_types = c("rds", "csv", "parquet"))`.

## Code Style

- Follow the parent package's R style guide (tidyverse, snake_case, 2-space indent, `cli::cli_*` for messaging) — see `wehoop/CLAUDE.md`.
- Don't add ESPN parsing logic here — call into `wehoop::espn_wnba_*()`.
- Keep `DESCRIPTION` Imports minimal.
- `sportsdataversedata::sportsdataverse_save()` is the only upload boundary — never call `piggyback` directly.
- Don't drop the `.gz` suffix on PBP CSV outputs without aligning downstream `wehoop::load_wnba_pbp()`.

## Cross-Repo References

- Conventions, parser internals, ESPN function pattern: <https://github.com/sportsdataverse/wehoop/blob/main/CLAUDE.md>
- Upstream JSON cache: <https://github.com/sportsdataverse/wehoop-wnba-raw>

## Conventional Commits

Use: `type(scope): description`. Common types: `feat`, `fix`, `docs`, `chore`, `ci`, `refactor`. Use `type!:` or a `BREAKING CHANGE:` footer for breaking changes.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
