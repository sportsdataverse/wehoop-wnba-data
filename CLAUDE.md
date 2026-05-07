# CLAUDE.md — wehoop-wnba-data Development Guide

## Repo Overview

`wehoop-wnba-data` is the R-side parser for ESPN WNBA play-by-play. It
consumes per-game JSON already cached in `wehoop-wnba-raw`, compiles
tidy per-season tables (play-by-play, team box, player box), and uploads
them as releases on `sportsdataverse/sportsdataverse-data` via
`piggyback` / `sportsdataversedata::sportsdataverse_save()`. The
`wehoop` R package then loads from those releases.

## Pipeline Position

```
ESPN APIs --[python]--> wehoop-wnba-raw --[push trigger]--> wehoop-wnba-data [HERE]
                                                                 |
                                                                 | sportsdataverse_save()
                                                                 v
                                                           sportsdataverse-data releases
                                                                 |
                                                                 | load_wnba_*()
                                                                 v
                                                             wehoop R package
```

CI in `.github/workflows/daily_wnba.yml` runs on schedule, on
`repository_dispatch: daily_wnba_data` (fired by `wehoop-wnba-raw`), and
on manual `workflow_dispatch`. It calls
`scripts/daily_wnba_R_processor.sh`, which loops the season range and
runs the three creation scripts in order.

## Build & Development Commands

```sh
# Full daily flow for one or more seasons (CI entry point)
bash scripts/daily_wnba_R_processor.sh -s 2025 -e 2025

# Or call the creation scripts directly
Rscript R/espn_wnba_01_pbp_creation.R         -s 2025 -e 2025
Rscript R/espn_wnba_02_team_box_creation.R    -s 2025 -e 2025
Rscript R/espn_wnba_03_player_box_creation.R  -s 2025 -e 2025
```

The creation scripts pull schedule + per-game JSON from
`https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/...`,
parse via `wehoop::espn_wnba_pbp()` family functions, write per-season
artifacts locally under `wnba/`, and call
`sportsdataversedata::sportsdataverse_save()` with
`file_types = c("rds", "csv", "parquet")` to upload to release tags
(`espn_wnba_pbp`, `espn_wnba_team_boxscores`, `espn_wnba_player_boxscores`,
`espn_wnba_schedules`). PBP CSVs are gzipped (`play_by_play_{y}.csv.gz`)
to keep the release artifacts manageable.

## Project Structure

```
R/
  0000_create_wehoop_releases_init.R   # One-shot release-tag bootstrapper
  0001_push_existing_release_data.R    # Backfill existing local data into releases
  espn_wnba_01_pbp_creation.R          # Per-season PBP compile + upload
  espn_wnba_02_team_box_creation.R     # Per-season team box compile + upload
  espn_wnba_03_player_box_creation.R   # Per-season player box compile + upload
  minify_json_folders.R                # Strip whitespace from json/raw blobs
scripts/
  daily_wnba_R_processor.sh            # CI entry point — loops seasons, commits, pushes
.github/workflows/
  daily_wnba.yml                       # Scheduled + dispatch + manual triggers
DESCRIPTION                            # R deps (uses wehoop, sportsdataversedata, piggyback)
requirements.txt                       # Python deps (used by helpers if present)
```

## Cross-Repo References

- Shared coding conventions, tidyverse style, cli messaging, ESPN wrapper pattern: <https://github.com/sportsdataverse/wehoop/blob/main/CLAUDE.md>
- Upstream raw data source: <https://github.com/sportsdataverse/wehoop-wnba-raw>
- Downstream consumer: <https://github.com/sportsdataverse/wehoop>
- Sister repo (same shape, different sport): <https://github.com/sportsdataverse/wehoop-wbb-data>

The actual ESPN parser lives in `wehoop`. The creation scripts here are
thin compile-and-upload wrappers; bug fixes to per-game parsing belong in
`wehoop` itself.

## Project-Specific Gotchas

- The three creation scripts MUST run in order (`01_pbp` → `02_team_box` → `03_player_box`). 02/03 depend on outputs from 01.
- PBP CSV is written as `wnba/pbp/csv/play_by_play_{year}.csv.gz` (gzipped) — different from the WBB sister repo's plain `.csv`. Don't drop the `.gz` suffix without aligning the release-side consumer in `wehoop`.
- `sportsdataversedata::sportsdataverse_save()` with `file_types = c("rds", "csv", "parquet")` is the upload boundary. Set `SPORTSDATAVERSE.UPLOAD.MAX_TIMES` (CI sets `20`) to control retry budget on flaky GitHub release uploads.
- The CI workflow runs on `windows-latest`. R-version- or platform-sensitive code should be tested with that in mind.
- `scripts/daily_wnba_R_processor.sh` does `git pull` / `git commit` / `git push` between seasons — local artifacts under `wnba/` are committed back to this repo. Don't add new files there casually.

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(pbp): expose ESPN shot-clock columns in play_by_play export
fix(player-box): fix unicode handling in player names for OT games
chore(release): bump piggyback retry budget for daily_wnba.yml
docs: clarify pipeline diagram in README
```

Prefer scoped subjects. Use `type!:` or a `BREAKING CHANGE:` footer for
breaking changes. Split unrelated work into separate commits.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
