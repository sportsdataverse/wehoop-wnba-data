One-time bootstraps — run from the repo root; not part of any scheduled pipeline.
- `0000_create_wehoop_releases_init.R` — idempotently create this repo's `espn_wnba_*` release tags on `sportsdataverse/sportsdataverse-data`; re-run when a new creation script/tag lands.
- `0001_push_existing_release_data.R` — historical one-shot that pushed the pre-existing local `{wbb,wnba}/<dataset>/rds/` trees to their release tags.
