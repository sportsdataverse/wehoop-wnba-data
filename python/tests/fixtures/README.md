# Test fixtures — provenance

Two trees, both real captures. Nothing here is synthetic.

## `raw/` — inputs (mirror of `wehoop-wnba-raw`)

Byte-for-byte copies of files from
<https://github.com/sportsdataverse/wehoop-wnba-raw>, retaining the upstream
`wnba/<dataset>/...` layout so `raw_root=tests/fixtures/raw` is a drop-in for a
real checkout. Captured 2026-07-12.

| Path | Contents |
|---|---|
| `wnba/json/final/{401820325,401736112,401736113}.json` | 2025 games |
| `wnba/json/final/{401856890,401856891,401856892}.json` | 2026 games |
| `wnba/game_rosters/json/*.json`, `wnba/officials/json/*.json` | sidecars for the three 2025 games |
| `wnba/team_rosters/json/2025/{3,5}.json`, `wnba/team_stats/json/2025/{3,5}.json` | two teams |
| `wnba/player_season_stats/json/2025/{924,2529047,2955898,3058893,3142055}.json` | five athletes of those teams |
| `wnba/standings/json/2025.json`, `wnba/draft/json/2026.json` | whole-season payloads |
| `wnba/schedules/parquet/wnba_schedule_{2025,2026}.parquet` | **trimmed** to the three fixture games of each season (the only edit to any raw file — a row filter, no column or value changes) |

The `json/final/*.json` payloads are the *processed* sdv-py vintage the raw repo
actually commits (flat dotted play keys, engineered features baked in), not the
untouched ESPN summary — that is what the R producer reads, so it is what we
read.

## `released/` — oracles (the R producer's published output)

The published `sportsdataverse-data` release assets, downloaded from the release
CDN on 2026-07-12 and **pre-filtered to the fixture games/teams/athletes above**
(a row filter only — no column, dtype, or value changes). These are the
golden masters the parity tests assert against: they are what the R pipeline
actually shipped, so matching them is the definition of a correct port.

| File | Tag | Rows × cols | Filter |
|---|---|---|---|
| `play_by_play_2025.parquet` | `espn_wnba_pbp` | 1235 × 64 | 3 games |
| `play_by_play_2026.parquet` | `espn_wnba_pbp` | 1350 × 64 | 3 games |
| `shots_2025.parquet` | `espn_wnba_shots` | 554 × 15 | 3 games |
| `team_box_2025.parquet` | `espn_wnba_team_boxscores` | 6 × 57 | 3 games |
| `player_box_2025.parquet` | `espn_wnba_player_boxscores` | 68 × 57 | 3 games |
| `wnba_schedule_2025.parquet` | `espn_wnba_schedules` | 3 × 77 | 3 games |
| `game_rosters_2025.parquet` | `espn_wnba_game_rosters` | 68 × 22 | 3 games |
| `officials_2025.parquet` | `espn_wnba_officials` | 10 × 11 | 3 games |
| `rosters_2025.parquet` | `espn_wnba_rosters` | 27 × 36 | teams 3, 5 |
| `team_season_stats_2025.parquet` | `espn_wnba_team_season_stats` | 90 × 16 | teams 3, 5 |
| `player_season_stats_2025.parquet` | `espn_wnba_player_season_stats` | 215 × 16 | 5 athletes |
| `standings_2025.parquet` | `espn_wnba_standings` | 299 × 24 | none (full asset) |
| `draft_2026.parquet` | `espn_wnba_draft` | 45 × 35 | none (full asset) |

`standings` and `draft` build from a single whole-season payload, so their
oracles need no filtering. `draft` is 2026 because that tag publishes 2026 only.

Re-fetch an asset with:

```sh
curl -L -o <file> \
  https://github.com/sportsdataverse/sportsdataverse-data/releases/download/<tag>/<file>
```

(Use the release CDN, not the `gh` API — the API rate-limits on a full re-pull.)
