"""Dataset registry -- one row per released WNBA dataset.

Mirrors each ``espn_wnba_NN_*_creation.R`` script: ``(dataset, stem, tag,
reshaper)`` where ``reshaper`` keys into ``wnba_data_build.reshapers.RESHAPERS``.
Tags are verbatim from ``wehoop::load_wnba_*`` URL builders -- do not rename.
"""

from __future__ import annotations

from dataclasses import dataclass

RAW_ROOT_ENV = "WEHOOP_WNBA_RAW_ROOT"  # sibling wehoop-wnba-raw checkout root
_T = "espn_wnba_"


@dataclass(frozen=True)
class DatasetSpec:
    """How to build one released dataset.

    Attributes:
        dataset: directory name under ``wnba/`` and the manifest key.
        stem: output file stem (``{stem}_{season}.parquet`` / ``.csv``).
        tag: the ``sportsdataverse-data`` release tag (load-bearing).
        reshaper: key into ``reshapers.RESHAPERS``.
        csv_suffix: tree csv extension. The WNBA repo commits the big-three
            csvs gzipped (``play_by_play_2025.csv.gz`` -- R ``fwrite`` to
            ``.csv.gz``); release assets stay plain ``.csv`` (publish
            decompresses to a temp file, matching ``sportsdataverse_save``).
    """

    dataset: str
    stem: str
    tag: str
    reshaper: str
    csv_suffix: str = ".csv"


REGISTRY: dict[str, DatasetSpec] = {
    "pbp": DatasetSpec("pbp", "play_by_play", _T + "pbp", "pbp", csv_suffix=".csv.gz"),
    "schedules": DatasetSpec("schedules", "wnba_schedule", _T + "schedules", "schedules"),
    "shots": DatasetSpec("shots", "shots", _T + "shots", "shots"),
    "team_box": DatasetSpec(
        "team_box", "team_box", _T + "team_boxscores", "team_box", csv_suffix=".csv.gz"
    ),
    "player_box": DatasetSpec(
        "player_box", "player_box", _T + "player_boxscores", "player_box", csv_suffix=".csv.gz"
    ),
    "rosters": DatasetSpec("rosters", "rosters", _T + "rosters", "rosters"),
    "player_season_stats": DatasetSpec(
        "player_season_stats",
        "player_season_stats",
        _T + "player_season_stats",
        "player_season_stats",
    ),
    "team_season_stats": DatasetSpec(
        "team_season_stats", "team_season_stats", _T + "team_season_stats", "team_season_stats"
    ),
    "standings": DatasetSpec("standings", "standings", _T + "standings", "standings"),
    "game_rosters": DatasetSpec(
        "game_rosters", "game_rosters", _T + "game_rosters", "game_rosters"
    ),
    "officials": DatasetSpec("officials", "officials", _T + "officials", "officials"),
    # WNBA-only: annual draft compiled from the single wnba/draft/json/{year}.json
    # (espn_wnba_08_draft_creation.R); runs on annual_wnba_draft.yml, not daily.
    "draft": DatasetSpec("draft", "draft", _T + "draft", "draft"),
    # crosswalks -- tag/stem confirmed via Task 0 discovery grep against
    # R/wnba_1{1,2,3}_*_creation.R: all three publish to the SAME shared
    # release tag "wnba_crosswalk" (not the per-dataset espn_wnba_* prefix
    # used by the per-game datasets above); stems match each script's
    # `file_name = glue::glue("wnba_{...}_crosswalk_{y}")`.
    "team_crosswalk": DatasetSpec(
        "team_crosswalk", "wnba_team_crosswalk", "wnba_crosswalk", "team_crosswalk"
    ),
    "schedule_crosswalk": DatasetSpec(
        "schedule_crosswalk", "wnba_schedule_crosswalk", "wnba_crosswalk", "schedule_crosswalk"
    ),
    "player_crosswalk": DatasetSpec(
        "player_crosswalk", "wnba_player_crosswalk", "wnba_crosswalk", "player_crosswalk"
    ),
}
