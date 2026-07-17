"""Dataset registry -- one row per released WNBA dataset.

Mirrors each ``espn_wnba_NN_*_creation.R`` script: ``(dataset, stem, tag,
reshaper)`` where ``reshaper`` keys into ``wnba_data_build.reshapers.RESHAPERS``.
Tags are verbatim from ``wehoop::load_wnba_*`` URL builders -- do not rename.
"""

from __future__ import annotations

from dataclasses import dataclass

RAW_ROOT_ENV = "WEHOOP_WNBA_RAW_ROOT"  # sibling wehoop-wnba-raw checkout root
_T = "espn_wnba_"

# The manifest's source_endpoint records the PUBLIC raw URL the dataset was
# compiled from -- verbatim what the R scripts glue -- regardless of whether
# this run actually read from a local checkout or over HTTP.
_RAW = "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba"

# --- rds contract -------------------------------------------------------------
# wehoop::load_wnba_* reads .rds EXCLUSIVELY, so the rds is not a courtesy
# format -- it is the R package's entire read path. Python writes it natively
# via sportsdataverse._rds.write_rds (byte-validated against R's saveRDS);
# there is no R serialize step.
#
# These reproduce wehoop:::make_wehoop_data() + sportsdataversedata::
# sportsdataverse_save() exactly, in the attribute order every published asset
# already carries: class, wehoop_timestamp, wehoop_type,
# sportsdataverse_type, sportsdataverse_timestamp. The class is load-bearing --
# wehoop registers print.wehoop_data on it.
RDS_CLASS: tuple[str, ...] = ("wehoop_data", "tbl_df", "tbl", "data.table", "data.frame")
RDS_ATTR_PREFIX = "wehoop"
RDS_TYPE_TEMPLATE = "ESPN WNBA {dataset} from wehoop data repository"


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
        manifest_endpoint: ``source_endpoint`` template for the dataset's
            manifest row (``{season}`` is substituted), or None for the
            datasets R does NOT manifest. Exactly the 8 datasets with a
            template here have a ``load_wnba_*_manifest()`` loader in wehoop;
            writing a manifest for the others would publish an asset nothing
            reads.
    """

    dataset: str
    stem: str
    tag: str
    reshaper: str
    csv_suffix: str = ".csv"
    manifest_endpoint: str | None = None


REGISTRY: dict[str, DatasetSpec] = {
    "pbp": DatasetSpec("pbp", "play_by_play", _T + "pbp", "pbp", csv_suffix=".csv.gz"),
    "schedules": DatasetSpec("schedules", "wnba_schedule", _T + "schedules", "schedules"),
    "shots": DatasetSpec(
        "shots",
        "shots",
        _T + "shots",
        "shots",
        manifest_endpoint="derived from espn_wnba pbp",
    ),
    "team_box": DatasetSpec(
        "team_box", "team_box", _T + "team_boxscores", "team_box", csv_suffix=".csv.gz"
    ),
    "player_box": DatasetSpec(
        "player_box", "player_box", _T + "player_boxscores", "player_box", csv_suffix=".csv.gz"
    ),
    "rosters": DatasetSpec(
        "rosters",
        "rosters",
        _T + "rosters",
        "rosters",
        manifest_endpoint=_RAW + "/team_rosters/json/{season}/<team_id>.json",
    ),
    "player_season_stats": DatasetSpec(
        "player_season_stats",
        "player_season_stats",
        _T + "player_season_stats",
        "player_season_stats",
        manifest_endpoint=_RAW + "/player_season_stats/json/{season}/<athlete_id>.json",
    ),
    # Athlete identity + bio. NEW dataset -- no R creation script exists, and
    # nothing published this before: the player_season_stats payload carries no
    # identity at all (not even the athlete id -- only the filename does).
    # NB: unlike this league's player_season_stats, the raw tree is FLAT (no
    # {season} segment) -- a core record is per-athlete and the core-v2 athlete
    # resource takes no season param. "Who played in season Y" comes from the
    # built player_box.
    "player_core": DatasetSpec(
        "player_core",
        "player_core",
        _T + "player_core",
        "player_core",
        # NO manifest_endpoint: a manifest is the contract for an R
        # load_wnba_<ds>_manifest() loader, and player_core has no loader yet --
        # manifesting it would publish an asset nothing reads.
    ),
    "team_season_stats": DatasetSpec(
        "team_season_stats",
        "team_season_stats",
        _T + "team_season_stats",
        "team_season_stats",
        # NB: the raw dir is team_stats, not team_season_stats.
        manifest_endpoint=_RAW + "/team_stats/json/{season}/<team_id>.json",
    ),
    "standings": DatasetSpec(
        "standings",
        "standings",
        _T + "standings",
        "standings",
        manifest_endpoint=_RAW + "/standings/json/{season}.json",
    ),
    "game_rosters": DatasetSpec(
        "game_rosters",
        "game_rosters",
        _T + "game_rosters",
        "game_rosters",
        manifest_endpoint=_RAW + "/game_rosters/json/<game_id>.json",
    ),
    "officials": DatasetSpec(
        "officials",
        "officials",
        _T + "officials",
        "officials",
        manifest_endpoint=_RAW + "/officials/json/<game_id>.json",
    ),
    # WNBA-only: annual draft compiled from the single wnba/draft/json/{year}.json
    # (espn_wnba_08_draft_creation.R); runs on annual_wnba_draft.yml, not daily.
    "draft": DatasetSpec(
        "draft",
        "draft",
        _T + "draft",
        "draft",
        manifest_endpoint=_RAW + "/draft/json/{season}.json",
    ),
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
