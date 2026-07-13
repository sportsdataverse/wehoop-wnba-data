"""Per-dataset reshapers -- each takes one game's final.json + returns a frame.

Every reshaper delegates the actual reshape to a ``sportsdataverse.wnba``
producer (thin league shims over the shared basketball implementations);
this module is just the registry + per-game glue. Signature contract:
``(final, *, season, game_id) -> pl.DataFrame``.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from sportsdataverse.wnba import (
    helper_wnba_play_by_play,
    helper_wnba_player_box,
    helper_wnba_schedule,
    helper_wnba_team_box,
)

from wnba_data_build._logging import get_logger

log = get_logger()


def team_box_reshaper(final: dict, *, season: int, game_id: int) -> pl.DataFrame:
    return helper_wnba_team_box(final)


def pbp_reshaper(final: dict, *, season: int, game_id: int) -> pl.DataFrame:
    return helper_wnba_play_by_play(final)


def player_box_reshaper(final: dict, *, season: int, game_id: int) -> pl.DataFrame:
    return helper_wnba_player_box(final)


RESHAPERS: dict = {
    "team_box": team_box_reshaper,
    "pbp": pbp_reshaper,
    "player_box": player_box_reshaper,
}

# --- season-level builders (no per-game loop) --------------------------------
# Signature contract: (season, *, raw_root, base) -> pl.DataFrame. Each reads
# the raw season tree and/or the already-built parquets under ``base``.

_SHOTS_COLS = (
    "game_id",
    "season",
    "period_number",
    "clock_display_value",
    "team_id",
    "athlete_id_1",
    "athlete_id_2",
    "type_id",
    "type_text",
    "scoring_play",
    "score_value",
    "coordinate_x",
    "coordinate_y",
    "coordinate_x_raw",
    "coordinate_y_raw",
)


def shots_from_pbp(pbp: pl.DataFrame) -> pl.DataFrame:
    """R espn_wnba_01 shots block: filter shooting plays, project the shot cols."""
    if pbp.is_empty():
        return pl.DataFrame()
    out = pbp.filter(pl.col("shooting_play") == True)  # noqa: E712
    return out.select([c for c in _SHOTS_COLS if c in out.columns])


def _built_game_ids(base: Path, dataset: str, stem: str, season: int) -> list[int]:
    p = base / dataset / "parquet" / f"{stem}_{season}.parquet"
    if not p.exists():
        # Fails open (every flag -> False), like R's empty-espn_df branch. Say so
        # loudly: if this is a pipeline-order violation (or a failed upstream
        # build) rather than a genuinely unbuilt season, the schedule would ship
        # PBP=FALSE for every game. build.py additionally refuses to publish the
        # schedule extras when that leaves zero PBP games.
        log.warning(
            "%s %s: no built parquet at %s -- schedule flags for it will all be False",
            dataset,
            season,
            p,
        )
        return []
    return (
        pl.read_parquet(p, columns=["game_id"])
        .get_column("game_id")
        .cast(pl.Int64)
        .unique()
        .to_list()
    )


def schedules_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    """Released schedule = raw schedule + casts/dates + PBP/team_box/player_box flags."""
    raw = pl.read_parquet(
        raw_root / "wnba" / "schedules" / "parquet" / f"wnba_schedule_{season}.parquet"
    )
    return helper_wnba_schedule(
        raw,
        pbp_game_ids=_built_game_ids(base, "pbp", "play_by_play", season),
        team_box_game_ids=_built_game_ids(base, "team_box", "team_box", season),
        player_box_game_ids=_built_game_ids(base, "player_box", "player_box", season),
    )


def shots_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    """Shots derive from the already-built play_by_play parquet (no extra I/O in R)."""
    p = base / "pbp" / "parquet" / f"play_by_play_{season}.parquet"
    if not p.exists():
        return pl.DataFrame()
    return shots_from_pbp(pl.read_parquet(p))


def _sidecar_builder(subdir: str, helper) -> object:
    """Per-game sidecar loop (R scripts 08/09): completed games, tryCatch skips."""

    def _build(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
        from wnba_data_build import ingest

        frames: list[pl.DataFrame] = []
        for gid in ingest.season_completed_game_ids(season, raw_root=raw_root):
            payload = ingest.read_final(gid, raw_root=raw_root, subdir=subdir)
            if payload is None:
                continue
            try:
                frame = helper(payload, season=season, game_id=gid)
            except Exception as e:  # R tryCatch(...) -> NULL parity
                log.warning("%s: parse failed for game %s: %s", subdir, gid, e)
                continue
            if frame.height:
                frames.append(frame)
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal_relaxed")

    return _build


def _game_rosters_builder() -> object:
    from sportsdataverse.wnba import helper_wnba_game_rosters

    return _sidecar_builder("game_rosters/json", helper_wnba_game_rosters)


def _officials_builder() -> object:
    from sportsdataverse.wnba import helper_wnba_officials

    return _sidecar_builder("officials/json", helper_wnba_officials)


def _per_entity_frames(
    subdir: str, season: int, raw_root: Path, helper, id_kw: str
) -> list[pl.DataFrame]:
    """R scripts 04/05/06: loop the season's per-entity JSONs, tryCatch skips."""
    from wnba_data_build import ingest

    frames: list[pl.DataFrame] = []
    for eid in ingest.season_dir_ids(subdir, season, raw_root=raw_root):
        payload = ingest.read_final(eid, raw_root=raw_root, subdir=f"{subdir}/json/{season}")
        if payload is None:
            continue
        try:
            frame = helper(payload, **{"season": season, id_kw: eid})
        except Exception as e:  # R tryCatch(...) -> NULL parity
            log.warning("%s: parse failed for entity %s: %s", subdir, eid, e)
            continue
        if frame.height:
            frames.append(frame)
    return frames


def _season_concat(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame()
    # R: season-level distinct().
    return pl.concat(frames, how="diagonal_relaxed").unique(maintain_order=True, keep="first")


def rosters_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    from sportsdataverse.wnba import helper_wnba_rosters

    return _season_concat(
        _per_entity_frames("team_rosters", season, raw_root, helper_wnba_rosters, "team_id")
    )


def team_season_stats_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    from sportsdataverse.wnba import helper_wnba_team_season_stats

    return _season_concat(
        _per_entity_frames("team_stats", season, raw_root, helper_wnba_team_season_stats, "team_id")
    )


def player_season_stats_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    from sportsdataverse.wnba import build_athlete_identity_lookup, helper_wnba_player_season_stats

    from wnba_data_build import ingest

    rosters = {
        tid: ingest.read_final(tid, raw_root=raw_root, subdir=f"team_rosters/json/{season}")
        for tid in ingest.season_dir_ids("team_rosters", season, raw_root=raw_root)
    }
    lookup = build_athlete_identity_lookup({t: r for t, r in rosters.items() if r})

    def _helper(payload: dict, *, season: int, athlete_id: int) -> pl.DataFrame:
        return helper_wnba_player_season_stats(
            payload, season=season, athlete_id=athlete_id, identity_lookup=lookup
        )

    return _season_concat(
        _per_entity_frames("player_season_stats", season, raw_root, _helper, "athlete_id")
    )


def standings_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    from sportsdataverse.wnba import helper_wnba_standings

    from wnba_data_build import ingest

    payload = ingest.read_final(season, raw_root=raw_root, subdir="standings/json")
    if payload is None:
        return pl.DataFrame()
    out = helper_wnba_standings(payload, season=season)
    # espn_wnba_07:190 ends in dplyr::distinct(). No dupes in today's payloads
    # (2025 is 299 rows either way), but a team nested under both a conference
    # and a league group would double up without this.
    return out.unique(maintain_order=True)


def draft_builder(season: int, *, raw_root: Path, base: Path) -> pl.DataFrame:
    """WNBA-only: the annual draft compiles from the single wnba/draft/json/{year}.json."""
    from sportsdataverse.wnba import helper_wnba_draft

    from wnba_data_build import ingest

    payload = ingest.read_final(season, raw_root=raw_root, subdir="draft/json")
    if payload is None:
        return pl.DataFrame()
    return helper_wnba_draft(payload, season=season)


SEASON_BUILDERS: dict = {
    "schedules": schedules_builder,
    "shots": shots_builder,
    "game_rosters": _game_rosters_builder(),
    "officials": _officials_builder(),
    "rosters": rosters_builder,
    "team_season_stats": team_season_stats_builder,
    "player_season_stats": player_season_stats_builder,
    "standings": standings_builder,
    "draft": draft_builder,
}


# --- season-level post-processing (after the per-game concat) -----------------


def pbp_season_postprocess(out: pl.DataFrame) -> pl.DataFrame:
    """espn_wnba_01: a season whose payload union lacks ``type_abbreviation``
    ships it as an all-null String column appended at the end."""
    if "type_abbreviation" not in out.columns and out.width > 1:
        out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("type_abbreviation"))
    return out


def team_box_season_postprocess(out: pl.DataFrame) -> pl.DataFrame:
    """espn_wnba_02:78-82 -- a season whose payload union lacks ``largest_lead``
    still ships it, as an all-null String column appended last.

    Not dead code: the released team_box assets for 2003-2011, 2013 and 2015
    carry ``largest_lead`` as an all-null column that exists *only* because of
    that R line. The Python helper's stat spread is payload-driven, so without
    this those 11 seasons would rebuild one column short and
    ``load_wnba_team_box(2003:2026)`` would bind divergent schemas.
    """
    if "largest_lead" not in out.columns and out.width > 1:
        out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias("largest_lead"))
    return out


SEASON_POSTPROCESS: dict = {
    "pbp": pbp_season_postprocess,
    "team_box": team_box_season_postprocess,
}


# --- schedule extras (master + games_in_data_repo) ----------------------------

# espn_wnba_03's master block re-casts every season file before binding
# (historical rds/parquet dtypes drift across seasons).
_MASTER_INT32_COLS = (
    "id",
    "game_id",
    "type_id",
    "status_type_id",
    "home_id",
    "home_venue_id",
    "home_conference_id",
    "home_score",
    "away_id",
    "away_venue_id",
    "away_conference_id",
    "away_score",
    "season",
    "season_type",
    "groups_id",
    "tournament_id",
    "venue_id",
)


def build_schedule_extras(*, base: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Port of espn_wnba_03's master-schedule block.

    Reads every committed per-season schedule parquet under
    ``{base}/schedules/parquet/``, homogenizes dtypes (the R block's
    ``as.integer`` casts + NY-tz ``game_date_time``/``game_date`` recompute),
    binds, dedupes, and sorts by ``date`` descending. Returns
    ``(wnba_schedule_master, wnba_games_in_data_repo)`` where the second is
    the ``PBP == TRUE`` filter of the first.
    """
    import re

    pq_dir = base / "schedules" / "parquet"
    files = [
        p
        for p in sorted(pq_dir.glob("wnba_schedule_*.parquet"))
        if re.fullmatch(r"wnba_schedule_\d{4}\.parquet", p.name)
    ]
    if not files:
        return pl.DataFrame(), pl.DataFrame()
    frames = []
    for f in files:
        df = pl.read_parquet(f)
        # Float64 intermediate keeps R as.integer semantics ("59.0" -> 59).
        df = df.with_columns(
            [
                pl.col(c).cast(pl.Float64, strict=False).cast(pl.Int32)
                for c in _MASTER_INT32_COLS
                if c in df.columns
            ]
        )
        if "status_display_clock" in df.columns:
            df = df.with_columns(pl.col("status_display_clock").cast(pl.Utf8))
        df = df.with_columns(
            pl.col("date")
            .str.replace(r"Z$", "")
            .str.strptime(pl.Datetime("us"), "%Y-%m-%dT%H:%M", strict=False)
            .dt.replace_time_zone("UTC")
            .dt.convert_time_zone("America/New_York")
            .alias("game_date_time")
        )
        df = df.with_columns(pl.col("game_date_time").dt.date().alias("game_date"))
        frames.append(df)
    master = (
        pl.concat(frames, how="diagonal_relaxed")
        .unique(maintain_order=True, keep="first")
        .sort("date", descending=True, nulls_last=True, maintain_order=True)
    )
    games = master.filter(pl.col("PBP") == True)  # noqa: E712
    return master, games
