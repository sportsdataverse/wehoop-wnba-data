"""Per-season build driver -- polars port of the R ``wnba_<dataset>_games(y)`` loop.

Enumerate season game-ids -> read each final.json -> reshape (delegating to
the sdv-py producer) -> drift-safe union -> write -> (opt) publish. Per-game
failures are swallowed (R tryCatch parity) so one bad payload can't sink the
season.
"""

from __future__ import annotations

import time
from pathlib import Path

import polars as pl

from wnba_data_build import ingest, io, publish, reshapers
from wnba_data_build._logging import get_logger
from wnba_data_build.config import REGISTRY

log = get_logger()

# Non-TTY runs (CI) get a heartbeat line every N games instead of a progress bar.
_PROGRESS_EVERY = 250


def build_season(
    dataset: str,
    season: int,
    *,
    base: str | Path = "wnba",
    raw_root: str | Path | None = None,
    publish_release: bool = False,
    dry_run: bool = False,
) -> pl.DataFrame:
    """Build one dataset/season from the raw checkout: reshape, union, write, (opt) publish.

    Args:
        dataset: Key into ``config.REGISTRY`` (e.g. ``"team_box"``).
        season: Season year to build.
        base: Output root directory for ``io.write_dataset``.
        raw_root: Sibling ``wehoop-wnba-raw`` checkout root (arg > ``WEHOOP_WNBA_RAW_ROOT`` env).
        publish_release: If True, upload the written files via ``publish.publish_dataset``.
        dry_run: If True, run the publish step in dry-run mode (no ``gh`` calls).

    Returns:
        pl.DataFrame: The built season frame, or an empty frame if no games qualified.

    Example:
        Quick start::

            from wnba_data_build.build import build_season
            df = build_season("team_box", 2025)
            print(df.shape)
    """
    spec = REGISTRY[dataset]
    if dataset not in reshapers.SEASON_BUILDERS and spec.reshaper not in reshapers.RESHAPERS:
        # The three crosswalks build from LIVE ESPN+Torvik+Fox inputs (not the
        # raw repo) via wehoop::wnba_*_crosswalk; they stay on the R scripts
        # (wnba_1{1,2,3}_*_creation.R) until the Torvik/Fox source surfaces are
        # ported to sportsdataverse.
        raise NotImplementedError(f"{dataset}: crosswalks still build via the R creation scripts")
    root = ingest.raw_root(raw_root)
    started = time.monotonic()
    mode = "http" if isinstance(root, str) else "disk"
    if dataset in reshapers.SEASON_BUILDERS:
        # Season-level datasets (schedules/shots/...) build from the raw season
        # tree and/or already-built parquets -- no per-game loop.
        log.info("%s %s: season-level build starting (raw=%s via %s)", dataset, season, root, mode)
        out = reshapers.SEASON_BUILDERS[dataset](season, raw_root=root, base=Path(base))
        if out.height == 0:
            log.warning(
                "%s %s: season-level build produced 0 rows; nothing written", dataset, season
            )
            return out
        io.write_dataset(out, spec, season, base=base)
        if publish_release or dry_run:
            publish.publish_dataset(spec, season, base=base, dry_run=dry_run)
        if dataset == "schedules":
            # espn_wnba_03 tail: rebuild the full-history master + PBP==TRUE
            # extras from the committed per-season parquets and publish them
            # to the schedules tag (WNBA actively ships these; WBB doesn't).
            master, games = reshapers.build_schedule_extras(base=Path(base))
            if master.height and not games.height:
                # games = the PBP==TRUE filter. Master rows but zero PBP games
                # means the pbp parquets are missing (a failed/skipped pbp
                # build), not that no game has pbp -- publishing here would
                # overwrite wnba_games_in_data_repo with an empty asset.
                log.error(
                    "schedules %s: master has %d rows but ZERO games flagged PBP -- "
                    "refusing to publish the schedule extras. Build pbp first.",
                    season,
                    master.height,
                )
            elif master.height:
                extra_files = io.write_schedule_extras(master, games, base=base)
                if publish_release or dry_run:
                    publish.publish_files(spec.tag, extra_files, dry_run=dry_run)
        log.info(
            "%s %s: done -- %d rows in %.1fs",
            dataset,
            season,
            out.height,
            time.monotonic() - started,
        )
        return out
    game_ids = ingest.season_game_ids(season, raw_root=root)
    if not game_ids:
        log.warning(
            "%s %s: no game_json games in the season schedule; nothing built", dataset, season
        )
        return pl.DataFrame()
    log.info(
        "%s %s: per-game build starting -- %d games (raw=%s via %s)",
        dataset,
        season,
        len(game_ids),
        root,
        mode,
    )
    reshape = reshapers.RESHAPERS[spec.reshaper]
    frames: list[pl.DataFrame] = []
    missing = 0
    failed = 0

    def _read_reshape(gid: int):
        final = ingest.read_final(gid, raw_root=root)
        if final is None:
            return ("missing", None)
        try:
            frame = reshape(final, season=season, game_id=gid)
        except Exception as e:  # R tryCatch(...) -> NULL parity
            log.warning("%s %s: reshape failed for game %s: %s", dataset, season, gid, e)
            return ("failed", None)
        return ("ok", frame if (frame is not None and frame.height) else None)

    # Fan the per-game reads+reshapes out over a thread pool -- the reads are
    # I/O-bound (HTTP in CI), so this is the difference between a minutes-long
    # and a tens-of-minutes-long season. Results come back in game_ids order,
    # so the concat + stable sort below stay byte-identical to the serial build.
    for n, (status, frame) in enumerate(ingest.parallel_map(_read_reshape, game_ids), start=1):
        if status == "missing":
            missing += 1
        elif status == "failed":
            failed += 1
        elif frame is not None:
            frames.append(frame)
        if n % _PROGRESS_EVERY == 0:
            log.info("%s %s: %d/%d games collected", dataset, season, n, len(game_ids))
    if missing:
        log.warning(
            "%s %s: %d/%d games had no readable payload", dataset, season, missing, len(game_ids)
        )
    if failed:
        log.warning("%s %s: %d/%d games failed to reshape", dataset, season, failed, len(game_ids))
    if not frames:
        log.warning("%s %s: 0 games reshaped; nothing written", dataset, season)
        return pl.DataFrame()
    out = pl.concat(frames, how="diagonal_relaxed")
    # R: every per-game season compile is arrange(desc(game_date)) before
    # write/publish (stable, NA last).
    if "game_date" in out.columns:
        out = out.sort("game_date", descending=True, nulls_last=True, maintain_order=True)
    # WNBA season-level fixups (e.g. espn_wnba_01's type_abbreviation backfill).
    if spec.reshaper in reshapers.SEASON_POSTPROCESS:
        out = reshapers.SEASON_POSTPROCESS[spec.reshaper](out)
    io.write_dataset(out, spec, season, base=base)
    if publish_release or dry_run:
        publish.publish_dataset(spec, season, base=base, dry_run=dry_run)
    log.info(
        "%s %s: done -- %d rows from %d/%d games in %.1fs",
        dataset,
        season,
        out.height,
        len(frames),
        len(game_ids),
        time.monotonic() - started,
    )
    return out
