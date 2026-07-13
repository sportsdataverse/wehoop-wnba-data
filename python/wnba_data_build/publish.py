"""Release publishing -- per-file ``gh release upload --clobber`` (create-if-missing).

Port of the R ``sportsdataverse_save`` upload. Multi-asset globs silently drop
large files, so upload one file at a time. ``runner``/``exists_check`` are
injectable for hermetic tests.

WNBA deltas vs the WBB publisher: the big-three tree csvs are ``.csv.gz``
while the release-asset contract stays plain ``.csv`` (decompressed to a temp
file before upload), and the per-dataset ``wnba_<ds>_in_data_repo.csv``
manifest is uploaded alongside -- wehoop exports ``load_wnba_*_manifest()``
loaders, so the manifest assets are load-bearing.
"""

from __future__ import annotations

import gzip
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Callable

from wnba_data_build._logging import get_logger, human_size
from wnba_data_build.config import DatasetSpec

_LEAGUE = "wnba"

DEFAULT_REPO = "sportsdataverse/sportsdataverse-data"

log = get_logger()


def _gh(args: list[str]) -> None:
    subprocess.run(["gh", *args], check=True)


def _gh_release_exists(tag: str, repo: str) -> bool:
    return (
        subprocess.run(
            ["gh", "release", "view", tag, "--repo", repo],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    )


def _dataset_files(spec: DatasetSpec, season: int, base: Path) -> list[Path]:
    root = base / spec.dataset
    cands = [
        root / "parquet" / f"{spec.stem}_{season}.parquet",
        root / "csv" / f"{spec.stem}_{season}.csv",
        # Manifest asset name == file name (R manifest_upload_helper contract).
        root / f"{_LEAGUE}_{spec.dataset}_in_data_repo.csv",
    ]
    files = [f for f in cands if f.exists()]
    # Tree csv is gzipped for the big three; the release asset stays plain
    # .csv (R sportsdataverse_save wrote its own plain csv). Decompress to a
    # temp file carrying the asset name.
    gz = root / "csv" / f"{spec.stem}_{season}.csv.gz"
    if spec.csv_suffix == ".csv.gz" and gz.exists():
        tmp = Path(tempfile.mkdtemp(prefix="wnba_publish_")) / f"{spec.stem}_{season}.csv"
        with gzip.open(gz, "rb") as src, open(tmp, "wb") as dst:
            shutil.copyfileobj(src, dst)
        files.insert(1, tmp)
    return files


def publish_dataset(
    spec: DatasetSpec,
    season: int,
    *,
    base: str | Path = "wnba",
    repo: str = DEFAULT_REPO,
    dry_run: bool = False,
    runner: Callable[[list[str]], None] | None = None,
    exists_check: Callable[[str, str], bool] | None = None,
) -> dict:
    """Upload a dataset/season's parquet + csv to the release, creating it if missing.

    Args:
        spec: Dataset spec (``dataset``/``stem``/``tag``) from ``config.REGISTRY``.
        season: Season year; must match the files already written by ``io.write_dataset``.
        base: Root directory containing ``{dataset}/{parquet,csv}/...``.
        repo: ``owner/repo`` slug for the release target.
        dry_run: If True, skip all ``gh`` calls and print the would-be uploads.
        runner: Injectable ``gh`` arg-list executor; defaults to a real subprocess call.
        exists_check: Injectable ``(tag, repo) -> bool`` release-existence check.

    Returns:
        dict: ``{"tag": ..., "files": [...], "uploaded": <count>}``.

    Example:
        Quick start::

            from wnba_data_build.config import REGISTRY
            from wnba_data_build import publish
            publish.publish_dataset(REGISTRY["team_box"], 2025)
    """
    run = runner or _gh
    exists = exists_check or _gh_release_exists
    files = _dataset_files(spec, season, Path(base))
    if not files:
        log.warning("%s %s: no files to publish under %s", spec.dataset, season, base)
    if not dry_run and not exists(spec.tag, repo):
        log.info("release %s missing on %s -- creating it", spec.tag, repo)
        run(
            [
                "release",
                "create",
                spec.tag,
                "--repo",
                repo,
                "--title",
                spec.tag,
                "--notes",
                f"{spec.tag} (WNBA dataset, Python-built).",
            ]
        )
    count = 0
    for f in files:
        size = human_size(f.stat().st_size)
        if dry_run:
            log.info("[dry-run] upload %s (%s) -> %s:%s", f, size, repo, spec.tag)
            continue
        log.info("uploading %s (%s) -> %s:%s", f.name, size, repo, spec.tag)
        run(["release", "upload", spec.tag, str(f), "--repo", repo, "--clobber"])
        count += 1
        log.info("uploaded %s -> %s (asset %d/%d)", f.name, spec.tag, count, len(files))
    return {"tag": spec.tag, "files": [str(f) for f in files], "uploaded": count}


def publish_files(
    tag: str,
    files: list[Path],
    *,
    repo: str = DEFAULT_REPO,
    dry_run: bool = False,
    runner: Callable[[list[str]], None] | None = None,
) -> dict:
    """Upload arbitrary already-written files to a release tag (extras path).

    Used for the season-independent schedule extras
    (``wnba_schedule_master`` + ``wnba_games_in_data_repo``) that
    ``espn_wnba_03_player_box_creation.R`` publishes to the schedules tag.
    The tag is assumed to exist (the per-season publish creates it).

    Args:
        tag: Release tag to upload to.
        files: Files to upload; the file name becomes the asset name.
        repo: ``owner/repo`` slug for the release target.
        dry_run: If True, log the would-be uploads without calling ``gh``.
        runner: Injectable ``gh`` arg-list executor for hermetic tests.

    Returns:
        dict: ``{"tag": ..., "files": [...], "uploaded": <count>}``.
    """
    run = runner or _gh
    count = 0
    for f in files:
        size = human_size(f.stat().st_size)
        if dry_run:
            log.info("[dry-run] upload %s (%s) -> %s:%s", f, size, repo, tag)
            continue
        log.info("uploading %s (%s) -> %s:%s", f.name, size, repo, tag)
        run(["release", "upload", tag, str(f), "--repo", repo, "--clobber"])
        count += 1
        log.info("uploaded %s -> %s (asset %d/%d)", f.name, tag, count, len(files))
    return {"tag": tag, "files": [str(f) for f in files], "uploaded": count}
