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
import hashlib
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Callable

import polars as pl
from sportsdataverse.release import upload_release_sidecars

from wnba_data_build import io as build_io
from wnba_data_build._logging import get_logger, human_size
from wnba_data_build.config import PKG_FUNCTION, DatasetSpec

_LEAGUE = "wnba"

DEFAULT_REPO = "sportsdataverse/sportsdataverse-data"

log = get_logger()


def _gh(args: list[str]) -> None:
    subprocess.run(["gh", *args], check=True, stderr=subprocess.PIPE, text=True)


def _release_digests(tag: str, repo: str) -> dict[str, str] | None:
    """``{asset name: "sha256:<hex>"}`` for *tag* in ONE API call; ``None`` = no release.

    The release object embeds every asset with a GitHub-computed ``digest``, so
    one listing answers both "does the release exist" and "which assets
    changed" -- the droplet shares one 5,000/h quota. Only a 404 counts as
    absence; a rate limit / auth / network failure raises.
    """
    proc = subprocess.run(
        [
            "gh",
            "api",
            f"repos/{repo}/releases/tags/{tag}",
            "--jq",
            r'.assets[] | "\(.name)\t\(.digest)"',
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode:
        if "404" in (proc.stderr or ""):
            return None
        raise RuntimeError(
            f"gh api releases/tags/{tag} --repo {repo} failed: {proc.stderr.strip()}"
        )
    return dict(ln.split("\t", 1) for ln in proc.stdout.splitlines() if "\t" in ln)


def _sha256(path: Path) -> str:
    with open(path, "rb") as fh:
        return "sha256:" + hashlib.file_digest(fh, "sha256").hexdigest()


def _manifest_asset(spec: DatasetSpec, base: Path) -> Path | None:
    """Collapse the manifest append-log to one row per season for the release.

    R's ``upload_wnba_manifest`` uploads
    ``distinct(season, .keep_all = TRUE) %>% arrange(season)`` -- and dplyr's
    distinct keeps the FIRST occurrence, so the published row_count freezes at
    a season's first-ever run (the 2026 shots asset still advertised 2047 rows
    against a 30,628-row dataset). We keep the LATEST row per season instead,
    so the manifest describes the asset actually published alongside it. That
    is a deliberate divergence from R; completed seasons are unaffected (their
    first run == their last).
    """
    src = build_io.manifest_path(spec, base)
    if spec.manifest_endpoint is None or not src.exists():
        return None
    latest = (
        pl.read_csv(src).unique(subset=["season"], keep="last", maintain_order=True).sort("season")
    )
    tmp = Path(tempfile.mkdtemp(prefix="wnba_manifest_")) / src.name
    latest.write_csv(tmp)
    return tmp


def _dataset_files(spec: DatasetSpec, season: int, base: Path) -> list[Path]:
    root = build_io.dataset_dir(spec, base)
    pq = root / "parquet" / f"{spec.stem}_{season}.parquet"
    cands = [
        pq,
        # .rds is wehoop::load_wnba_*'s ONLY read path -- publishing the parquet
        # without it silently freezes every downstream loader.
        root / "rds" / f"{spec.stem}_{season}.rds",
        root / "csv" / f"{spec.stem}_{season}.csv",
    ]
    files = [f for f in cands if f.exists()]
    if not spec.write_tree_csv and pq.exists():
        # Crosswalks commit no tree csv (their crosswalk/*.csv IS the
        # manifest), but R's file_types = c("rds", "csv", "parquet") still
        # ships a plain .csv asset -- generate it from the parquet.
        tmp = Path(tempfile.mkdtemp(prefix="wnba_publish_")) / f"{spec.stem}_{season}.csv"
        pl.read_parquet(pq).write_csv(tmp)
        files.append(tmp)
    # Manifest asset name == file name (R manifest_upload_helper contract).
    manifest = _manifest_asset(spec, base)
    if manifest is not None:
        files.append(manifest)
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


def _stamp(tag: str, run: Callable[[list[str]], None], repo: str) -> None:
    """Re-stamp a tag's timestamp / package_function sidecars after an upload.

    R's sportsdataverse_save() attaches these to every published tag; the
    Python publisher dropped them, which left the tag carrying a timestamp.json
    frozen at the last R run while the data kept moving. Runs LAST so the stamp
    reflects the finished upload, and only when something actually uploaded --
    a stamp on a no-op run would claim data moved when it did not. Goes through
    the same injected ``run`` as the data assets so tests stay offline.
    """
    uploaded = upload_release_sidecars(
        tag, runner=run, pkg_function=PKG_FUNCTION.get(tag), repo=repo
    )
    log.info("stamped %s with %s", tag, ", ".join(uploaded))


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
    files = _dataset_files(spec, season, Path(base))
    if not files:
        log.warning("%s %s: no files to publish under %s", spec.dataset, season, base)
    remote: dict[str, str] | None = {}
    if dry_run:
        pass
    elif exists_check is not None:  # tests: old existence probe, upload everything
        remote = {} if exists_check(spec.tag, repo) else None
    else:
        remote = _release_digests(spec.tag, repo)
    if remote is None:
        log.info("release %s missing on %s -- creating it", spec.tag, repo)
        try:
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
        except subprocess.CalledProcessError as exc:
            # Belt-and-suspenders for the race exists() didn't catch (e.g. a
            # concurrent run created the tag between the check and here).
            stderr = (exc.stderr or "").lower() if isinstance(exc.stderr, str) else ""
            if "already exists" in stderr:
                log.info("release %s already exists on %s -- continuing", spec.tag, repo)
            else:
                raise
        remote = {}
    # The rds embeds a write timestamp and the manifest a generated_at, so neither
    # ever matches itself; both (and the csv) derive from the parquet. A season
    # whose parquet matches and whose siblings are all on the release is skipped
    # whole. The parquet uploads LAST, so a matching parquet means its siblings'
    # previous upload completed.
    files.sort(key=lambda f: f.suffix == ".parquet")
    pq_asset = files[-1] if files and files[-1].suffix == ".parquet" else None
    if (
        not dry_run
        and pq_asset is not None
        and remote.get(pq_asset.name) == _sha256(pq_asset)
        and all(f.name in remote for f in files)
    ):
        log.info("%s %s: unchanged on %s -- skipped", spec.dataset, season, spec.tag)
        return {"tag": spec.tag, "files": [str(f) for f in files], "uploaded": 0}
    count = 0
    for f in files:
        size = human_size(f.stat().st_size)
        if dry_run:
            log.info("[dry-run] upload %s (%s) -> %s:%s", f, size, repo, spec.tag)
            continue
        if f is not pq_asset and remote.get(f.name) == _sha256(f):
            log.info("unchanged %s on %s -- skipped", f.name, spec.tag)
            continue
        log.info("uploading %s (%s) -> %s:%s", f.name, size, repo, spec.tag)
        run(["release", "upload", spec.tag, str(f), "--repo", repo, "--clobber"])
        count += 1
        log.info("uploaded %s -> %s (asset %d/%d)", f.name, spec.tag, count, len(files))
    if count:
        _stamp(spec.tag, run, repo)
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
    remote = {} if dry_run or runner is not None else (_release_digests(tag, repo) or {})
    count = 0
    for f in files:
        size = human_size(f.stat().st_size)
        if dry_run:
            log.info("[dry-run] upload %s (%s) -> %s:%s", f, size, repo, tag)
            continue
        if remote.get(f.name) == _sha256(f):
            log.info("unchanged %s on %s -- skipped", f.name, tag)
            continue
        log.info("uploading %s (%s) -> %s:%s", f.name, size, repo, tag)
        run(["release", "upload", tag, str(f), "--repo", repo, "--clobber"])
        count += 1
        log.info("uploaded %s -> %s (asset %d/%d)", f.name, tag, count, len(files))
    if count:
        _stamp(tag, run, repo)
    return {"tag": tag, "files": [str(f) for f in files], "uploaded": count}
