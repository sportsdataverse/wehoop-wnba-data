"""Read the wehoop-wnba-raw tree — from a sibling checkout OR over HTTP.

The Python producer prefers the sibling checkout on disk (sdv-build-data
convention), but the raw repo is ~58GB — far beyond any CI runner — so
``raw_root`` may instead be the ``raw.githubusercontent.com`` base URL
(exactly what the R pipeline always read). Game payloads live at
``{raw_root}/wnba/json/final/{game_id}.json``; the per-season schedule at
``{raw_root}/wnba/schedules/parquet/wnba_schedule_{season}.parquet``.

HTTP mode details: per-game/per-entity JSON is cached under
``$WEHOOP_WNBA_CACHE`` (default ``.wnba_raw_cache``, gitignored) so the 11
dataset builds don't re-fetch the same payloads; the season schedule is
fetched fresh every call (its ``game_json``/status flags change daily);
directory listings use the GitHub contents API (PAT-aware via
``GITHUB_PAT``/``GH_TOKEN``), mirroring the R scripts' ``list_*_ids`` —
including their 1000-entries-per-directory API cap (no pagination).
"""

from __future__ import annotations

import io
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

from wnba_data_build.config import RAW_ROOT_ENV

_RAW_REPO_API = "https://api.github.com/repos/sportsdataverse/wehoop-wnba-raw/contents"


def build_workers() -> int:
    """Thread-pool size for the per-game/per-entity read+reshape fan-out.

    The reads are I/O-bound (HTTP over raw.githubusercontent, or disk), so a
    thread pool overlaps them -- the dominant cost of an HTTP-mode CI build.
    Tunable via ``WEHOOP_WNBA_BUILD_WORKERS`` (default 16).
    """
    try:
        return max(1, int(os.environ.get("WEHOOP_WNBA_BUILD_WORKERS", "16")))
    except ValueError:
        return 16


def parallel_map(fn, items):
    """Map ``fn`` over ``items`` with a thread pool; results in INPUT order.

    Input-order results keep the downstream ``concat`` (+ its stable sort)
    byte-identical to the sequential build -- parallelism is a pure speedup.
    """
    items = list(items)
    if len(items) <= 1:
        return [fn(x) for x in items]
    with ThreadPoolExecutor(max_workers=min(build_workers(), len(items))) as ex:
        return list(ex.map(fn, items))


def _resolve_root(explicit: str | Path | None) -> Path | str:
    """Resolve the wehoop-wnba-raw root (arg > env): a local Path or a base URL."""
    val = explicit or os.environ.get(RAW_ROOT_ENV)
    if not val:
        raise RuntimeError(
            f"set {RAW_ROOT_ENV} to the wehoop-wnba-raw checkout root or its "
            f"raw.githubusercontent base URL, or pass raw_root="
        )
    if isinstance(val, str) and val.startswith(("http://", "https://")):
        return val.rstrip("/")
    return Path(val)


def raw_root(explicit: str | Path | None = None) -> Path | str:
    """Resolve the wehoop-wnba-raw root (arg > env): a local Path or a base URL."""
    return _resolve_root(explicit)


def _http_get_bytes(url: str) -> bytes | None:
    """GET ``url`` -> bytes; ``None`` on any failure (R tryCatch parity)."""
    import requests

    headers: dict[str, str] = {}
    pat = os.environ.get("GITHUB_PAT") or os.environ.get("GH_TOKEN")
    if pat and url.startswith("https://api.github.com/"):
        headers["Authorization"] = f"token {pat}"
    try:
        resp = requests.get(url, headers=headers, timeout=60)
    except requests.RequestException:
        return None
    return resp.content if resp.status_code == 200 else None


def _cache_root() -> Path:
    return Path(os.environ.get("WEHOOP_WNBA_CACHE", ".wnba_raw_cache"))


def _read_season_schedule(season: int, root: Path | str) -> pl.DataFrame | None:
    """The raw season schedule frame, from disk or fetched fresh over HTTP."""
    rel = f"wnba/schedules/parquet/wnba_schedule_{season}.parquet"
    if isinstance(root, Path):
        f = root / rel
        return pl.read_parquet(f) if f.exists() else None
    body = _http_get_bytes(f"{root}/{rel}")
    if body is None:
        return None
    return pl.read_parquet(io.BytesIO(body))


def season_game_ids(season: int, *, raw_root: str | Path | None = None) -> list[int]:
    """Game ids for ``season`` that have a final.json (R: ``game_json == TRUE``)."""
    df = _read_season_schedule(season, _resolve_root(raw_root))
    if df is None:
        return []
    df = df.filter(pl.col("game_json") == True)  # noqa: E712
    return df.get_column("game_id").cast(pl.Int64).to_list()


def season_completed_game_ids(season: int, *, raw_root: str | Path | None = None) -> list[str]:
    """Completed-game ids for ``season`` (R ``list_game_ids`` in scripts 08/09).

    Unlike :func:`season_game_ids` this does NOT require ``game_json`` -- it
    filters to completed games (``status_type_completed`` truthy, with the
    ``status_type_name`` regex fallback) and returns ids as strings, matching
    the R producer's ``as.character(unique(game_id))``.
    """
    df = _read_season_schedule(season, _resolve_root(raw_root))
    if df is None or "game_id" not in df.columns:
        return []
    if "status_type_completed" in df.columns:
        df = df.filter(pl.col("status_type_completed") == True)  # noqa: E712
    elif "status_type_name" in df.columns:
        df = df.filter(
            pl.col("status_type_name")
            .str.to_uppercase()
            .str.contains("POSTPONED|CANCEL|SUSPENDED|FORFEIT")
            .fill_null(False)
            == False  # noqa: E712
        )
    ids = df.get_column("game_id").cast(pl.Int64).cast(pl.Utf8).unique(maintain_order=True)
    return [i for i in ids.to_list() if i]


def season_dir_ids(subdir: str, season: int, *, raw_root: str | Path | None = None) -> list[int]:
    """Numeric ids of the per-entity JSONs under ``wnba/{subdir}/json/{season}``.

    Mirrors the R scripts' GitHub-contents listing (alphabetical by file NAME,
    numeric names only). HTTP mode uses the contents API directly.
    """
    root = _resolve_root(raw_root)
    if isinstance(root, Path):
        d = root / "wnba" / subdir / "json" / str(season)
        if not d.is_dir():
            return []
        names = sorted(f.stem for f in d.glob("*.json"))
        return [int(n) for n in names if n.isdigit()]
    body = _http_get_bytes(f"{_RAW_REPO_API}/wnba/{subdir}/json/{season}")
    if body is None:
        return []
    try:
        listing = json.loads(body)
    except json.JSONDecodeError:
        return []
    if not isinstance(listing, list):
        return []
    stems = sorted(
        str(entry.get("name", "")).removesuffix(".json")
        for entry in listing
        if str(entry.get("name", "")).endswith(".json")
    )
    return [int(s) for s in stems if s.isdigit()]


def read_final(
    game_id: int | str,
    *,
    raw_root: str | Path | None = None,
    subdir: str = "json/final",
) -> dict | None:
    """Read one game's raw JSON; ``None`` if absent/malformed (R tryCatch parity).

    ``subdir`` selects the raw subtree under ``wnba/`` -- ``"json/final"``
    (default), ``"game_rosters/json"``, ``"officials/json"``, or the
    season-scoped ``"team_rosters/json/{season}"`` / ``"standings/json"``
    forms. HTTP mode caches each payload under ``$WEHOOP_WNBA_CACHE``.
    """
    root = _resolve_root(raw_root)
    rel = f"wnba/{subdir}/{game_id}.json"
    if isinstance(root, Path):
        f = root / rel
        if not f.exists():
            return None
        try:
            return json.loads(f.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
    cached = _cache_root() / rel
    if cached.exists():
        try:
            return json.loads(cached.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
    body = _http_get_bytes(f"{root}/{rel}")
    if body is None:
        return None
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None
    try:
        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(body)
    except OSError:
        pass  # cache is best-effort; the payload is already in hand
    return payload
