"""Dataset IO -- polars port of the R write + ``.append_manifest`` steps.

Writes ``{base}/{dataset}/parquet/{stem}_{season}.parquet`` and
``{base}/{dataset}/csv/{stem}_{season}{csv_suffix}`` (the big three commit
``.csv.gz`` to the tree, matching R ``fwrite``; the rest plain ``.csv``), and
upserts the ``{league}_{dataset}_in_data_repo.csv`` manifest. ``.rds`` is R's
native format and is produced by the retained R serialize step (cutover); the
parity bar here is the parquet.
"""

from __future__ import annotations

import gzip
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import polars as pl

from sportsdataverse._rds import write_rds

from wnba_data_build._logging import get_logger, human_size
from wnba_data_build.config import (
    RDS_ATTR_PREFIX,
    RDS_CLASS,
    RDS_TYPE_TEMPLATE,
    DatasetSpec,
)

_LEAGUE = "wnba"

log = get_logger()


def _utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def manifest_path(spec: DatasetSpec, base: Path) -> Path:
    return base / spec.dataset / f"{_LEAGUE}_{spec.dataset}_in_data_repo.csv"


def _append_manifest(spec: DatasetSpec, season: int, row_count: int, base: Path) -> Path | None:
    """Append one run's row to the dataset's manifest log (R ``fwrite(append=TRUE)``).

    The tree file is an append LOG -- one row per run, not per season (the real
    committed manifests carry 140-155 rows). ``publish`` is what collapses it to
    one row per season for the release asset. Rewriting the tree file as an
    upsert here would silently destroy that published history.
    """
    if spec.manifest_endpoint is None:
        return None  # R does not manifest this dataset; see DatasetSpec
    f = manifest_path(spec, base)
    f.parent.mkdir(parents=True, exist_ok=True)
    row = pl.DataFrame(
        {
            "season": [int(season)],
            "row_count": [int(row_count)],
            "generated_at_utc": [_utc_now_str()],
            "source_endpoint": [spec.manifest_endpoint.format(season=season)],
        }
    )
    if f.exists():
        row = pl.concat([pl.read_csv(f), row], how="diagonal_relaxed")
    row.write_csv(f)
    return f


def write_dataset(
    df: pl.DataFrame, spec: DatasetSpec, season: int, *, base: str | Path = "wnba"
) -> list[Path]:
    """Write parquet + csv + manifest for one dataset/season; return parquet+csv paths."""
    base = Path(base)
    pq_dir = base / spec.dataset / "parquet"
    csv_dir = base / spec.dataset / "csv"
    pq_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    pq = pq_dir / f"{spec.stem}_{season}.parquet"
    csv = csv_dir / f"{spec.stem}_{season}{spec.csv_suffix}"
    df.write_parquet(pq)
    if spec.csv_suffix.endswith(".gz"):
        buf = BytesIO()
        df.write_csv(buf)
        with gzip.open(csv, "wb") as fh:
            fh.write(buf.getvalue())
    else:
        df.write_csv(csv)
    # .rds is wehoop::load_wnba_*'s ONLY read path -- written natively here, in the
    # same pass as the parquet, so the two can never drift apart. The NBA
    # sibling proved they do: its rds was left to a retained R step it never
    # had, so the parquet updated daily while the rds froze.
    rds_dir = base / spec.dataset / "rds"
    rds_dir.mkdir(parents=True, exist_ok=True)
    rds = rds_dir / f"{spec.stem}_{season}.rds"
    stamped = datetime.now(timezone.utc)
    write_rds(
        df,
        rds,
        cls=list(RDS_CLASS),
        # Attribute ORDER is the published contract (make_wehoop_data stamps its
        # pair first, sportsdataverse_save appends its own).
        attributes={
            f"{RDS_ATTR_PREFIX}_timestamp": stamped,
            f"{RDS_ATTR_PREFIX}_type": RDS_TYPE_TEMPLATE.format(dataset=spec.dataset),
            "sportsdataverse_type": f"{spec.dataset} data",
            "sportsdataverse_timestamp": stamped,
        },
    )
    manifest = _append_manifest(spec, season, df.height, base)
    log.info(
        "wrote %s (%s) + %s (%s) + %s (%s), %d rows x %d cols; manifest %s",
        pq,
        human_size(pq.stat().st_size),
        csv.name,
        human_size(csv.stat().st_size),
        rds.name,
        human_size(rds.stat().st_size),
        df.height,
        df.width,
        f"{manifest.name} appended" if manifest else "n/a (not manifested)",
    )
    return [pq, rds, csv]


def write_schedule_extras(
    master: pl.DataFrame, games: pl.DataFrame, *, base: str | Path = "wnba"
) -> list[Path]:
    """Write the master-schedule extras under ``{base}/schedules/``.

    R never committed these (``sportsdataverse_save`` uploaded straight from
    the frame); the tree copy exists so the R serialize tail can produce the
    ``.rds`` assets. Files sit at the ``schedules/`` root -- NOT inside
    ``parquet/`` -- so the per-season glob in ``build_schedule_extras`` never
    picks the master back up.
    """
    root = Path(base) / "schedules"
    root.mkdir(parents=True, exist_ok=True)
    out: list[Path] = []
    for name, df in (("wnba_schedule_master", master), ("wnba_games_in_data_repo", games)):
        pq = root / f"{name}.parquet"
        csv = root / f"{name}.csv"
        df.write_parquet(pq)
        df.write_csv(csv)
        log.info(
            "wrote %s (%s), %d rows x %d cols",
            pq,
            human_size(pq.stat().st_size),
            df.height,
            df.width,
        )
        out.extend([pq, csv])
    return out
