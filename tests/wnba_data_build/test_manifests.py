"""Build manifests carry one row per season.

Ported from wehoop-wbb-data/tests/wbb_data_build/test_manifests.py, which
caught this defect there: the R creation scripts wrote manifests with
`data.table::fwrite(..., append = TRUE)` -- a blind append with no dedupe --
so a re-run of a season leaves a duplicate row instead of replacing it.
Anything counting rows ends up counting RUNS, not seasons.
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFESTS = sorted(REPO_ROOT.glob("wnba/*/wnba_*_in_data_repo.csv"))


@pytest.mark.archive
@pytest.mark.parametrize("path", MANIFESTS, ids=lambda p: p.stem)
def test_one_row_per_season(path):
    frame = pl.read_csv(path)
    if "season" not in frame.columns:
        pytest.skip(f"{path.name} has no season column")
    duplicated = frame.group_by("season").len().filter(pl.col("len") > 1).sort("season").to_dicts()
    assert duplicated == [], f"{path.name}: seasons with multiple rows: {duplicated}"


@pytest.mark.archive
@pytest.mark.parametrize("path", MANIFESTS, ids=lambda p: p.stem)
def test_seasons_are_sorted(path):
    frame = pl.read_csv(path)
    if "season" not in frame.columns:
        pytest.skip(f"{path.name} has no season column")
    seasons = frame["season"].to_list()
    assert seasons == sorted(seasons), f"{path.name}: seasons out of order"


@pytest.mark.archive
def test_at_least_one_manifest_exists():
    """If the glob silently matched nothing the tests above would all pass."""
    assert MANIFESTS


def test_no_r_script_appends_to_a_manifest():
    """The writers must upsert. A blind append is what produced the duplicates,
    and it would reintroduce them the next time a season is rebuilt.

    Scoped to fwrite calls that target a manifest path, so an unrelated
    `append = TRUE` elsewhere in a script is not a false positive.
    """
    pattern = re.compile(
        r"fwrite\((?:[^()]|\([^()]*\))*manifest(?:[^()]|\([^()]*\))*append\s*=\s*TRUE",
        re.S | re.I,
    )
    offenders = [
        p.name
        for p in sorted((REPO_ROOT / "R").glob("*.R"))
        if pattern.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == [], f"R scripts still appending to a manifest: {offenders}"


def test_every_manifest_writer_uses_the_shared_helper():
    """A per-script hand-rolled upsert is how the scope bug happened elsewhere:
    a copy referenced the loop variable while sitting inside a function whose
    argument had a different name. One shared helper avoids that class of bug."""
    writers = [
        p
        for p in sorted((REPO_ROOT / "R").glob("*.R"))
        if "manifest_path" in p.read_text(encoding="utf-8")
        or "shots_manifest_path" in p.read_text(encoding="utf-8")
    ]
    assert writers, "no manifest-writing R scripts found"
    missing = [
        p.name
        for p in writers
        if "upsert_manifest_row(" not in p.read_text(encoding="utf-8")
        and p.name != "manifest_upload_helper.R"
    ]
    assert missing == [], f"manifest writers not using the helper: {missing}"
