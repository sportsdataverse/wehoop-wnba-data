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

#: A build manifest is identified by its SHAPE, not its filename.
#:
#: The `*_in_data_repo.csv` suffix is ALSO used by real per-game datasets:
#: `wnba/schedules/nba_games_in_data_repo.csv` is ~31k rows x 78 columns of
#: actual games. Selecting by filename swept that in and demanded one row per
#: season from a per-game dataset -- a test that cannot pass, and whose only
#: "fix" would be deleting real data. The pre-existing
#: `if "season" not in frame.columns: skip` guard did not save it either,
#: because the games file HAS a season column.
#: The CORE signature only. `source_endpoint` is present on some manifests and
#: absent on others (wehoop-wbb-data has both 3- and 4-column manifests), so
#: requiring it excluded four real manifests there -- caught by
#: `test_the_shape_filter_only_excluded_wide_datasets` below, which exists for
#: exactly this failure. These three still discriminate cleanly: the per-game
#: schedule datasets that share the filename suffix carry neither `row_count`
#: nor `generated_at_utc`.
MANIFEST_COLUMNS = {"season", "row_count", "generated_at_utc"}

#: Deliberately prefix-agnostic. `_is_manifest` does the discriminating, so
#: requiring a `<league>_` prefix here would only re-introduce a filename
#: assumption -- and it would MISS misnamed manifests, which exist: several
#: mbb stages write `mbb/<dataset>/nba_<dataset>_in_data_repo.csv`, so a
#: `mbb_*` glob silently skips exactly the files most likely to be wrong.
_CANDIDATES = sorted(REPO_ROOT.glob("wnba/*/*_in_data_repo.csv"))


def _is_manifest(path: Path) -> bool:
    """True when the file carries the manifest column signature."""
    try:
        return MANIFEST_COLUMNS <= set(pl.read_csv(path, n_rows=0).columns)
    except Exception:  # unreadable/exotic csv is not a manifest
        return False


MANIFESTS = [p for p in _CANDIDATES if _is_manifest(p)]
NON_MANIFESTS = [p for p in _CANDIDATES if p not in MANIFESTS]


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


@pytest.mark.archive
def test_the_shape_filter_only_excluded_wide_datasets():
    """Guard the guard from the other side.

    ``_is_manifest`` narrows the candidate set, so a filter that became too
    strict would quietly drop real manifests and leave every check above
    passing vacuously. Anything excluded must be materially wider than a
    manifest (i.e. an actual dataset); a narrow file being excluded means the
    signature is wrong, not that the file is.
    """
    too_narrow = [
        p.name
        for p in NON_MANIFESTS
        if len(pl.read_csv(p, n_rows=0).columns) <= len(MANIFEST_COLUMNS) + 2
    ]
    assert too_narrow == [], (
        f"manifest-sized files were excluded by the shape filter: {too_narrow} — "
        "the MANIFEST_COLUMNS signature is probably wrong"
    )


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
