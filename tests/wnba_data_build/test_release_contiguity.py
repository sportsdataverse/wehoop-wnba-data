"""The post-publish season-contiguity check (wehoop-wbb-data#44)."""

import polars as pl
import pytest
from wnba_data_build import cli, publish
from wnba_data_build.config import REGISTRY

SPEC = REGISTRY["team_box"]


def _assets(*seasons: int) -> list[str]:
    names = [f"{SPEC.stem}_{s}.{ext}" for s in seasons for ext in ("parquet", "rds", "csv")]
    return [*names, "timestamp.json", f"other_{seasons[0] + 1}.parquet"]


def test_contiguous_release_has_no_missing_season():
    assert publish.missing_interior_seasons(SPEC, _assets(2019, 2020, 2021)) == []


def test_interior_hole_is_reported_and_edges_are_not():
    # a hole in the middle counts; seasons before the first or after the last do not
    assert publish.missing_interior_seasons(SPEC, _assets(2019, 2022, 2023)) == [2020, 2021]


def test_known_gap_is_excused():
    spec = REGISTRY["schedules"]
    assert 1998 in publish.KNOWN_GAPS["schedules"]
    names = [f"{spec.stem}_{s}.parquet" for s in (1998 - 1, 1998 + 1)]
    assert publish.missing_interior_seasons(spec, names) == []


def test_check_raises_on_a_hole():
    with pytest.raises(publish.SeasonGapError, match=r"\[2020\]"):
        publish.check_release_contiguous(SPEC, lister=lambda tag, repo: _assets(2019, 2021))


@pytest.fixture
def no_build(monkeypatch):
    monkeypatch.setattr(cli, "build_season", lambda *a, **k: pl.DataFrame({"x": [1]}))
    calls = []

    def _check(spec, *a, **k):
        calls.append(spec.dataset)
        raise publish.SeasonGapError("team_box: interior season(s) [2020] missing")

    monkeypatch.setattr(cli.publish, "check_release_contiguous", _check)
    return calls


def test_publish_run_fails_on_a_hole(no_build):
    assert cli.main(["--dataset", "team_box", "-s", "2025", "-e", "2025", "--publish"]) == 1
    assert no_build == ["team_box"]


def test_allow_gaps_skips_the_check(no_build):
    argv = ["--dataset", "team_box", "-s", "2025", "-e", "2025", "--publish", "--allow-gaps"]
    assert cli.main(argv) == 0
    assert no_build == []


def test_no_publish_no_check(no_build):
    assert cli.main(["--dataset", "team_box", "-s", "2025", "-e", "2025", "--dry-run"]) == 0
    assert no_build == []
