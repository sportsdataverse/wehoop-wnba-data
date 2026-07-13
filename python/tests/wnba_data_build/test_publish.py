from pathlib import Path

import polars as pl

from wnba_data_build import io, publish
from wnba_data_build.config import REGISTRY


def test_publish_uploads_each_file_with_clobber(tmp_path):
    spec = REGISTRY["team_box"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    res = publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,  # release already exists
    )
    uploads = [c for c in calls if c[:2] == ["release", "upload"]]
    # team_box is not manifested -> parquet + csv only. The tree csv is gzipped,
    # but the release asset contract is plain .csv (decompressed to a temp file).
    assert len(uploads) == 2
    assets = sorted(Path(c[3]).name for c in uploads)  # gh release upload <tag> <path>
    assert assets == ["team_box_2025.csv", "team_box_2025.parquet"]
    assert all("--clobber" in c for c in uploads)
    assert res["tag"] == spec.tag


def test_publish_uploads_manifest_for_manifested_datasets(tmp_path):
    spec = REGISTRY["standings"]
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )
    uploads = [c for c in calls if c[:2] == ["release", "upload"]]
    assets = sorted(Path(c[3]).name for c in uploads)
    # The manifest is load-bearing: wehoop exports load_wnba_standings_manifest().
    assert assets == [
        "standings_2025.csv",
        "standings_2025.parquet",
        "wnba_standings_in_data_repo.csv",
    ]


def test_published_manifest_collapses_the_log_to_the_latest_run_per_season(tmp_path):
    spec = REGISTRY["standings"]
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2024, base=tmp_path)
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    io.write_dataset(pl.DataFrame({"team_id": [1, 2, 3]}), spec, 2025, base=tmp_path)  # rerun

    asset = publish._manifest_asset(spec, tmp_path)
    m = pl.read_csv(asset)
    # One row per season, ascending -- and the LATEST run wins. R's helper keeps
    # the FIRST (dplyr distinct), which froze the published row_count at a
    # season's first-ever run; we publish the count that describes the asset we
    # are actually shipping beside it.
    assert m["season"].to_list() == [2024, 2025]
    assert m["row_count"].to_list() == [1, 3]


def test_publish_creates_release_when_missing(tmp_path):
    spec = REGISTRY["team_box"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: False,
    )
    assert any(c[:2] == ["release", "create"] for c in calls)
