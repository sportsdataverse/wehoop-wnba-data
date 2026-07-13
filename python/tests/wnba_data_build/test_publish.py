from pathlib import Path

import polars as pl

from wnba_data_build.config import REGISTRY
from wnba_data_build import io, publish


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
    # parquet + csv + manifest. The tree csv is gzipped for team_box, but the
    # release asset contract is plain .csv (decompressed to a temp file), and
    # the manifest is load-bearing (wehoop exports load_wnba_*_manifest()).
    assert len(uploads) == 3
    assets = sorted(Path(c[3]).name for c in uploads)  # gh release upload <tag> <path>
    assert assets == [
        "team_box_2025.csv",
        "team_box_2025.parquet",
        "wnba_team_box_in_data_repo.csv",
    ]
    assert all("--clobber" in c for c in uploads)
    assert res["tag"] == spec.tag


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
