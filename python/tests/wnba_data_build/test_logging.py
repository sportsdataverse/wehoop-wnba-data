"""The pipeline narrates itself: timestamped stdout lines that the daily
processor tees into the per-season rotating logfile, incl. explicit gh-release
upload confirmations. The logger's handler resolves sys.stdout at emit time,
so capsys sees exactly what the Actions console / season logfile would."""

from pathlib import Path

import polars as pl

from wnba_data_build import io as build_io
from wnba_data_build import publish
from wnba_data_build.build import build_season
from wnba_data_build.config import REGISTRY

FX = Path(__file__).parent.parent / "fixtures"


def test_build_season_narrates_lifecycle(tmp_path, capsys):
    build_season("team_box", 2025, base=tmp_path, raw_root=FX / "raw")
    out = capsys.readouterr().out
    assert "team_box 2025: per-game build starting -- 3 games" in out
    assert "wrote" in out and "team_box_2025.parquet" in out
    assert "manifest" in out
    assert "team_box 2025: done -- 6 rows from 3/3 games" in out
    assert "[INFO] wnba_data_build:" in out  # timestamped, labeled lines


def test_publish_logs_upload_confirmations(tmp_path, capsys):
    spec = REGISTRY["team_box"]
    build_io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    capsys.readouterr()  # drop the write lines
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: None,
        exists_check=lambda tag, repo: True,
    )
    out = capsys.readouterr().out
    assert "uploading team_box_2025.parquet" in out
    assert f"uploaded team_box_2025.csv -> {spec.tag} (asset 2/3)" in out
    # The manifest is the third asset and is load-bearing for wehoop's
    # load_wnba_*_manifest() loaders -- log it like any other upload.
    assert f"uploaded wnba_team_box_in_data_repo.csv -> {spec.tag} (asset 3/3)" in out


def test_publish_dry_run_logs_would_be_uploads(tmp_path, capsys):
    spec = REGISTRY["team_box"]
    build_io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    capsys.readouterr()
    publish.publish_dataset(spec, 2025, base=tmp_path, dry_run=True)
    out = capsys.readouterr().out
    assert "[dry-run] upload" in out and spec.tag in out
