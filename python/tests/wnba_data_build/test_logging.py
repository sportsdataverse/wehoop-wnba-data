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
    # rds now uploads alongside parquet+csv. team_box takes the gz path, so
    # its plain csv is decompressed to a temp file and appended AFTER the
    # cands filter -- hence rds lands last here but 2nd for standings.
    assert f"uploaded team_box_2025.rds -> {spec.tag} (asset 3/3)" in out


def test_publish_logs_the_manifest_upload(tmp_path, capsys):
    # The manifest is load-bearing for wehoop's load_wnba_*_manifest() loaders,
    # so its upload gets confirmed like any other asset.
    spec = REGISTRY["standings"]
    build_io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    capsys.readouterr()
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: None,
        exists_check=lambda tag, repo: True,
    )
    out = capsys.readouterr().out
    assert f"uploaded wnba_standings_in_data_repo.csv -> {spec.tag} (asset 4/4)" in out


def test_publish_dry_run_logs_would_be_uploads(tmp_path, capsys):
    spec = REGISTRY["team_box"]
    build_io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    capsys.readouterr()
    publish.publish_dataset(spec, 2025, base=tmp_path, dry_run=True)
    out = capsys.readouterr().out
    assert "[dry-run] upload" in out and spec.tag in out
