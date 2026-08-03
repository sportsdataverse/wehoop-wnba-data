"""End-to-end: team_box builds a season from fixtures + dry-run publishes."""

from pathlib import Path

from wnba_data_build.build import build_season

FX = Path(__file__).parent.parent / "fixtures"


def test_build_team_box_writes_and_dry_run_publishes(tmp_path, capsys):
    df = build_season(
        "team_box",
        2025,
        base=tmp_path,
        raw_root=FX / "raw",
        dry_run=True,
    )
    assert df.height >= 2
    assert (tmp_path / "team_box" / "parquet" / "team_box_2025.parquet").exists()
    assert (tmp_path / "team_box" / "csv" / "team_box_2025.csv.gz").exists()
    assert "[dry-run] upload" in capsys.readouterr().out
