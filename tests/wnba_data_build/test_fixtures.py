from pathlib import Path

import polars as pl

FX = Path(__file__).parent.parent / "fixtures"


def test_released_oracle_present_and_small():
    r = pl.read_parquet(FX / "released" / "team_box_2025.parquet")
    assert r.height > 0 and "game_id" in r.columns
    assert r.get_column("game_id").n_unique() == 3


def test_raw_fixtures_present():
    # 3 season-2025 games + 3 season-2026 games (second fixture wave).
    finals = list((FX / "raw" / "wnba" / "json" / "final").glob("*.json"))
    assert len(finals) == 6
    for season in (2025, 2026):
        sched = pl.read_parquet(
            FX / "raw" / "wnba" / "schedules" / "parquet" / f"wnba_schedule_{season}.parquet"
        )
        assert sched.filter(pl.col("game_json") == True).height == 3  # noqa: E712
    for sub in ("game_rosters", "officials"):
        assert len(list((FX / "raw" / "wnba" / sub / "json").glob("*.json"))) == 3
