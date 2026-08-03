import polars as pl

from wnba_data_build.build import build_season


def test_build_empty_season_returns_empty(tmp_path):
    # schedule with no game_json==True rows -> season_game_ids empty -> early return, no reshaper needed
    sched = tmp_path / "wnba" / "schedules" / "parquet"
    sched.mkdir(parents=True)
    pl.DataFrame({"game_id": [1, 2], "game_json": [False, False]}).write_parquet(
        sched / "wnba_schedule_2025.parquet"
    )
    out = build_season("team_box", 2025, base=tmp_path / "out", raw_root=tmp_path)
    assert out.height == 0
    assert not (tmp_path / "out").exists()  # nothing written on empty season
