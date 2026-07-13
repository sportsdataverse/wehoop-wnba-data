import polars as pl

from wnba_data_build import ingest


def test_read_final_missing_returns_none(tmp_path):
    assert ingest.read_final(999, raw_root=tmp_path) is None


def test_season_game_ids_filters_game_json(tmp_path):
    sched_dir = tmp_path / "wnba" / "schedules" / "parquet"
    sched_dir.mkdir(parents=True)
    pl.DataFrame({"game_id": [1, 2, 3], "game_json": [True, False, True]}).write_parquet(
        sched_dir / "wnba_schedule_2025.parquet"
    )
    assert ingest.season_game_ids(2025, raw_root=tmp_path) == [1, 3]
