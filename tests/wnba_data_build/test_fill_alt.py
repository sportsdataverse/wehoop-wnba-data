"""--fill-alt: the player and schedule crosswalks' Fox / Yahoo legs filled from wehoop-wnba-alt-data, never dropping data."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

pytest.importorskip("sportsdataverse._crosswalk_alt")

from wnba_data_build import reshapers  # noqa: E402

WNBA = Path(__file__).resolve().parents[2] / "wnba"
SEASON = 2024


@pytest.fixture
def tree(tmp_path, monkeypatch):
    """A base tree holding the committed 2024 crosswalk, schedule and game rosters, and an alt root."""
    base = tmp_path / "wnba"
    for rel in (
        f"crosswalk/parquet/wnba_player_crosswalk_{SEASON}.parquet",
        f"crosswalk/parquet/wnba_schedule_crosswalk_{SEASON}.parquet",
        f"schedules/parquet/wnba_schedule_{SEASON}.parquet",
        f"game_rosters/parquet/game_rosters_{SEASON}.parquet",
    ):
        src = WNBA / rel
        if not src.exists():
            pytest.skip(f"{rel} not in this checkout")
        (base / rel).parent.mkdir(parents=True, exist_ok=True)
        (base / rel).write_bytes(src.read_bytes())
    alt = tmp_path / "alt"
    marker = (
        alt / "yahoo" / "game_rosters" / "parquet" / f"wnba_yahoo_game_rosters_{SEASON}.parquet"
    )
    marker.parent.mkdir(parents=True)
    pl.DataFrame({"x": [1]}).write_parquet(marker)
    monkeypatch.setenv("WEHOOP_WNBA_ALT_DATA_ROOT", str(alt))
    return base


def test_alt_root_needs_a_parsed_season(tree):
    assert reshapers._alt_root(SEASON) is not None
    assert reshapers._alt_root(SEASON - 1) is None


def test_fill_keeps_columns_and_counts_and_refuses_a_loss(tree, monkeypatch):
    import sportsdataverse._crosswalk_alt as alt

    def fill(league, season, xwalk, *args, **kwargs):
        return xwalk.with_columns(pl.lit("wnba.p.1").alias("yahoo_player_id"))

    monkeypatch.setattr(alt, "with_alt_player_legs", fill)
    out = reshapers.crosswalk_fill_alt("player", SEASON, base=tree, archive_root=tree)
    assert out["yahoo_player_id"].null_count() == 0

    def drop(league, season, xwalk, *args, **kwargs):
        return xwalk.with_columns(pl.lit(None, dtype=pl.Utf8).alias("wnba_player_id"))

    monkeypatch.setattr(alt, "with_alt_player_legs", drop)
    with pytest.raises(RuntimeError, match="wnba_player_id"):
        reshapers.crosswalk_fill_alt("player", SEASON, base=tree, archive_root=tree)
    with pytest.raises(ValueError, match="player and schedule crosswalks"):
        reshapers.crosswalk_fill_alt("team", SEASON, base=tree, archive_root=tree)


def test_schedule_fill_uses_the_game_legs_and_refuses_a_loss(tree, monkeypatch):
    import sportsdataverse._crosswalk_alt as alt

    def fill(league, season, xwalk, *args, **kwargs):
        return xwalk.with_columns(pl.lit("42000").alias("fox_game_id"))

    monkeypatch.setattr(alt, "with_alt_game_legs", fill)
    out = reshapers.crosswalk_fill_alt("schedule", SEASON, base=tree, archive_root=tree)
    assert out["fox_game_id"].null_count() == 0

    def drop(league, season, xwalk, *args, **kwargs):
        return xwalk.with_columns(pl.lit(None, dtype=pl.Utf8).alias("wnba_game_id"))

    monkeypatch.setattr(alt, "with_alt_game_legs", drop)
    with pytest.raises(RuntimeError, match="wnba_game_id"):
        reshapers.crosswalk_fill_alt("schedule", SEASON, base=tree, archive_root=tree)
