"""Parity: Python schedules vs the R-released parquet oracle.

Port provenance: the schedule blocks of ``espn_wnba_0{1,2,3}_*_creation.R``
(casts + game_date_time/game_date + PBP/team_box/player_box flag stamping;
script 03 uploads). Oracle: ``tests/fixtures/released/wnba_schedule_2025.parquet``
— the published ``espn_wnba_schedules`` asset pre-filtered
to the three fixture games. All three fixture games compiled in the real R run,
so all three flags are True on every oracle row; the fixture id-sets mirror that.
"""

import re
from pathlib import Path

import polars as pl
from wnba_data_build import reshapers

from tests.wnba_data_build._parity_helpers import assert_parquet_parity

FX = Path(__file__).parent.parent / "fixtures"

GIDS = [401820325, 401736112, 401736113]


def test_schedules_parity_2025(tmp_path):
    from sportsdataverse.wnba import helper_wnba_schedule

    raw = pl.read_parquet(
        FX / "raw" / "wnba" / "schedules" / "parquet" / "wnba_schedule_2025.parquet"
    )
    py = helper_wnba_schedule(
        raw, pbp_game_ids=GIDS, team_box_game_ids=GIDS, player_box_game_ids=GIDS
    )
    oracle = FX / "released" / "wnba_schedule_2025.parquet"
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c != "game_id"]
    assert_parquet_parity(py, oracle, keys=["game_id"], sample_cols=sample)


def test_schedules_builder_reads_built_ids(tmp_path):
    # No built datasets under base -> all flags False (R empty-espn_df branch).
    out = reshapers.schedules_builder(2025, raw_root=FX / "raw", base=tmp_path)
    assert out.get_column("PBP").to_list() == [False] * out.height
    assert out.get_column("team_box").to_list() == [False] * out.height


def test_schedules_builder_fills_records_linescores_from_finals(tmp_path):
    # wehoop-wnba-raw's 2002-2022 schedule parquets were last scraped 2023-04-08
    # by a scoreboard parser that emitted no records/linescores columns, so the
    # released schedule lacked them. The game's final summary carries both
    # (header.competitions[0].competitors[]); 250603003 = NYL @ TUL, 2005-06-03.
    raw = pl.read_parquet(
        FX / "raw" / "wnba" / "schedules" / "parquet" / "wnba_schedule_2005.parquet"
    )
    assert "home_records" not in raw.columns  # the real raw gap this test pins

    out = reshapers.schedules_builder(2005, raw_root=FX / "raw", base=tmp_path)

    row = out.filter(pl.col("game_id") == 250603003).row(0, named=True)
    assert out.schema["home_records"] == pl.Utf8
    assert "'type': 'total', 'summary': '4-0'" in row["home_records"]
    assert "'type': 'total', 'summary': '1-3'" in row["away_records"]
    assert [float(v) for v in re.findall(r"'value': ([\d.]+)", row["home_linescores"])] == [
        18.0,
        36.0,
        14.0,
    ]
    assert [float(v) for v in re.findall(r"'value': ([\d.]+)", row["away_linescores"])] == [
        28.0,
        26.0,
        12.0,
    ]
