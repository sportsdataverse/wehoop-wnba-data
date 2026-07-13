"""Parity: Python draft vs the R-released parquet oracle.

Port provenance: ``parse_one_pick`` + the flat-``picks[]`` path in
``wehoop-wnba-data/R/espn_wnba_08_draft_creation.R`` (no wehoop helper exists).
Oracle: ``tests/fixtures/released/draft_2026.parquet`` -- the full published
``espn_wnba_draft`` asset (45x35, unfiltered; the tag publishes 2026 only).

Draft is the headline WNBA delta (WBB has no draft dataset), so it gets a
full-frame assertion: every column, in order, plus R's
``arrange(overall_pick, round, pick)`` row order.
"""

from pathlib import Path

import polars as pl

from tests.wnba_data_build._parity_helpers import assert_parquet_parity
from wnba_data_build import reshapers

FX = Path(__file__).parent.parent / "fixtures"


def test_draft_parity_2026(tmp_path):
    py = reshapers.SEASON_BUILDERS["draft"](2026, raw_root=FX / "raw", base=tmp_path)
    oracle = FX / "released" / "draft_2026.parquet"
    keys = ["overall_pick", "round", "pick"]
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c not in keys]
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample)


def test_draft_row_order_is_r_arrange_order(tmp_path):
    # assert_parquet_parity sorts both sides, so it cannot see row order. R ends
    # in arrange(overall_pick, round, pick) -- pin it unsorted.
    py = reshapers.SEASON_BUILDERS["draft"](2026, raw_root=FX / "raw", base=tmp_path)
    oracle = pl.read_parquet(FX / "released" / "draft_2026.parquet")
    assert py.get_column("overall_pick").to_list() == oracle.get_column("overall_pick").to_list()
    assert py.get_column("athlete_id").to_list() == oracle.get_column("athlete_id").to_list()
