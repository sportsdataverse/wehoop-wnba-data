"""Parity: Python team_box vs the R-released parquet oracle.

Port provenance: ``wehoop:::helper_espn_wnba_team_box``
(``wehoop/R/espn_wnba_data.R`` lines 2338-2551, wehoop 3.0.0 checkout).
Oracle: ``tests/fixtures/released/team_box_2025.parquet`` — the published
``espn_wnba_team_boxscores`` asset pre-filtered to the three 2025 fixture
games (401820325/401736112/401736113, 6 rows). Every shared column is asserted
value-equal (exact — no float thresholds; the R pipeline is deterministic
reshaping, not a model).
"""

from pathlib import Path

import polars as pl

from tests.wnba_data_build._parity_helpers import assert_parquet_parity
from wnba_data_build.build import build_season

FX = Path(__file__).parent.parent / "fixtures"

KEYS = ["game_id", "team_id"]


def test_team_box_parity_2025(tmp_path):
    # Production path: build_season owns the R arrange(desc(game_date)) sort.
    py = build_season("team_box", 2025, base=tmp_path, raw_root=FX / "raw")
    oracle = FX / "released" / "team_box_2025.parquet"
    all_cols = [c for c in pl.read_parquet_schema(str(oracle)) if c not in KEYS]
    assert_parquet_parity(py, oracle, keys=KEYS, sample_cols=all_cols)
