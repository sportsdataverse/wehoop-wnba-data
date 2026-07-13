"""Parity: Python shots vs the R-released parquet oracle.

Port provenance: the shots block of ``espn_wnba_01_pbp_creation.R`` (filter
``shooting_play == TRUE`` on the compiled season pbp, project the 15 shot
columns). Oracle: ``tests/fixtures/released/shots_2025.parquet`` — the
published ``espn_wnba_shots`` asset pre-filtered to the three 2025 fixture
games (554 rows, live coordinates). The shot rows have no unique key, so ALL
columns are sort keys (total order; duplicate rows compare fine as multisets).
"""

from pathlib import Path

import polars as pl

from tests.wnba_data_build._parity_helpers import assert_parquet_parity
from wnba_data_build.build import build_season

FX = Path(__file__).parent.parent / "fixtures"


def test_shots_parity_2025(tmp_path):
    # Production path: shots read the built pbp parquet under the same base.
    build_season("pbp", 2025, base=tmp_path, raw_root=FX / "raw")
    py = build_season("shots", 2025, base=tmp_path, raw_root=FX / "raw")
    oracle = FX / "released" / "shots_2025.parquet"
    keys = list(pl.read_parquet_schema(str(oracle)))
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=[])
