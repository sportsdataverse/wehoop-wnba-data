"""Parity: Python game_rosters + officials vs the R-released parquet oracles.

Port provenance: script-local ``parse_one_game``/``parse_one_athlete`` in
``wehoop-wnba-data/R/espn_wnba_09_game_rosters_creation.R`` and
``parse_one_game``/``parse_one_official`` in ``espn_wnba_10_officials_creation.R``
(no wehoop helpers exist for these datasets). Oracles:
``tests/fixtures/released/{game_rosters,officials}_2025.parquet`` — the
published assets pre-filtered to the three 2025 fixture games
(401820325/401736112/401736113). ``game_id`` is String in both (R keeps
``as.character``).
"""

from pathlib import Path

import polars as pl
import pytest

from tests.wnba_data_build._parity_helpers import assert_parquet_parity
from wnba_data_build import reshapers

FX = Path(__file__).parent.parent / "fixtures"


@pytest.mark.parametrize(
    ("dataset", "stem", "keys"),
    [
        ("game_rosters", "game_rosters_2025", ["game_id", "athlete_id"]),
        ("officials", "officials_2025", ["game_id", "official_id"]),
    ],
)
def test_sidecar_parity_2025(dataset, stem, keys, tmp_path):
    py = reshapers.SEASON_BUILDERS[dataset](2025, raw_root=FX / "raw", base=tmp_path)
    oracle = FX / "released" / f"{stem}.parquet"
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c not in keys]
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample)
