"""Parity: rosters / player_season_stats / team_season_stats / standings vs
the R-released parquet oracles (2025).

Port provenance: the script-local parsers in
``wehoop-wnba-data/R/espn_wnba_0{4,5,6,7}_*_creation.R`` (no wehoop helpers).
Fixtures: team_ids 3 and 5 (rosters + team_season_stats) and five of their
athletes present in the released player_season_stats asset; those two oracles
are pre-filtered to match. ``standings`` is a single whole-season payload, so
its oracle is the full released asset (299 rows, unfiltered). Long-format
frames have no compact unique key, so ALL columns act as sort keys (total
order; duplicate rows compare as multisets).
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
        ("rosters", "rosters_2025", ["team_id", "athlete_id"]),
        ("player_season_stats", "player_season_stats_2025", None),
        ("team_season_stats", "team_season_stats_2025", None),
        ("standings", "standings_2025", None),
    ],
)
def test_season_dataset_parity_2025(dataset, stem, keys, tmp_path):
    py = reshapers.SEASON_BUILDERS[dataset](2025, raw_root=FX / "raw", base=tmp_path)
    oracle = FX / "released" / f"{stem}.parquet"
    all_cols = list(pl.read_parquet_schema(str(oracle)))
    keys = keys if keys is not None else all_cols
    sample = [c for c in all_cols if c not in keys]
    assert_parquet_parity(py, oracle, keys=keys, sample_cols=sample)
