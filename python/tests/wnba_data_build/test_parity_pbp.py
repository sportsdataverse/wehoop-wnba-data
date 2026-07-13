"""Parity: Python play_by_play vs the R-released parquet oracle.

Port provenance: ``wehoop:::helper_espn_wnba_pbp``
(``wehoop/R/espn_wnba_data.R`` lines 2056-2337, wehoop 3.0.0). Oracles:
``tests/fixtures/released/play_by_play_{2025,2026}.parquet`` — the published
``espn_wnba_pbp`` assets pre-filtered to the fixture games (2025:
401820325/401736112/401736113, 2026: 401856890/401856891/401856892). Both
seasons ship coordinates on every play, so both exercise the live coordinate
transform (FT pin + home-flip); the seasons are parametrized to cover the
type_abbreviation delta (absent in 2025, backfilled by the reshaper).
"""

from pathlib import Path

import polars as pl
import pytest

from tests.wnba_data_build._parity_helpers import assert_parquet_parity
from wnba_data_build.build import build_season

FX = Path(__file__).parent.parent / "fixtures"

KEYS = ["game_id", "game_play_number"]


@pytest.mark.parametrize("season", [2025, 2026])
def test_pbp_parity(season, tmp_path):
    # Production path: build_season owns the R arrange(desc(game_date)) sort.
    py = build_season("pbp", season, base=tmp_path, raw_root=FX / "raw")
    oracle = FX / "released" / f"play_by_play_{season}.parquet"
    sample = [c for c in pl.read_parquet_schema(str(oracle)) if c not in KEYS]
    assert_parquet_parity(
        py,
        oracle,
        keys=KEYS,
        sample_cols=sample,
        # pbp column order is payload-first-seen; the raw repo has been
        # re-scraped since the oracle was compiled (the released 2025 and
        # 2026 assets already disagree on order), so order is not asserted.
        require_order=False,
        # Deliberate improvement: R/jsonlite has no int64, so the released
        # `id` is Float64 and collides above 2^53 (adjacent ~4e17 play ids
        # round to the same double). The Python producer emits exact Int64;
        # values still compare equal under the oracle's lossy Float64 view.
        dtype_upgrades={"id": (pl.Int64(), pl.Float64())},
    )
