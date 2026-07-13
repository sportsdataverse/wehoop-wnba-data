"""Every implemented dataset builds end-to-end from the fixture raw tree.

All datasets build from the 2025 fixture season except ``draft``, whose raw
tree and release both start at 2026. The three crosswalks raise
NotImplementedError (they build from live ESPN+Torvik+Fox inputs via the
retained R scripts, not from the raw repo).
"""

from pathlib import Path

import pytest

from wnba_data_build.build import build_season
from wnba_data_build.config import REGISTRY

FX = Path(__file__).parent.parent / "fixtures"

_SEASON = {
    "pbp": 2025,
    "schedules": 2025,
    "team_box": 2025,
    "player_box": 2025,
    "shots": 2025,
    "game_rosters": 2025,
    "officials": 2025,
    "rosters": 2025,
    "player_season_stats": 2025,
    "team_season_stats": 2025,
    "standings": 2025,
    "draft": 2026,
}


@pytest.mark.parametrize("dataset", sorted(REGISTRY))
def test_each_dataset_builds(dataset, tmp_path):
    if dataset.endswith("_crosswalk"):
        with pytest.raises(NotImplementedError):
            build_season(dataset, 2026, base=tmp_path, raw_root=FX / "raw")
        return
    season = _SEASON[dataset]
    if dataset == "shots":  # shots read the built pbp parquet
        build_season("pbp", season, base=tmp_path, raw_root=FX / "raw")
    df = build_season(dataset, season, base=tmp_path, raw_root=FX / "raw", dry_run=True)
    assert df.height > 0
    spec = REGISTRY[dataset]
    assert (tmp_path / spec.dataset / "parquet" / f"{spec.stem}_{season}.parquet").exists()
    # pbp/team_box/player_box commit gzipped csv to the tree (spec.csv_suffix).
    assert (tmp_path / spec.dataset / "csv" / f"{spec.stem}_{season}{spec.csv_suffix}").exists()
