"""The declared schemas match the real released assets.

MODELS was generated from the release-parity fixtures (plus the published
player_core / wnba_crosswalk parquets) and is kept by hand; this is the test
that notices when a build adds, drops, or retypes a column without the model
moving with it.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from wnba_data_build.config import REGISTRY
from wnba_data_build.models import MODELS, check_frame, polars_schema

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "released"

#: dataset -> committed release-parity fixture. player_core and the three
#: crosswalks have no committed fixture; their models were generated from the
#: published assets and are exercised structurally below.
FIXTURE_OF = {
    "pbp": "play_by_play_2026.parquet",
    "schedules": "wnba_schedule_2025.parquet",
    "shots": "shots_2025.parquet",
    "team_box": "team_box_2025.parquet",
    "player_box": "player_box_2025.parquet",
    "rosters": "rosters_2025.parquet",
    "player_season_stats": "player_season_stats_2025.parquet",
    "team_season_stats": "team_season_stats_2025.parquet",
    "standings": "standings_2025.parquet",
    "game_rosters": "game_rosters_2025.parquet",
    "officials": "officials_2025.parquet",
    "draft": "draft_2026.parquet",
}


def test_every_registry_dataset_has_a_model():
    assert set(MODELS) == set(REGISTRY)


@pytest.mark.parametrize("dataset", sorted(FIXTURE_OF))
def test_model_matches_released_fixture(dataset):
    frame = pl.read_parquet(FIXTURES / FIXTURE_OF[dataset])
    assert check_frame(dataset, frame) == []


@pytest.mark.parametrize("dataset", sorted(MODELS))
def test_polars_schema_is_nonempty_and_typed(dataset):
    schema = polars_schema(dataset)
    assert len(schema) > 0
    # The class-namespace shadowing bug this guards against: a column literally
    # named `date`/`datetime` silently turned every later date annotation into
    # NoneType, which polars_schema then rendered as Utf8.
    assert all(dtype is not None for dtype in schema.dtypes())


def test_schedules_date_columns_survive_name_shadowing():
    schema = polars_schema("schedules")
    assert schema["game_date"] == pl.Date
    assert schema["game_date_time"] == pl.Datetime(time_unit="us", time_zone="America/New_York")
    assert schema["date"] == pl.Utf8  # the column actually named `date` is a string
