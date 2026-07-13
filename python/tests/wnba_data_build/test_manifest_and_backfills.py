"""The two release-asset contracts no fixture season happens to exercise.

Both were shipped wrong and caught in review: the manifest was missing
``source_endpoint`` (and was being written for datasets R never manifests), and
the ``team_box`` ``largest_lead`` backfill was absent, which would have stripped
a column from 11 already-published seasons the moment anyone rebuilt them.
"""

import polars as pl

from wnba_data_build.config import REGISTRY
from wnba_data_build.reshapers import team_box_season_postprocess

_RAW = "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba"

# Captured verbatim from the committed manifests in wnba/<dataset>/ (which are
# what the R scripts wrote), season-substituted for 2026.
EXPECTED_ENDPOINT = {
    "shots": "derived from espn_wnba pbp",
    "rosters": f"{_RAW}/team_rosters/json/2026/<team_id>.json",
    "player_season_stats": f"{_RAW}/player_season_stats/json/2026/<athlete_id>.json",
    "team_season_stats": f"{_RAW}/team_stats/json/2026/<team_id>.json",
    "standings": f"{_RAW}/standings/json/2026.json",
    "draft": f"{_RAW}/draft/json/2026.json",
    "game_rosters": f"{_RAW}/game_rosters/json/<game_id>.json",
    "officials": f"{_RAW}/officials/json/<game_id>.json",
}


def test_exactly_the_eight_r_manifested_datasets_have_a_manifest():
    # wehoop exports exactly 8 load_wnba_*_manifest() loaders; a manifest on any
    # other dataset would publish an asset nothing reads.
    manifested = {k for k, v in REGISTRY.items() if v.manifest_endpoint is not None}
    assert manifested == set(EXPECTED_ENDPOINT)


def test_manifest_endpoints_match_the_committed_r_output():
    for dataset, expected in EXPECTED_ENDPOINT.items():
        spec = REGISTRY[dataset]
        assert spec.manifest_endpoint is not None
        assert spec.manifest_endpoint.format(season=2026) == expected, dataset


def test_team_box_backfills_largest_lead_when_the_payload_union_lacks_it():
    # Released team_box for 2003-2011/2013/2015 carries largest_lead as an
    # all-null String column that exists only because of espn_wnba_02:78-82.
    without = pl.DataFrame({"game_id": [1, 2], "team_id": [3, 4]})
    out = team_box_season_postprocess(without)
    assert out.columns[-1] == "largest_lead"  # R relocates it to last
    assert out.schema["largest_lead"] == pl.Utf8
    assert out.get_column("largest_lead").to_list() == [None, None]


def test_team_box_backfill_leaves_a_season_that_ships_largest_lead_alone():
    with_ll = pl.DataFrame({"game_id": [1], "largest_lead": ["12"], "team_id": [3]})
    out = team_box_season_postprocess(with_ll)
    assert out.columns == ["game_id", "largest_lead", "team_id"]  # untouched
    assert out.get_column("largest_lead").to_list() == ["12"]
