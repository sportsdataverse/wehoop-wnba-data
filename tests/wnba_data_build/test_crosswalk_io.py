"""The crosswalk output contract: shared ``wnba/crosswalk/`` dir, no tree csv,
bespoke rds type strings, and a manifest that UPSERTS instead of appending.

R's ``wnba_1{1,2,3}_*_crosswalk_creation.R`` hard-code one shared directory and
commit no tree csv -- ``wnba/crosswalk/*.csv`` are the MANIFESTS, one row per
season. If the Python producer appended like the per-game datasets do, a daily
re-run would grow a duplicate row per season.
"""

import polars as pl
from wnba_data_build import io, publish
from wnba_data_build.config import REGISTRY

CROSSWALKS = ("team_crosswalk", "schedule_crosswalk", "player_crosswalk")


def _frame() -> pl.DataFrame:
    return pl.DataFrame({"season": [2026, 2026], "espn_team_id": [1, 2]})


def test_all_three_crosswalks_share_one_directory(tmp_path):
    for ds in CROSSWALKS:
        assert io.dataset_dir(REGISTRY[ds], tmp_path) == tmp_path / "crosswalk"


def test_manifest_file_name_still_carries_the_dataset(tmp_path):
    assert io.manifest_path(REGISTRY["team_crosswalk"], tmp_path) == (
        tmp_path / "crosswalk" / "wnba_team_crosswalk_in_data_repo.csv"
    )


def test_write_lands_under_crosswalk_with_no_tree_csv(tmp_path):
    spec = REGISTRY["team_crosswalk"]
    paths = io.write_dataset(_frame(), spec, 2026, base=tmp_path)
    assert (tmp_path / "crosswalk" / "parquet" / "wnba_team_crosswalk_2026.parquet").exists()
    assert (tmp_path / "crosswalk" / "rds" / "wnba_team_crosswalk_2026.rds").exists()
    assert not (tmp_path / "crosswalk" / "csv").exists()
    assert not (tmp_path / "team_crosswalk").exists()
    assert len(paths) == 2


def test_non_crosswalk_datasets_still_write_their_tree_csv(tmp_path):
    spec = REGISTRY["standings"]
    io.write_dataset(_frame(), spec, 2026, base=tmp_path)
    assert (tmp_path / "standings" / "csv" / f"standings_2026{spec.csv_suffix}").exists()


def test_rerunning_a_season_upserts_the_manifest_row(tmp_path):
    spec = REGISTRY["player_crosswalk"]
    for _ in range(3):
        io.write_dataset(_frame(), spec, 2026, base=tmp_path)
    io.write_dataset(_frame(), spec, 2025, base=tmp_path)
    m = pl.read_csv(io.manifest_path(spec, tmp_path))
    assert m["season"].to_list() == [2025, 2026]  # sorted, one row per season
    assert m["source_endpoint"].unique().to_list() == ["wehoop::wnba_player_crosswalk()"]


def test_per_game_datasets_still_append_their_manifest_log(tmp_path):
    spec = REGISTRY["rosters"]
    for _ in range(3):
        io._append_manifest(spec, 2026, 5, tmp_path)
    assert pl.read_csv(io.manifest_path(spec, tmp_path)).height == 3


def test_publish_finds_the_crosswalk_files_and_still_ships_a_csv_asset(tmp_path):
    spec = REGISTRY["schedule_crosswalk"]
    io.write_dataset(_frame(), spec, 2026, base=tmp_path)
    names = [p.name for p in publish._dataset_files(spec, 2026, tmp_path)]
    assert "wnba_schedule_crosswalk_2026.parquet" in names
    assert "wnba_schedule_crosswalk_2026.rds" in names
    # No tree csv, but R's file_types still ship one (built from the parquet).
    assert "wnba_schedule_crosswalk_2026.csv" in names
    assert "wnba_schedule_crosswalk_in_data_repo.csv" in names


def test_rds_carries_the_bespoke_crosswalk_type_not_the_generic_template():
    assert REGISTRY["team_crosswalk"].rds_type == "WNBA team crosswalk (ESPN / WNBA Stats / Fox)"
    assert REGISTRY["schedule_crosswalk"].rds_type == "WNBA schedule crosswalk (ESPN / WNBA Stats)"
    assert (
        REGISTRY["player_crosswalk"].rds_type == "WNBA player crosswalk (ESPN / WNBA Stats / Fox)"
    )
