import polars as pl

from wnba_data_build import io
from wnba_data_build.config import REGISTRY


def test_write_dataset_emits_parquet_and_gzipped_csv(tmp_path):
    df = pl.DataFrame({"game_id": [1, 2], "score": [70, 65]})
    spec = REGISTRY["team_box"]
    paths = io.write_dataset(df, spec, 2025, base=tmp_path)
    names = sorted(p.name for p in paths)
    # team_box is one of the three datasets the WNBA tree commits gzipped.
    assert names == ["team_box_2025.csv.gz", "team_box_2025.parquet", "team_box_2025.rds"]
    assert (tmp_path / "team_box" / "parquet" / "team_box_2025.parquet").exists()
    assert (tmp_path / "team_box" / "csv" / "team_box_2025.csv.gz").exists()


def test_no_manifest_for_datasets_r_does_not_manifest(tmp_path):
    # R manifests exactly 8 datasets and wehoop exports exactly 8 matching
    # load_wnba_*_manifest() loaders. team_box is not one of them -- writing
    # (and publishing) a manifest for it would create an asset nothing reads.
    spec = REGISTRY["team_box"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    assert not io.manifest_path(spec, tmp_path).exists()


def test_manifest_is_an_append_log_with_source_endpoint(tmp_path):
    spec = REGISTRY["standings"]  # one of the 8 manifested datasets
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    io.write_dataset(pl.DataFrame({"team_id": [1, 2, 3]}), spec, 2025, base=tmp_path)
    m = pl.read_csv(io.manifest_path(spec, tmp_path))
    # R fwrite(append=TRUE): one row per RUN, not per season. The committed
    # manifests carry 140-155 rows; collapsing to one row per season here would
    # destroy that history (publish is what collapses it for the asset).
    assert m.columns == ["season", "row_count", "generated_at_utc", "source_endpoint"]
    assert m.height == 2
    assert m["row_count"].to_list() == [1, 3]
    assert (
        m["source_endpoint"].to_list()
        == [
            "https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-raw/main/wnba/standings/json/2025.json"
        ]
        * 2
    )
