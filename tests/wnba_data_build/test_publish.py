import subprocess
from pathlib import Path

import polars as pl
import pytest
from wnba_data_build import io, publish
from wnba_data_build.config import REGISTRY

#: release metadata sidecars -- asserted separately, not a data asset
SIDECARS = ("timestamp.", "package_function.")


def test_publish_uploads_each_file_with_clobber(tmp_path):
    spec = REGISTRY["team_box"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    res = publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,  # release already exists
    )
    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    # team_box is not manifested -> parquet + rds + csv. The tree csv is gzipped,
    # but the release asset contract is plain .csv (decompressed to a temp file).
    # rds is NOT optional: wehoop::load_wnba_* reads rds exclusively, so a
    # publish without it freezes the loader while the parquet moves on.
    assert len(uploads) == 3
    assets = sorted(Path(c[3]).name for c in uploads)  # gh release upload <tag> <path>
    assert assets == ["team_box_2025.csv", "team_box_2025.parquet", "team_box_2025.rds"]
    assert all("--clobber" in c for c in uploads)
    assert res["tag"] == spec.tag


def test_publish_uploads_manifest_for_manifested_datasets(tmp_path):
    spec = REGISTRY["standings"]
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: True,
    )
    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assets = sorted(Path(c[3]).name for c in uploads)
    # The manifest is load-bearing: wehoop exports load_wnba_standings_manifest().
    assert assets == [
        "standings_2025.csv",
        "standings_2025.parquet",
        "standings_2025.rds",
        "wnba_standings_in_data_repo.csv",
    ]


def test_published_manifest_collapses_the_log_to_the_latest_run_per_season(tmp_path):
    spec = REGISTRY["standings"]
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2024, base=tmp_path)
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    io.write_dataset(pl.DataFrame({"team_id": [1, 2, 3]}), spec, 2025, base=tmp_path)  # rerun

    asset = publish._manifest_asset(spec, tmp_path)
    m = pl.read_csv(asset)
    # One row per season, ascending -- and the LATEST run wins. R's helper keeps
    # the FIRST (dplyr distinct), which froze the published row_count at a
    # season's first-ever run; we publish the count that describes the asset we
    # are actually shipping beside it.
    assert m["season"].to_list() == [2024, 2025]
    assert m["row_count"].to_list() == [1, 3]


def test_publish_creates_release_when_missing(tmp_path):
    spec = REGISTRY["team_box"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []
    publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=lambda args: calls.append(args),
        exists_check=lambda tag, repo: False,
    )
    assert any(c[:2] == ["release", "create"] for c in calls)


def test_release_digests_parses_the_listing(monkeypatch):
    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(
            args, 0, stdout="a.parquet\tsha256:aa\nb.rds\tnull\n", stderr=""
        )

    monkeypatch.setattr(publish.subprocess, "run", fake_run)
    assert publish._release_digests("tag", "repo") == {"a.parquet": "sha256:aa", "b.rds": "null"}


def test_release_digests_none_on_genuine_not_found(monkeypatch):
    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args, 1, stdout="", stderr="gh: Not Found (HTTP 404)")

    monkeypatch.setattr(publish.subprocess, "run", fake_run)
    assert publish._release_digests("tag", "repo") is None


def test_release_digests_raises_loudly_on_other_failure(monkeypatch):
    # Regression: a rate-limit / auth / network failure must never be read as
    # "release missing" -- that's the 2026-08-23 incident (fail-open crashed
    # a backfill mid-publish because `gh release create` ran on a live tag).
    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(
            args, 1, stdout="", stderr="API rate limit exceeded (HTTP 403)"
        )

    monkeypatch.setattr(publish.subprocess, "run", fake_run)
    with pytest.raises(RuntimeError, match="rate limit"):
        publish._release_digests("tag", "repo")


def test_publish_tolerates_create_race(tmp_path):
    # exists() said missing, but the injected runner's create call races
    # against a concurrent creator and gh reports "already exists" -- the
    # publish must continue (log + proceed to upload), not crash.
    spec = REGISTRY["team_box"]
    io.write_dataset(pl.DataFrame({"game_id": [1]}), spec, 2025, base=tmp_path)
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["release", "create"]:
            raise subprocess.CalledProcessError(1, args, stderr="already exists")

    res = publish.publish_dataset(
        spec,
        2025,
        base=tmp_path,
        runner=runner,
        exists_check=lambda tag, repo: False,
    )
    uploads = [
        c
        for c in calls
        if c[:2] == ["release", "upload"] and not Path(c[3]).name.startswith(SIDECARS)
    ]
    assert len(uploads) == 3
    assert res["uploaded"] == 3


def _standings(tmp_path):
    spec = REGISTRY["standings"]  # manifested: parquet + rds + csv + manifest
    io.write_dataset(pl.DataFrame({"team_id": [1]}), spec, 2025, base=tmp_path)
    return spec, publish._dataset_files(spec, 2025, tmp_path)


def _publish_against(monkeypatch, tmp_path, spec, listing):
    monkeypatch.setattr(publish, "_release_digests", lambda tag, repo: listing)
    calls = []
    res = publish.publish_dataset(spec, 2025, base=tmp_path, runner=calls.append)
    return res, [Path(c[3]).name for c in calls if c[:2] == ["release", "upload"]], calls


def test_unchanged_parquet_skips_season_even_though_rds_and_manifest_restamped(
    tmp_path, monkeypatch
):
    spec, files = _standings(tmp_path)
    listing = {f.name: "sha256:yesterdays-bytes" for f in files}
    pq = next(f for f in files if f.suffix == ".parquet")
    listing[pq.name] = publish._sha256(pq)
    res, uploads, calls = _publish_against(monkeypatch, tmp_path, spec, listing)
    assert res["uploaded"] == 0 and calls == []  # no upload, no sidecar stamp


def test_changed_parquet_uploads_changed_files_parquet_last_then_stamps(tmp_path, monkeypatch):
    spec, files = _standings(tmp_path)
    listing = {f.name: publish._sha256(f) for f in files}
    pq = next(f for f in files if f.suffix == ".parquet")
    rds = next(f for f in files if f.suffix == ".rds")
    listing[pq.name] = listing[rds.name] = "sha256:stale"
    res, uploads, _calls = _publish_against(monkeypatch, tmp_path, spec, listing)
    assert uploads[:2] == [rds.name, pq.name]  # csv + manifest unchanged -> skipped
    assert len(uploads) == 2 + 4 and res["uploaded"] == 2


def test_missing_sibling_uploads_even_when_parquet_matches(tmp_path, monkeypatch):
    spec, files = _standings(tmp_path)
    pq = next(f for f in files if f.suffix == ".parquet")
    _res, uploads, _calls = _publish_against(
        monkeypatch, tmp_path, spec, {pq.name: publish._sha256(pq)}
    )
    assert uploads[: len(files)][-1] == pq.name and len(uploads) == len(files) + 4


def test_no_release_creates_it_then_uploads_everything(tmp_path, monkeypatch):
    spec, files = _standings(tmp_path)
    res, uploads, calls = _publish_against(monkeypatch, tmp_path, spec, None)
    assert calls[0][:3] == ["release", "create", spec.tag]
    assert res["uploaded"] == len(files) and uploads[len(files) - 1].endswith(".parquet")
