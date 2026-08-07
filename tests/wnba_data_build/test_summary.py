"""Run summary: Python port of R/run_summary.R (kept in-repo as the rollback path).

The log format is the contract. The R producer emits
``Updated YYYY ESPN LEAGUE <ds> GitHub Release``; the Python producer emits
``uploaded <file> -> <tag> (asset N/M)``. Both must be recognised -- the R
rollback path still writes the former, and dropping it would silently report
0 updated on any R run.
"""

from __future__ import annotations

from wnba_data_build.summary import (
    extract_errors,
    extract_warnings,
    render,
    summarize_run,
    summarize_warnings,
    updated_tags,
)


def test_recognises_the_python_producer_format():
    lines = ["uploaded play_by_play_2026.parquet -> espn_womens_college_basketball_pbp (asset 1/3)"]
    assert updated_tags(lines) == ["espn_womens_college_basketball_pbp"]


def test_recognises_the_legacy_r_producer_format():
    """The R rollback path still writes this; dropping it reports 0 updated."""
    lines = ["Updated 2026 ESPN WNBA play_by_play GitHub Release"]
    assert updated_tags(lines) == ["play_by_play"]


def test_both_formats_in_one_run_are_merged_and_deduped():
    lines = [
        "uploaded x.parquet -> espn_wnba_pbp (asset 1/3)",
        "uploaded y.csv -> espn_wnba_pbp (asset 2/3)",
        "Updated 2026 ESPN WNBA standings GitHub Release",
    ]
    assert updated_tags(lines) == ["espn_wnba_pbp", "standings"]


def test_warnings_and_errors_are_separated():
    lines = [
        "! 12:00: skip pbp 2026/401811123: missing raw JSON",
        "::warning ::shots for season 2026 exited with code 1",
        "Error: something broke",
        "::error ::officials failed",
    ]
    assert len(extract_warnings(lines)) == 2
    assert len(extract_errors(lines)) == 2


def test_skip_noise_collapses_to_one_line_per_dataset():
    warnings = [
        "! skip pbp 2026/1: 404",
        "! skip pbp 2026/2: 404",
        "! skip officials 2026/3: 404",
    ]
    rolled = summarize_warnings(warnings)
    assert any("2 game(s) skipped in pbp" in r for r in rolled)
    assert any("1 game(s) skipped in officials" in r for r in rolled)


def test_clean_run_reports_nothing(tmp_path):
    log = tmp_path / "wehoop_wnba_data_logfile_2025.log"
    log.write_text("pbp 2025: season complete\nseason 2025 EXIT=0\n", encoding="utf-8")
    result = summarize_run([log])
    assert result["warnings"] == []
    assert result["errors"] == []
    assert result["seasons"] == {2025: 0}


def test_exit_codes_are_collected_per_season(tmp_path):
    a = tmp_path / "wehoop_wnba_data_logfile_2025.log"
    b = tmp_path / "wehoop_wnba_data_logfile_2026.log"
    a.write_text("season 2025 EXIT=0\n", encoding="utf-8")
    b.write_text("::warning ::shots failed\nseason 2026 EXIT=1\n", encoding="utf-8")
    result = summarize_run([a, b])
    assert result["seasons"] == {2025: 0, 2026: 1}
    assert len(result["warnings"]) == 1


def test_missing_logfile_is_not_a_crash(tmp_path):
    assert summarize_run([tmp_path / "nope.log"])["seasons"] == {}


def test_render_produces_markdown():
    out = render({"updated": ["espn_wnba_pbp"], "warnings": [], "errors": [], "seasons": {2026: 0}})
    assert "## WNBA data run summary" in out
    assert "`espn_wnba_pbp`" in out
    assert "| 2026 | 0 |" in out
