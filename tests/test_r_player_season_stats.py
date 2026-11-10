"""R twin of player_season_stats reads the same season row as the Python build.

ESPN's per-category flat ``totals`` array is the CAREER line; the season line is
the ``statistics[]`` entry whose ``season.year`` matches (wehoop-wnba-data#29,
sportsdataverse-py#1094). These run the R stage's parsers on the real 2025
fixtures: values and team fields must equal sdv-py's helper, and a traded
player's "2025 Totals" row must win over its team stints wherever it sits.
Needs a local ``Rscript`` with jsonlite/purrr/tibble/glue/rlang; skipped
otherwise (the CI runner has no R).
"""

from __future__ import annotations

import io
import json
import shutil
import subprocess
from pathlib import Path

import polars as pl
import pytest
from sportsdataverse.wnba import helper_wnba_player_season_stats

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "R" / "espn_wnba_05_player_season_stats_creation.R"
RAW = REPO / "tests" / "fixtures" / "raw" / "wnba" / "player_season_stats" / "json"
SEASON = 2025
TRADED = 2529047  # real traded-player payload: two team stints + a "2025 Totals" row
COLS = ["athlete_id", "team_id", "team_display_name", "category", "stat_name", "display_value"]

# Evaluates only the script's `name <- function(...)` definitions, so its
# optparse / network main loop never runs. Args: script, raw_base, season,
# optional identity-lookup JSON ({athlete_id: {team_id, ...}}).
R_DRIVER = r"""
args <- commandArgs(trailingOnly = TRUE)
suppressPackageStartupMessages({ library(rlang); library(purrr); library(tibble) })
for (e in parse(file = args[[1]])) {
  if (is.call(e) && identical(e[[1]], as.name("<-")) && is.call(e[[3]]) &&
      identical(e[[3]][[1]], as.name("function"))) eval(e, globalenv())
}
season <- as.integer(args[[3]])
ident <- if (length(args) >= 4) jsonlite::fromJSON(args[[4]], simplifyVector = FALSE) else list()
ids <- sub("\\.json$", "", list.files(file.path(args[[2]], season), pattern = "\\.json$"))
out <- purrr::map_dfr(ids, function(a) parse_one_athlete(season, a, ident, args[[2]]))
cols <- c("athlete_id", "team_id", "team_display_name", "category", "stat_name", "display_value")
utils::write.csv(out[, cols], stdout(), row.names = FALSE)
"""


def _r_available() -> bool:
    if shutil.which("Rscript") is None:
        return False
    probe = 'q(status = !all(sapply(c("jsonlite","purrr","tibble","glue","rlang"), requireNamespace, quietly = TRUE)))'
    return subprocess.run(["Rscript", "-e", probe], capture_output=True).returncode == 0


pytestmark = pytest.mark.skipif(
    not _r_available(), reason="needs Rscript + jsonlite/purrr/tibble/glue/rlang"
)


def _run_r(tmp_path: Path, raw_base: Path, ident: dict | None = None) -> pl.DataFrame:
    driver = tmp_path / "driver.R"
    driver.write_text(R_DRIVER, encoding="utf-8")
    cmd = ["Rscript", str(driver), str(SCRIPT), raw_base.as_posix(), str(SEASON)]
    if ident is not None:
        (tmp_path / "ident.json").write_text(json.dumps(ident), encoding="utf-8")
        cmd.append((tmp_path / "ident.json").as_posix())
    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", check=True)
    return pl.read_csv(
        io.StringIO(res.stdout),
        schema_overrides={"display_value": pl.String, "team_display_name": pl.String},
        null_values="NA",
    ).with_columns(pl.col("athlete_id", "team_id").cast(pl.Int32))


def _one_payload(tmp_path: Path, payload: dict) -> Path:
    """A raw root holding just ``payload`` as the traded athlete's file."""
    root = tmp_path / "raw"
    (root / str(SEASON)).mkdir(parents=True)
    (root / str(SEASON) / f"{TRADED}.json").write_text(json.dumps(payload), encoding="utf-8")
    return root


def _season_value(payload: dict, slug: str, stat: str) -> str:
    """The ``averages`` category's ``stat`` from the season row tagged ``slug``."""
    cat = next(c for c in payload["categories"] if c["name"] == "averages")
    row = next(
        s for s in cat["statistics"] if s["season"]["year"] == SEASON and s.get("teamSlug") == slug
    )
    return str(row["stats"][cat["names"].index(stat)])


def _gp(r: pl.DataFrame) -> pl.DataFrame:
    return r.filter((pl.col("category") == "averages") & (pl.col("stat_name") == "gamesPlayed"))


def test_r_player_season_stats_matches_the_python_build(tmp_path):
    r = _run_r(tmp_path, RAW)
    py = pl.concat(
        [
            helper_wnba_player_season_stats(
                json.loads(f.read_text(encoding="utf-8")), season=SEASON, athlete_id=int(f.stem)
            )
            for f in sorted(RAW.joinpath(str(SEASON)).glob("*.json"))
        ]
    ).select(COLS)
    assert r.schema["athlete_id"] == py.schema["athlete_id"]
    assert r.schema["team_id"] == py.schema["team_id"]
    assert r.sort(COLS, nulls_last=True).equals(py.sort(COLS, nulls_last=True))
    # The season line, not the career one: no WNBA season has more than 44 games.
    gp = r.filter(pl.col("stat_name") == "gamesPlayed")
    assert gp.height > 0
    assert gp["display_value"].cast(pl.Float64).max() <= 44


def test_r_traded_player_takes_the_totals_row_wherever_it_sits(tmp_path):
    payload = json.loads((RAW / str(SEASON) / f"{TRADED}.json").read_text(encoding="utf-8"))
    totals_slug = f"{SEASON} Totals"
    want = _season_value(payload, totals_slug, "gamesPlayed")
    assert want != _season_value(
        payload, "indiana-fever", "gamesPlayed"
    )  # the fixture can tell them apart
    # Totals rows first: a "last stint" rule would now pick a team stint.
    for cat in payload["categories"]:
        cat["statistics"].sort(key=lambda s: s.get("teamSlug") != totals_slug)
    ident = {str(TRADED): {"team_id": 999, "team_display_name": "Roster Team"}}
    gp = _gp(_run_r(tmp_path, _one_payload(tmp_path, payload), ident))
    assert gp["display_value"].to_list() == [want]
    # The Totals row names no team, so both team fields come from the roster identity.
    assert gp.select("team_id", "team_display_name").row(0) == (999, "Roster Team")


def test_r_team_stint_team_fields_beat_the_roster_identity(tmp_path):
    payload = json.loads((RAW / str(SEASON) / f"{TRADED}.json").read_text(encoding="utf-8"))
    for cat in payload["categories"]:  # no Totals row: the last stint (Indiana) is the season row
        cat["statistics"] = [
            s for s in cat["statistics"] if s.get("teamSlug") != f"{SEASON} Totals"
        ]
    ident = {str(TRADED): {"team_id": 999, "team_display_name": "Roster Team"}}
    gp = _gp(_run_r(tmp_path, _one_payload(tmp_path, payload), ident))
    assert gp["display_value"].to_list() == [_season_value(payload, "indiana-fever", "gamesPlayed")]
    assert gp.select("team_id", "team_display_name").row(0) == (
        5,
        payload["teams"]["indiana-fever"]["displayName"],
    )
