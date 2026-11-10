"""R twin of player_season_stats reads the same season row as the Python build.

ESPN's per-category flat ``totals`` array is the CAREER line; the season line is
the ``statistics[]`` entry whose ``season.year`` matches (wehoop-wnba-data#29,
sportsdataverse-py#1094). This runs the R stage's parsers on the real 2025
fixtures and asserts they emit the same values as the Python build. Needs a
local ``Rscript`` with jsonlite/purrr/tibble/glue/rlang; skipped otherwise (the
CI runner has no R).
"""

from __future__ import annotations

import io
import shutil
import subprocess
from pathlib import Path

import polars as pl
import pytest
from wnba_data_build import reshapers

REPO = Path(__file__).resolve().parents[1]
FX = REPO / "tests" / "fixtures" / "raw"
SCRIPT = REPO / "R" / "espn_wnba_05_player_season_stats_creation.R"
RAW = FX / "wnba" / "player_season_stats" / "json"
SEASON = 2025
KEYS = ["athlete_id", "category", "stat_name", "display_value"]

# Evaluates only the script's `name <- function(...)` definitions, so its
# optparse / network main loop never runs.
R_DRIVER = r"""
args <- commandArgs(trailingOnly = TRUE)
suppressPackageStartupMessages({ library(rlang); library(purrr); library(tibble) })
for (e in parse(file = args[[1]])) {
  if (is.call(e) && identical(e[[1]], as.name("<-")) && is.call(e[[3]]) &&
      identical(e[[3]][[1]], as.name("function"))) eval(e, globalenv())
}
season <- as.integer(args[[3]])
ids <- sub("\\.json$", "", list.files(file.path(args[[2]], season), pattern = "\\.json$"))
out <- purrr::map_dfr(ids, function(a) parse_one_athlete(season, a, list(), args[[2]]))
utils::write.csv(out[, c("athlete_id", "category", "stat_name", "display_value")], stdout(), row.names = FALSE)
"""


def _r_available() -> bool:
    if shutil.which("Rscript") is None:
        return False
    probe = 'q(status = !all(sapply(c("jsonlite","purrr","tibble","glue","rlang"), requireNamespace, quietly = TRUE)))'
    return subprocess.run(["Rscript", "-e", probe], capture_output=True).returncode == 0


@pytest.mark.skipif(not _r_available(), reason="needs Rscript + jsonlite/purrr/tibble/glue/rlang")
def test_r_player_season_stats_reads_the_season_row(tmp_path):
    driver = tmp_path / "driver.R"
    driver.write_text(R_DRIVER, encoding="utf-8")
    res = subprocess.run(
        ["Rscript", str(driver), str(SCRIPT), RAW.as_posix(), str(SEASON)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    r = pl.read_csv(
        io.StringIO(res.stdout), schema_overrides={"display_value": pl.String}
    ).with_columns(pl.col("athlete_id").cast(pl.Int32))
    py = reshapers.SEASON_BUILDERS["player_season_stats"](
        SEASON, raw_root=FX, base=tmp_path
    ).select(KEYS)
    assert r.schema["athlete_id"] == py.schema["athlete_id"]
    assert r.sort(KEYS).equals(py.sort(KEYS))
    # The season line, not the career one: no WNBA season has more than 44 games.
    gp = r.filter(pl.col("stat_name") == "gamesPlayed")
    assert gp.height > 0
    assert gp["display_value"].cast(pl.Float64).max() <= 44
