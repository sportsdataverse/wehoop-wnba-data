"""Stage 06 -- team_season_stats.

Mirrors ``R/espn_wnba_06_team_season_stats_creation.R`` -- same stage number, same dataset.

Thin shim over the tested build package: the pipeline logic lives in
``wnba_data_build``; this file exists so the stage sequence is readable from a
directory listing. It lines up with ``R/espn_wnba_06_team_season_stats_creation.R``.

Equivalent to::

    python -m wnba_data_build --dataset team_season_stats -s <start> -e <end>
"""

from __future__ import annotations

import sys

from wnba_data_build.cli import main

DATASET = "team_season_stats"

if __name__ == "__main__":
    # DATASET is appended, not prepended: argparse takes the last value for a
    # single-value option, so a stray --dataset on the command line cannot make
    # stage 06 build something other than team_season_stats.
    sys.exit(main([*sys.argv[1:], "--dataset", DATASET]))
