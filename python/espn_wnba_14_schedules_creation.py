"""Stage 14 -- schedules.

**No numbered R stage produces `schedules` on its own.** R emits it inside ``R/espn_wnba_01_pbp_creation.R``, ``R/espn_wnba_02_team_box_creation.R``.
Numbered after the R range so the shared 01-13 numbers keep meaning
the same dataset in both languages (never compact a hole).

Thin shim over the tested build package: the pipeline logic lives in
``wnba_data_build``; this file exists so the stage sequence is readable from a
directory listing.

Equivalent to::

    python -m wnba_data_build --dataset schedules -s <start> -e <end>
"""

from __future__ import annotations

import sys

from wnba_data_build.cli import main

DATASET = "schedules"

if __name__ == "__main__":
    # DATASET is appended, not prepended: argparse takes the last value for a
    # single-value option, so a stray --dataset on the command line cannot make
    # stage 14 build something other than schedules.
    sys.exit(main([*sys.argv[1:], "--dataset", DATASET]))
