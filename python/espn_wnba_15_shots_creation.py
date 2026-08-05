"""Stage 15 -- shots.

**No numbered R stage produces `shots` on its own.** R emits it inside ``R/espn_wnba_01_pbp_creation.R``.
Numbered after the R range so the shared 01-13 numbers keep meaning
the same dataset in both languages (never compact a hole).

Thin shim over the tested build package: the pipeline logic lives in
``wnba_data_build``; this file exists so the stage sequence is readable from a
directory listing.

Equivalent to::

    python -m wnba_data_build --dataset shots -s <start> -e <end>
"""

from __future__ import annotations

import sys

from wnba_data_build.cli import main

DATASET = "shots"

if __name__ == "__main__":
    # DATASET is appended, not prepended: argparse takes the last value for a
    # single-value option, so a stray --dataset on the command line cannot make
    # stage 15 build something other than shots.
    sys.exit(main([*sys.argv[1:], "--dataset", DATASET]))
