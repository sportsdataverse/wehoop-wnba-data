"""Stage 12 -- schedule_crosswalk.

Mirrors ``R/wnba_12_schedule_crosswalk_creation.R`` -- same stage number, same dataset.

Thin shim over the tested build package: the pipeline logic lives in
``wnba_data_build``; this file exists so the stage sequence is readable from a
directory listing. It lines up with ``R/wnba_12_schedule_crosswalk_creation.R``.

Equivalent to::

    python -m wnba_data_build --dataset schedule_crosswalk -s <start> -e <end>
"""

from __future__ import annotations

import sys

from wnba_data_build.cli import main

DATASET = "schedule_crosswalk"

if __name__ == "__main__":
    # DATASET is appended, not prepended: argparse takes the last value for a
    # single-value option, so a stray --dataset on the command line cannot make
    # stage 12 build something other than schedule_crosswalk.
    sys.exit(main([*sys.argv[1:], "--dataset", DATASET]))
