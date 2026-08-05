"""Stage 16 -- player_core.

**No numbered R stage produces `player_core` on its own.** No R file references it at all -- this is an open PARITY GAP.
Numbered after the R range so the shared 01-13 numbers keep meaning
the same dataset in both languages (never compact a hole).

Thin shim over the tested build package: the pipeline logic lives in
``wnba_data_build``; this file exists so the stage sequence is readable from a
directory listing.

Equivalent to::

    python -m wnba_data_build --dataset player_core -s <start> -e <end>
"""

from __future__ import annotations

import sys

from wnba_data_build.cli import main

DATASET = "player_core"

if __name__ == "__main__":
    # DATASET is appended, not prepended: argparse takes the last value for a
    # single-value option, so a stray --dataset on the command line cannot make
    # stage 16 build something other than player_core.
    sys.exit(main([*sys.argv[1:], "--dataset", DATASET]))
