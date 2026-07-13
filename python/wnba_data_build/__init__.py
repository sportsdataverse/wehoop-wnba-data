"""Python producer for the ESPN WNBA release datasets.

Parity port of ``wehoop-wnba-data/R/espn_wnba_*_creation.R``. Reshapes the sibling
``wehoop-wnba-raw`` per-game JSON into season-level parquet/csv + manifest and
publishes to the ``espn_wnba_*`` release tags. R is retained
as the byte-parity oracle.
"""

__all__ = ["config", "ingest", "io", "build", "publish", "reshapers"]
