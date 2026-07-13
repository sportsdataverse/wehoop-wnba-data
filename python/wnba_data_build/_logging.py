"""Pipeline logging -- timestamped lines on stdout.

Everything the pipeline reports flows through one ``wnba_data_build`` logger
writing to *stdout* (not stderr), because the daily processor tees stdout into
the per-season rotating logfile (``logs/wehoop_wnba_data_logfile_{season}.log``)
-- the same paradigm as the other ``-data`` repos. Each record is flushed on
emit so lines land in the Actions console and the tee'd file in real time.

The handler resolves ``sys.stdout`` at emit time (not at handler creation) so
pytest's ``capsys`` redirection and any downstream stream swaps keep working.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

_FORMAT = "%(asctime)s [%(levelname)s] wnba_data_build: %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


class _StdoutHandler(logging.StreamHandler):
    """StreamHandler bound to the CURRENT sys.stdout at emit time."""

    def __init__(self) -> None:
        super().__init__(sys.stdout)

    @property
    def stream(self) -> Any:
        return sys.stdout

    @stream.setter
    def stream(self, value: Any) -> None:  # the base class assigns in __init__
        pass


def get_logger() -> logging.Logger:
    """The shared pipeline logger (configured once, idempotent)."""
    logger = logging.getLogger("wnba_data_build")
    if not logger.handlers:
        handler = _StdoutHandler()
        handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def human_size(n_bytes: int) -> str:
    """1234567 -> '1.2MB' (for upload/write confirmations)."""
    size = float(n_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size /= 1024
    return f"{size:.1f}GB"
