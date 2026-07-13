"""Parity assertions: Python-built frame vs the R-released parquet (the oracle).

The oracle fixture is PRE-FILTERED to the captured game-ids (see Task 7 capture),
so `py` and `r` cover the same games and row counts are directly comparable.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl


def assert_parquet_parity(
    py: pl.DataFrame,
    r_parquet: Path,
    *,
    keys: list[str],
    sample_cols: list[str],
    r_only_all_null_ok: tuple[str, ...] = (),
    require_order: bool = True,
    dtype_upgrades: dict[str, tuple[pl.DataType, pl.DataType]] | None = None,
) -> None:
    r = pl.read_parquet(r_parquet)
    if r_only_all_null_ok:
        # Season-union artifacts: a column another game contributed to the
        # season compile can appear in the oracle slice but not in a subset
        # rebuild — tolerated ONLY for the named columns AND only when
        # entirely null here (an explicit allowlist so a regression in a
        # genuinely-produced column can't hide behind this tolerance).
        r_only = [c for c in r.columns if c not in py.columns]
        assert set(r_only) <= set(r_only_all_null_ok), (
            f"unexpected r-only columns: {sorted(set(r_only) - set(r_only_all_null_ok))}"
        )
        droppable = [c for c in r_only if r.get_column(c).null_count() == r.height]
        assert r_only == droppable, (
            f"r-only columns with real values: {sorted(set(r_only) - set(droppable))}"
        )
        r = r.drop(droppable)
        sample_cols = [c for c in sample_cols if c not in droppable]
    assert set(py.columns) == set(r.columns), (
        f"column set diverges: only-py={set(py.columns) - set(r.columns)}, "
        f"only-r={set(r.columns) - set(py.columns)}"
    )
    # Column ORDER is part of the released-parquet contract (R select order).
    # require_order=False is for payload-order-driven frames (e.g. pbp) whose
    # oracle was compiled from a since-re-scraped payload layout: the released
    # 2025 and 2026 assets themselves disagree on order, so order parity is
    # not reproducible from current inputs.
    if require_order:
        assert py.columns == list(r.columns), (
            f"column order diverges: py={py.columns} r={list(r.columns)}"
        )
    # Deliberate dtype improvements over the R release: each entry pins the
    # exact (py, r) dtype pair, and values are still compared under the
    # oracle's (lossy) dtype so an unintended drift can't hide behind it.
    upgrades = dtype_upgrades or {}
    for c, (py_dtype, r_dtype) in upgrades.items():
        assert py.schema[c] == py_dtype, f"{c}: expected py dtype {py_dtype}, got {py.schema[c]}"
        assert r.schema[c] == r_dtype, f"{c}: expected oracle dtype {r_dtype}, got {r.schema[c]}"
    for c in keys + sample_cols:
        if c in upgrades:
            continue
        assert py.schema[c] == r.schema[c], (
            f"dtype mismatch on {c}: py={py.schema[c]} r={r.schema[c]}"
        )
    assert py.height == r.height, f"row count: py={py.height} r={r.height}"
    cols = keys + sample_cols
    py_cmp = py.with_columns([pl.col(c).cast(r.schema[c]) for c in upgrades if c in cols])
    pys = py_cmp.sort(keys).select(cols)
    rs = r.sort(keys).select(cols)
    assert pys.equals(rs), "value mismatch on keys+sample cols after sort"
