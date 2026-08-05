"""R <-> Python stage parity.

Standing policy (2026-08-03): this repo carries BOTH pipelines. Python is
primary and gets the work; the R chain is maintained as the methodological /
language equivalent; **both move together when either changes.**

The two sides decompose differently on purpose — R is dataset-per-file
(``R/espn_wnba_NN_<key>_creation.R``), Python is a build package with datasets
as ``config.REGISTRY`` rows. The numbered shims in ``python/`` bridge that:
``python/espn_wnba_NN_<key>_creation.py`` carries the SAME number as its R twin.

Numbers are per-repo. ``-data`` numbering follows BUILD ORDER and is a separate
namespace from ``-raw``, so this repo's numbers need not match its sibling
leagues' — only its own R chain. Holes are deliberate and never compacted.

**Neither side is authoritative.** A failure means the pipelines disagree about
what they produce; a human decides which is right.

Scope: contract-level (which datasets, which numbers). It does NOT prove the
two produce the same values — that is the output-parity harness, a separate
and heavier phase.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CONFIG = REPO / "python" / "wnba_data_build" / "config.py"

_R_STAGE = re.compile(r"^(?:espn_)?wnba_(?P<num>\d{2})_(?P<key>.+)_creation$")
_PY_STAGE = re.compile(r"^espn_wnba_(?P<num>\d{2})_(?P<key>.+)_creation$")

# Datasets Python declares that no NUMBERED R stage owns. Each entry must say
# WHY, so a genuinely new divergence fails instead of blending in. Removing an
# entry is how a gap gets closed.
KNOWN_UNPAIRED: dict[str, str] = {
    "schedules": (
        "R writes it inside espn_wnba_01_pbp_creation.R (verified: saveRDS to "
        "wnba/schedules/rds/) rather than as its own numbered stage. Later stages "
        "re-write it to stamp has_* flags."
    ),
    "shots": (
        "R writes it inside espn_wnba_01_pbp_creation.R (verified: saveRDS + "
        "write_parquet to wnba/shots/) rather than as its own numbered stage."
    ),
    # OPEN PARITY GAP, and blocked upstream rather than simply unwritten.
    #
    # Python builds it from a flat ONE-ROW-PER-ATHLETE projection of the ESPN
    # core-v2 athlete resource. The R package's nearest function
    # (`.espn_basketball_athlete_info` / its wehoop equivalent) is not a twin:
    #   1. it FETCHES, with no parse-only entry point, so a stage cannot feed it
    #      the raw tree — and re-fetching would break the one-way raw->data
    #      boundary AND mean the two pipelines read different bytes;
    #   2. it returns a NAMED LIST of tibbles (Bio, Team, ...), not the flat
    #      single row player_core releases.
    # All four ESPN -data repos share this gap identically, so it closes with ONE
    # league-parameterized upstream change (parse-only + flat projection, with
    # roxygen + testthat) and a release — not four separate fixes. This repo's
    # own rule ("all JSON I/O goes through the R package; no bespoke parsing
    # here") rules out a local workaround.
    "player_core": "OPEN PARITY GAP — blocked on an upstream parse-only flat projection; see the note above.",
}


def _registry_keys() -> list[str]:
    tree = ast.parse(CONFIG.read_text(encoding="utf-8"))
    for node in tree.body:
        tgs = [node.target] if isinstance(node, ast.AnnAssign) else getattr(node, "targets", [])
        if any(isinstance(t, ast.Name) and t.id == "REGISTRY" for t in tgs):
            return [ast.literal_eval(k) for k in node.value.keys]
    raise AssertionError(f"no REGISTRY assignment found in {CONFIG}")


def _r_stages() -> dict[str, str]:
    """key -> NN, from the R filenames."""
    out = {}
    for p in sorted((REPO / "R").glob("*.R")):
        m = _R_STAGE.match(p.stem)
        if m:
            out[m.group("key")] = m.group("num")
    return out


def _py_stages() -> dict[str, str]:
    """key -> NN, from the numbered python shims."""
    out = {}
    for p in sorted((REPO / "python").glob("*.py")):
        m = _PY_STAGE.match(p.stem)
        if m:
            out[m.group("key")] = m.group("num")
    return out


def test_parsers_find_something():
    """Guard the guard — a regex matching nothing would pass everything below."""
    assert _registry_keys(), "registry parsed empty"
    assert _r_stages(), "no numbered R stages found"
    assert _py_stages(), "no numbered python shims found"


def test_every_r_stage_has_a_python_shim():
    r, py = _r_stages(), _py_stages()
    missing = sorted(set(r) - set(py))
    assert not missing, (
        f"R stages with no numbered python shim: {missing}\n"
        "Every R stage needs its Python twin — that is the point of the numbering."
    )


def test_stage_numbers_agree():
    """The number must mean the same dataset in both languages, within this repo."""
    r, py = _r_stages(), _py_stages()
    clashes = [(k, r[k], py[k]) for k in sorted(set(r) & set(py)) if r[k] != py[k]]
    assert not clashes, (
        "Same dataset, different stage number:\n"
        + "\n".join(f"  {k}: R={rn} python={pn}" for k, rn, pn in clashes)
        + "\nRenumbering one side alone breaks the comparison the numbers exist for."
    )


def test_every_registry_dataset_has_a_shim():
    missing = sorted(set(_registry_keys()) - set(_py_stages()))
    assert not missing, f"REGISTRY datasets with no numbered shim: {missing}"


def test_no_shim_without_a_registry_entry():
    extra = sorted(set(_py_stages()) - set(_registry_keys()))
    assert not extra, f"numbered shims with no REGISTRY entry: {extra}"


def test_unpaired_datasets_are_declared():
    """Python-only datasets must be listed in KNOWN_UNPAIRED with a reason."""
    unpaired = sorted(set(_registry_keys()) - set(_r_stages()))
    undeclared = [k for k in unpaired if k not in KNOWN_UNPAIRED]
    assert not undeclared, (
        f"Python produces {undeclared} with no numbered R stage and no entry in "
        "KNOWN_UNPAIRED.\nEither add the R stage, or declare why it is unpaired."
    )
    stale = [k for k in KNOWN_UNPAIRED if k not in unpaired]
    assert not stale, (
        f"KNOWN_UNPAIRED still lists {stale}, but R now pairs them. "
        "Remove the entry — a closed gap should not keep its excuse."
    )
