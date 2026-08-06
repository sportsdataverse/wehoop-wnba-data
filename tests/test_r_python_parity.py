"""R <-> Python stage parity.

Standing policy (2026-08-03): this repo carries BOTH pipelines. Python is
primary and gets the work; the R chain is maintained as the methodological /
language equivalent; **both move together when either changes.**

The two sides decompose differently on purpose — R is dataset-per-file
(``R/<league>_NN_<key>_creation.R``, usually with an ``espn_`` prefix), Python
is a build package with datasets as ``config.REGISTRY`` rows. The numbered
shims in ``python/`` bridge that: each shim carries the SAME number as its R
twin, so the stage sequence is comparable by eye and by test.

Numbers are per-repo and express INTENDED build order plus a stable
cross-language dataset identity. They are NOT the execution schedule: the daily
driver's ordered array is the executable truth and may deviate for cadence
reasons (hoopR-nba-data runs 08_draft last, after 10_officials, because draft is
annual). Python drivers generally iterate dataset KEYS rather than the numbered
shims, so the shims are a legibility/identity device more than an entry point.
The ``-data`` namespace is separate from ``-raw``, so this repo's numbers need
not match its sibling leagues' — only its own R chain. Holes are deliberate and
never compacted.

**Neither side is authoritative.** A failure here means the pipelines disagree
about what they produce; a human decides which is right. The messages are
written to support that decision, not to pre-empt it.

Scope: this is the contract-level guard (which datasets, which numbers). It
does NOT prove the two produce the same values — that is the output-parity
harness, a separate and heavier phase.

Portability: the engine below is repo-agnostic — it derives the league slug and
the build package from the repo layout rather than hardcoding them, so this
file is byte-identical across every ``-data`` repo that carries the twin chain.
The ONLY repo-specific content is the ``KNOWN_UNPAIRED`` block. Diffing this
file against a sibling repo's copy should show that block and nothing else; any
other difference is drift between copies, not a real per-league distinction.

Requirements for a repo to carry this gate: a numbered R stage chain under
``R/`` AND a ``python/<league>_data_build/config.py`` declaring ``REGISTRY``.
A repo with no R dataset chain (helpers or scrape stages only) cannot use this
module — there is nothing to pair, and a vacuous pass is worse than no gate.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# --------------------------------------------------------------------------
# Repo-specific data — the ONLY block that differs between copies of this file.
#
# Datasets Python declares that no NUMBERED R stage owns. Each entry must say
# WHY, so that a genuinely new divergence fails instead of blending in.
# Removing a dataset from this map is how a parity gap gets closed.
# --------------------------------------------------------------------------
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
}

#: Numbered shims that are NOT datasets — orchestrators (``all``) or artifacts
#: assembled from other stages (``schedule_master``). They legitimately have no
#: REGISTRY row. Keep this as tight as possible: every name here is a shim the
#: "no shim without a registry entry" check can no longer catch.
NON_DATASET_STAGES: set[str] = set()
# --------------------------------------------------------------------------
# End repo-specific data. Everything below is the shared engine.
# --------------------------------------------------------------------------

#: Filename suffix marking a numbered build stage on both sides.
STAGE_SUFFIX = "creation"

#: Build-package directory suffix; its prefix is the league slug.
PACKAGE_SUFFIX = "_data_build"


def _config_path() -> Path:
    """The single ``python/<league>_data_build/config.py`` in this repo."""
    hits = sorted((REPO / "python").glob(f"*{PACKAGE_SUFFIX}/config.py"))
    assert len(hits) == 1, (
        f"expected exactly one python/*{PACKAGE_SUFFIX}/config.py under {REPO}, "
        f"found {[str(h.relative_to(REPO)) for h in hits]}.\n"
        "This gate identifies the repo by its build package; zero means the repo "
        "cannot carry the gate, more than one means the layout is ambiguous."
    )
    return hits[0]


def _league() -> str:
    """League slug, derived from the build package name (``nba_data_build`` -> ``nba``)."""
    slug = _config_path().parent.name[: -len(PACKAGE_SUFFIX)]
    assert slug, f"could not derive a league slug from {_config_path().parent.name!r}"
    return slug


def _stage_re() -> re.Pattern[str]:
    """Matches ``[espn_]<league>_NN_<key>_<suffix>`` for THIS repo's league.

    Pinning the league (rather than accepting any slug) is deliberate: a stray
    file from a sibling league would otherwise be silently adopted as a stage.
    """
    return re.compile(
        rf"^(?:espn_)?{re.escape(_league())}_(?P<num>\d{{2}})_(?P<key>.+)_{re.escape(STAGE_SUFFIX)}$"
    )


def _stages(subdir: str, suffix: str) -> dict[str, str]:
    """key -> NN for every numbered stage file in ``subdir``."""
    pattern = _stage_re()
    found: list[tuple[str, str]] = []
    for path in sorted((REPO / subdir).glob(f"*{suffix}")):
        match = pattern.match(path.stem)
        if match:
            found.append((match.group("key"), match.group("num")))

    # A dict would silently keep the last of a duplicated key, hiding the case
    # where two files claim the same dataset under different numbers.
    seen: dict[str, str] = {}
    dupes = []
    for key, num in found:
        if key in seen and seen[key] != num:
            dupes.append((key, seen[key], num))
        seen[key] = num
    assert not dupes, f"duplicate dataset keys among {subdir}/*{suffix}:\n" + "\n".join(
        f"  {k}: numbered both {a} and {b}" for k, a, b in dupes
    )
    return seen


def _registry_keys() -> list[str]:
    config = _config_path()
    tree = ast.parse(config.read_text(encoding="utf-8"))
    for node in tree.body:
        tgs = [node.target] if isinstance(node, ast.AnnAssign) else getattr(node, "targets", [])
        if any(isinstance(t, ast.Name) and t.id == "REGISTRY" for t in tgs):
            return [ast.literal_eval(k) for k in node.value.keys]
    raise AssertionError(f"no REGISTRY assignment found in {config}")


def _r_stages() -> dict[str, str]:
    """key -> NN, from the R filenames."""
    return _stages("R", ".R")


def _py_stages() -> dict[str, str]:
    """key -> NN, from the numbered python shims."""
    return _stages("python", ".py")


def test_repo_layout_is_discoverable():
    """The engine self-configures; if discovery is wrong every result below is."""
    assert _config_path().is_file()
    assert _league(), "no league slug derived"


def test_parsers_find_something():
    """Guard the guard — a regex that matched nothing would pass everything below."""
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
    """A dataset the package can build but no shim exposes is invisible in the
    directory listing, which defeats 'the listing IS the pipeline'."""
    missing = sorted(set(_registry_keys()) - set(_py_stages()))
    assert not missing, f"REGISTRY datasets with no numbered shim: {missing}"


def test_no_shim_without_a_registry_entry():
    """The inverse: a shim for a dataset the package cannot build would fail only
    when someone ran it. Orchestrator / assembled-artifact shims are exempted by
    name via NON_DATASET_STAGES."""
    extra = sorted(set(_py_stages()) - set(_registry_keys()) - NON_DATASET_STAGES)
    assert not extra, f"numbered shims with no REGISTRY entry: {extra}"


def test_non_dataset_exemptions_are_live():
    """An exemption for a shim that no longer exists is dead weight that would
    silently cover a future stage of the same name."""
    stale = sorted(NON_DATASET_STAGES - set(_py_stages()))
    assert not stale, (
        f"NON_DATASET_STAGES exempts {stale}, but no such shim exists. Remove the entry."
    )


def test_unpaired_datasets_are_declared():
    """Python-only datasets must be listed in KNOWN_UNPAIRED with a reason.

    This is the test that turns 'we know about these three' into something
    enforceable: a NEW python-only dataset fails until someone writes down
    whether R bundles it elsewhere or it is a real gap.
    """
    unpaired = sorted(set(_registry_keys()) - set(_r_stages()))
    undeclared = [k for k in unpaired if k not in KNOWN_UNPAIRED]
    assert not undeclared, (
        f"Python produces {undeclared} with no numbered R stage and no entry in "
        "KNOWN_UNPAIRED.\nEither add the R stage, or declare why it is unpaired "
        "(bundled elsewhere in R? genuine gap?) — do not leave it ambiguous."
    )
    stale = [k for k in KNOWN_UNPAIRED if k not in unpaired]
    assert not stale, (
        f"KNOWN_UNPAIRED still lists {stale}, but R now pairs them. "
        "Remove the entry — a closed gap should not keep its excuse."
    )
