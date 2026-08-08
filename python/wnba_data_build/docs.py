"""Generate the per-dataset documentation (spec D40/D43).

Answers, for every dataset: **what builds it, where it is published, what is in
it, and when it last ran.** Hand-written docs go stale the first time a column
is added, so these are generated and drift-gated -- adding a dataset or a column
without regenerating is a red build.

Sources, all of them existing:

* ``wnba_data_build.config.REGISTRY``  -- dataset, output stem, release tag
* ``wnba_data_build.models``           -- column names and types
* ``column_descriptions.yaml`` (this package) -- authored for this league
* ``wnba/<ds>/wnba_<ds>_in_data_repo.csv``    -- per-season row counts + build times
* ``gh release view``                  -- last published, asset count (opt-in)

Descriptions are authored for THIS league and live in this package, not
borrowed from another repo. The WBB pilot's borrowed store produced confidently
wrong text -- `assists` came back as "Assisted tackles" (NFL), `half` as
"Half-inning" (baseball). A borrowed description is an invented one.

A column with no entry gets an empty cell, because an empty cell is an honest
TODO and an invented sentence is worse than nothing.

Example:
    Regenerate everything::

        uv run python -m wnba_data_build.docs

    Fail if anything is stale (CI)::

        uv run python -m wnba_data_build.docs --check
"""

from __future__ import annotations

import argparse
import json
import subprocess
from functools import lru_cache
from pathlib import Path

import polars as pl

from wnba_data_build.config import REGISTRY
from wnba_data_build.models import MODELS, polars_schema

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_DIR = REPO_ROOT / "docs" / "datasets"
LEAGUE = "wnba"
RELEASE_REPO = "sportsdataverse/sportsdataverse-data"
RELEASE_URL = f"https://github.com/{RELEASE_REPO}/releases/tag"

BEGIN = "<!-- BEGIN GENERATED: datasets -->"
END = "<!-- END GENERATED: datasets -->"

#: dataset -> the numbered script that carries its stage identity. The daily
#: driver iterates registry keys through `python -m wnba_data_build`; the
#: numbered shims are the per-dataset entry points. Every dataset -- the three
#: crosswalks included -- is Python-built in production; the R twins are the
#: `-l R` rollback path (see scripts/daily_wnba_data_processor.sh).
BUILDER = {
    "pbp": "python/espn_wnba_01_pbp_creation.py",
    "team_box": "python/espn_wnba_02_team_box_creation.py",
    "player_box": "python/espn_wnba_03_player_box_creation.py",
    "rosters": "python/espn_wnba_04_rosters_creation.py",
    "player_season_stats": "python/espn_wnba_05_player_season_stats_creation.py",
    "team_season_stats": "python/espn_wnba_06_team_season_stats_creation.py",
    "standings": "python/espn_wnba_07_standings_creation.py",
    "draft": "python/espn_wnba_08_draft_creation.py",
    "game_rosters": "python/espn_wnba_09_game_rosters_creation.py",
    "officials": "python/espn_wnba_10_officials_creation.py",
    "team_crosswalk": "python/espn_wnba_11_team_crosswalk_creation.py",
    "schedule_crosswalk": "python/espn_wnba_12_schedule_crosswalk_creation.py",
    "player_crosswalk": "python/espn_wnba_13_player_crosswalk_creation.py",
    "schedules": "python/espn_wnba_14_schedules_creation.py",
    "shots": "python/espn_wnba_15_shots_creation.py",
    "player_core": "python/espn_wnba_16_player_core_creation.py",
}

AUTOMATION = (
    "`.github/workflows/daily_wnba.yml` — cron 07:00 UTC in season, plus "
    "`repository_dispatch` from `wehoop-wnba-raw`. Runs "
    "`scripts/daily_wnba_data_processor.sh` (Python build, crosswalks "
    "included). Draft refreshes annually via `annual_wnba_draft.yml`; rosters "
    "additionally refresh weekly via `weekly_wnba.yml`."
)

#: Manifests that don't live at the default `wnba/<ds>/wnba_<ds>_in_data_repo.csv`
#: path: the three crosswalks share the `wnba/crosswalk/` directory.
_MANIFEST_OVERRIDES = {
    "team_crosswalk": "wnba/crosswalk/wnba_team_crosswalk_in_data_repo.csv",
    "schedule_crosswalk": "wnba/crosswalk/wnba_schedule_crosswalk_in_data_repo.csv",
    "player_crosswalk": "wnba/crosswalk/wnba_player_crosswalk_in_data_repo.csv",
}


@lru_cache(maxsize=1)
def _descriptions() -> dict[str, str]:
    """Column name -> description, merged across every schema in the store.

    A column named ``game_id`` means the same thing in every dataset, so the
    store is flattened by name.
    """
    path = Path(__file__).with_name("column_descriptions.yaml")
    if not path.exists():
        return {}
    import yaml

    store = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return {
        name: text.strip() for name, text in store.items() if isinstance(text, str) and text.strip()
    }


def _manifest(dataset: str) -> pl.DataFrame | None:
    rel = _MANIFEST_OVERRIDES.get(
        dataset, f"{LEAGUE}/{dataset}/{LEAGUE}_{dataset}_in_data_repo.csv"
    )
    path = REPO_ROOT / rel
    if not path.exists():
        return None
    try:
        frame = pl.read_csv(path)
    except Exception:
        return None
    # The schedules dir carries a games list, not a build manifest; anything
    # without the manifest columns renders as "no manifest" rather than crashing.
    if not {"season", "row_count"}.issubset(frame.columns):
        return None
    return frame


def release_status(tag: str, *, live: bool) -> dict[str, str]:
    """Last-published info for a release tag. Empty when offline or missing."""
    if not live:
        return {}
    try:
        out = subprocess.run(
            ["gh", "release", "view", tag, "--repo", RELEASE_REPO, "--json", "publishedAt,assets"],
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
        if out.returncode != 0:
            return {}
        data = json.loads(out.stdout)
        assets = data.get("assets") or []
        # `publishedAt` is when the TAG was created, which for a rolling
        # release is years stale. The newest asset timestamp is the honest
        # "last published".
        updated = max((a.get("updatedAt") or "" for a in assets), default="")
        return {
            "published": updated[:10],
            "created": (data.get("publishedAt") or "")[:10],
            "assets": str(len(assets)),
        }
    except Exception:
        return {}


def column_table(dataset: str) -> str:
    """The ``col_name | type | description`` table for one dataset."""
    if dataset not in MODELS:
        return "_No schema model yet._\n"
    descriptions = _descriptions()
    lines = ["| col_name | type | description |", "|---|---|---|"]
    for name, dtype in polars_schema(dataset).items():
        lines.append(f"| `{name}` | {dtype} | {descriptions.get(name, '')} |")
    return "\n".join(lines) + "\n"


def coverage_table(dataset: str) -> str:
    manifest = _manifest(dataset)
    if manifest is None or manifest.is_empty():
        return "_No build manifest yet._\n"
    lines = ["| season | rows | built (UTC) |", "|---:|---:|---|"]
    for row in manifest.sort("season").to_dicts():
        lines.append(
            f"| {row.get('season')} | {row.get('row_count'):,} | {row.get('generated_at_utc', '')} |"
        )
    return "\n".join(lines) + "\n"


def _seasons_built(manifest: pl.DataFrame | None) -> str:
    """Human summary of which seasons a dataset has been built for.

    Counts DISTINCT seasons, not manifest rows: some of these files are
    append-logs written one row per run rather than per season. A sparse range
    is labelled non-contiguous rather than implying completeness.
    """
    if manifest is None or manifest.is_empty() or "season" not in manifest.columns:
        return ""
    seasons = manifest["season"].drop_nulls().unique().sort()
    count = seasons.len()
    if count == 0:
        return ""
    low, high = seasons.min(), seasons.max()
    noun = "season" if count == 1 else "seasons"
    if count == 1:
        return f"{low} (1 season)"
    contiguous = count == (int(high) - int(low) + 1)
    span = f"{low}–{high}"
    return f"{span} ({count} {noun})" if contiguous else f"{span} ({count} {noun}, non-contiguous)"


def dataset_page(dataset: str, *, live: bool) -> str:
    spec = REGISTRY[dataset]
    status = release_status(spec.tag, live=live)
    manifest = _manifest(dataset)
    seasons = _seasons_built(manifest)

    return f"""# `{dataset}`

| | |
|---|---|
| **Builder** | [`{BUILDER[dataset]}`]({"../../" + BUILDER[dataset]}) |
| **Release tag** | [`{spec.tag}`]({RELEASE_URL}/{spec.tag}) |
| **File stem** | `{spec.stem}_{{season}}.{{parquet,csv,rds}}` |
| **Seasons built** | {seasons or "—"} |
| **Last published** | {status.get("published") or "—"} (newest release asset) |
| **Tag created** | {status.get("created") or "—"} |
| **Release assets** | {status.get("assets") or "—"} |

## Automation

{AUTOMATION}

## Columns

{column_table(dataset)}
## Coverage

{coverage_table(dataset)}"""


def summary_table(*, live: bool) -> str:
    """The block embedded in README.md and CLAUDE.md."""

    # Sort by the script NUMBER, not the path -- the stage number is the
    # ordering, and a path sort would not give it.
    def _order(dataset: str) -> int:
        stem_parts = Path(BUILDER[dataset]).stem.split("_")
        for part in stem_parts:
            if part.isdigit():
                return int(part)
        return 99

    numbered = sorted(REGISTRY, key=_order)
    lines = [
        "| Script | Dataset | Release tag | Last published |",
        "|---|---|---|---|",
    ]
    for dataset in numbered:
        spec = REGISTRY[dataset]
        status = release_status(spec.tag, live=live)
        lines.append(
            f"| [`{BUILDER[dataset]}`]({BUILDER[dataset]}) "
            f"| [`{dataset}`](docs/datasets/{dataset}.md) "
            f"| [`{spec.tag}`]({RELEASE_URL}/{spec.tag}) "
            f"| {status.get('published', '—')} |"
        )
    return "\n".join(lines)


#: Lines whose values move on every publish, so the drift gate ignores them.
_VOLATILE = ("**Last published**", "**Tag created**", "**Release assets**", "**Seasons built**")


def _without_status(text: str) -> str:
    """Strip publish-status + payload-derived content so the drift gate
    compares structure only.

    Two kinds of noise, both keyed on data this repo doesn't commit to the
    generated files themselves: `_VOLATILE` lines move on every daily publish
    (release status), and the `## Coverage` section (always the last section
    of a dataset page) is read from the `wnba/<dataset>/*_in_data_repo.csv`
    manifests under the large payload trees CI's sparse checkout excludes --
    comparing it would redden every PR for a reason no PR caused, which is
    exactly the bug that broke the WBB pilot's CI.
    """
    kept: list[str] = []
    for line in text.splitlines():
        if line.strip() == "## Coverage":
            break  # coverage table is payload-derived and always the last section
        if any(marker in line for marker in _VOLATILE):
            continue
        # The summary table's trailing "| <date> |" column moves too; drop it.
        if line.startswith("| [`") and line.count("|") >= 5:
            line = "|".join(line.split("|")[:-2]) + "|"
        kept.append(line)
    return "\n".join(kept)


def _replace_block(text: str, block: str) -> str:
    if BEGIN not in text or END not in text:
        return text.rstrip() + f"\n\n## Datasets\n\n{BEGIN}\n{block}\n{END}\n"
    head, _, rest = text.partition(BEGIN)
    _, _, tail = rest.partition(END)
    return f"{head}{BEGIN}\n{block}\n{END}{tail}"


def generate(*, check: bool = False, live: bool = True) -> int:
    """Write (or verify) every generated doc. Returns 0 when in sync."""
    stale: list[str] = []
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    wanted: dict[Path, str] = {DOCS_DIR / f"{d}.md": dataset_page(d, live=live) for d in REGISTRY}
    block = summary_table(live=live)
    for name in ("README.md", "CLAUDE.md"):
        path = REPO_ROOT / name
        if path.exists():
            wanted[path] = _replace_block(path.read_text(encoding="utf-8"), block)

    for path, content in wanted.items():
        current = path.read_text(encoding="utf-8") if path.exists() else None
        if current == content:
            continue
        if check:
            # Compare everything EXCEPT the status block. Those values move
            # whenever a daily publish runs, so comparing them would turn every
            # PR red for a reason no PR caused.
            if current is not None and _without_status(current) == _without_status(content):
                continue
            stale.append(str(path.relative_to(REPO_ROOT)))
        else:
            path.write_text(content, encoding="utf-8", newline="")

    if check and stale:
        print("::error ::generated docs are stale; run `uv run python -m wnba_data_build.docs`")
        for item in stale:
            print(f"  {item}")
        return 1
    if not check:
        print(f"wrote {len(wanted)} generated file(s)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate per-dataset documentation.")
    parser.add_argument("--check", action="store_true", help="Fail if anything is stale")
    parser.add_argument(
        "--no-live",
        action="store_true",
        help="Skip `gh release view` (offline; status columns render as em dashes)",
    )
    args = parser.parse_args(argv)
    return generate(check=args.check, live=not args.no_live)


if __name__ == "__main__":
    raise SystemExit(main())
