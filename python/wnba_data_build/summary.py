"""Run summary: what a build published and what still went wrong.

Python port of ``R/run_summary.R`` (kept in-repo as the rollback path). Parses the per-season tracked logs the
daily processor writes and emits (a) a summary to stdout, visible in the
Actions log, and (b) a markdown summary to ``$GITHUB_STEP_SUMMARY`` when set,
so the run's Summary tab shows which releases updated plus any remaining
warnings and errors.

The log format is the contract, not an implementation detail: the R producer
emitted ``Updated YYYY ESPN LEAGUE <ds> GitHub Release`` and the Python
producer emits ``uploaded <file> -> <tag> (asset N/M)``. Both are recognised,
because the rollback path still writes the former -- dropping it would silently
report 0 updated on any R run.
"""

from __future__ import annotations

import argparse
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any

LOG_GLOB = "*_data_logfile_*.log"

_R_UPLOAD = re.compile(r"^Updated \d{4} ESPN [A-Z]+ (.+?) GitHub Release$")
_PY_UPLOAD = re.compile(r"uploaded \S+ -> (\S+) \(asset \d")
_SKIP = re.compile(r"skip ([a-z_]+)")
_HTTP_ERR = re.compile(r"40[0-9]")

_WARNING_PATTERNS = (
    re.compile(r"^!\s"),
    re.compile(r"cannot open URL.*40[0-9]"),
    re.compile(r"40[0-9] Not Found"),
    re.compile(r"no .* (rows|games|athletes) ", re.IGNORECASE),
    re.compile(r"^::warning"),
)
_ERROR_PATTERNS = (
    re.compile(r"Execution halted"),
    re.compile(r"^Error[: ]"),
    re.compile(r"^(✖|x )"),
    re.compile(r"^::error"),
)


def updated_tags(lines: list[str]) -> list[str]:
    """Release tags/datasets this run published, from either producer's format."""
    found: set[str] = set()
    for line in lines:
        line = line.strip()
        m = _R_UPLOAD.match(line)
        if m:
            found.add(m.group(1))
        m = _PY_UPLOAD.search(line)
        if m:
            found.add(m.group(1))
    return sorted(found)


def extract_warnings(lines: list[str]) -> list[str]:
    return [line.strip() for line in lines if any(p.search(line) for p in _WARNING_PATTERNS)]


def extract_errors(lines: list[str]) -> list[str]:
    return [line.strip() for line in lines if any(p.search(line) for p in _ERROR_PATTERNS)]


def summarize_warnings(warnings: list[str]) -> list[str]:
    """Collapse the per-game skip/404 noise into one line per dataset."""
    if not warnings:
        return []
    out: list[str] = []
    skips = Counter(m.group(1) for w in warnings if (m := _SKIP.search(w)))
    for dataset, n in sorted(skips.items()):
        out.append(f"{n} game(s) skipped in {dataset} (missing/404 raw JSON)")
    n_404 = sum(1 for w in warnings if _HTTP_ERR.search(w))
    if n_404:
        out.append(f"{n_404} HTTP 4xx warning(s)")
    return out


def summarize_run(logfiles: list[Path]) -> dict[str, Any]:
    """Collect updated tags, warnings, errors and per-season exit codes."""
    updated: set[str] = set()
    warnings: list[str] = []
    errors: list[str] = []
    seasons: dict[int, int] = {}
    exit_re = re.compile(r"season (\d{4}) EXIT=(\d+)")

    for path in logfiles:
        path = Path(path)
        if not path.exists():
            continue
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        updated.update(updated_tags(lines))
        warnings.extend(extract_warnings(lines))
        errors.extend(extract_errors(lines))
        for line in lines:
            m = exit_re.search(line)
            if m:
                seasons[int(m.group(1))] = int(m.group(2))
    return {
        "updated": sorted(updated),
        "warnings": warnings,
        "errors": errors,
        "seasons": seasons,
    }


def render(result: dict[str, Any]) -> str:
    """Markdown summary for $GITHUB_STEP_SUMMARY."""
    lines = ["## WNBA data run summary", ""]
    if result["seasons"]:
        lines.append("| season | exit |")
        lines.append("|---:|---:|")
        for season, code in sorted(result["seasons"].items()):
            lines.append(f"| {season} | {code} |")
        lines.append("")
    lines.append(f"**{len(result['updated'])} release tag(s) updated**")
    for tag in result["updated"]:
        lines.append(f"- `{tag}`")
    rolled = summarize_warnings(result["warnings"])
    if rolled:
        lines += ["", f"**{len(result['warnings'])} warning(s)**"] + [f"- {r}" for r in rolled]
    if result["errors"]:
        lines += ["", f"**{len(result['errors'])} error(s)**"]
        lines += [f"- `{e}`" for e in result["errors"][:20]]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize a WNBA data build run.")
    parser.add_argument("-s", "--start", type=int, default=None)
    parser.add_argument("-e", "--end", type=int, default=None)
    parser.add_argument("--logs", default="logs")
    args = parser.parse_args(argv)

    logs_dir = Path(args.logs)
    if args.start is not None:
        end = args.end if args.end is not None else args.start
        paths = [
            p
            for p in sorted(logs_dir.glob(LOG_GLOB))
            if _season_of(p) in range(args.start, end + 1)
        ]
    else:
        paths = sorted(logs_dir.glob(LOG_GLOB))

    if not paths:
        print(f"No data logs found in {logs_dir}/; nothing to summarize.")
        return 0

    result = summarize_run(paths)
    for season, code in sorted(result["seasons"].items()):
        print(f"season {season}: EXIT={code}")
    print(f"{len(result['updated'])} release tag(s) updated")
    for tag in result["updated"]:
        print(f"  {tag}")
    for rolled in summarize_warnings(result["warnings"]):
        print(f"WARNING: {rolled}")
    for err in result["errors"]:
        print(f"ERROR: {err}")

    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as fh:
            fh.write(render(result))
    return 0


def _season_of(path: Path) -> int:
    m = re.search(r"_(\d{4})\.log$", path.name)
    return int(m.group(1)) if m else -1


if __name__ == "__main__":
    raise SystemExit(main())
