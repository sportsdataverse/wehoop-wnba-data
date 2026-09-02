"""The release sidecars R's sportsdataverse_save() attaches to every tag.

Dropping them is what left published tags carrying a timestamp.json frozen at
the last R run while the data kept moving -- a consumer reading it to decide
whether to re-download got a confident wrong answer.
"""

import json
from pathlib import Path

from wnba_data_build import publish
from wnba_data_build.config import PKG_FUNCTION, REGISTRY

SIDECAR_NAMES = [
    "timestamp.txt",
    "timestamp.json",
    "package_function.txt",
    "package_function.json",
]


def test_stamp_uploads_the_four_sidecars_with_clobber():
    calls: list[list[str]] = []

    publish._stamp("espn_wnba_draft", calls.append, "sportsdataverse/sportsdataverse-data")

    assert [Path(c[3]).name for c in calls] == SIDECAR_NAMES
    assert all(c[:3] == ["release", "upload", "espn_wnba_draft"] for c in calls)
    assert all(c[-1] == "--clobber" for c in calls)
    # written to a temp dir that is cleaned up behind the upload
    assert not any(Path(c[3]).exists() for c in calls)


def test_stamp_names_the_loader_for_the_tag():
    """The package_function pair carries the tag's loader, both formats."""
    seen: dict[str, str] = {}

    def _runner(argv: list[str]) -> None:
        # read inside the runner: the temp dir is cleaned up behind the upload
        path = Path(argv[3])
        seen[path.name] = path.read_text()

    publish._stamp("espn_wnba_draft", _runner, "sportsdataverse/sportsdataverse-data")

    expected = PKG_FUNCTION["espn_wnba_draft"]
    assert seen["package_function.txt"].strip() == expected
    assert json.loads(seen["package_function.json"])["package_function"] == expected
    assert json.loads(seen["timestamp.json"])["last_updated"].strip()


def test_every_registry_tag_has_a_package_function():
    """A new dataset must not publish a tag with no loader named on it."""
    missing = sorted({s.tag for s in REGISTRY.values()} - set(PKG_FUNCTION))
    assert missing == [], f"tags with no PKG_FUNCTION entry: {missing}"
