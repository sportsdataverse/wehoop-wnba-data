import pytest

from wnba_data_build.cli import main


def test_help_exits_zero():
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0


def test_unknown_dataset_errors():
    with pytest.raises(SystemExit) as exc:
        main(["--dataset", "not_a_dataset", "-s", "2025", "-e", "2025"])
    assert exc.value.code == 2  # argparse invalid choice


def test_publish_and_dry_run_mutually_exclusive():
    with pytest.raises(SystemExit) as exc:
        main(["--dataset", "team_box", "-s", "2025", "-e", "2025", "--publish", "--dry-run"])
    assert exc.value.code == 2
