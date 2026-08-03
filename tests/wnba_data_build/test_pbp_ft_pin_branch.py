"""Coverage for the R Free-Throw coordinate-pin branch (real payload).

R (``wehoop/R/espn_wnba_data.R:2247-2257``) pins any play whose ``type.text``
matches ``"Free Throw"`` to raw coordinates (x=25, y=13.75) BEFORE the
home-flip transform. Unlike WBB — where the branch is dead code, because no
released season ships that vocabulary — the WNBA feed uses it in production:
every free throw arrives labelled ``"Free Throw - 1 of 2"`` (etc.) carrying
ESPN's int32 sentinel coordinates (-214748340 / -214748365), and the pin is
what rescues them. The 2025 release carries 11,332 pinned rows. So this test
asserts the branch against an untouched real payload (401856890) — no
relabeling, no synthesis.

Expected transform after the pin (R lines 2258-2265):
  home team: coordinate_x = -(13.75 - 41.75) = 28.0, coordinate_y = -(25 - 25) = 0.0
  away team: coordinate_x =  (13.75 - 41.75) = -28.0, coordinate_y = 0.0
"""

import json
from pathlib import Path

import polars as pl

FX = Path(__file__).parent.parent / "fixtures"

GAME_ID = 401856890


def test_ft_pin_and_flip_on_real_payload():
    from sportsdataverse.wnba import helper_wnba_play_by_play

    final = json.loads(
        (FX / "raw" / "wnba" / "json" / "final" / f"{GAME_ID}.json").read_text(encoding="utf-8")
    )
    competitors = final["header"]["competitions"][0]["competitors"]
    home_id = next(c["id"] for c in competitors if c["homeAway"] == "home")

    ft_plays = [p for p in final["plays"] if "Free Throw" in str(p.get("type.text", ""))]
    # The branch is only meaningful if the feed really ships this vocabulary
    # AND ships it with unusable coordinates.
    assert ft_plays, "fixture must contain live 'Free Throw' plays"
    assert {p.get("coordinate.x") for p in ft_plays} == {-214748340}
    home_ft = [p for p in ft_plays if p.get("team.id") == home_id]
    away_ft = [p for p in ft_plays if p.get("team.id") != home_id]
    assert home_ft and away_ft, "need free throws on both sides to test the flip"

    df = helper_wnba_play_by_play(final)

    def _row(play):
        row = df.filter(pl.col("game_play_number") == play["game_play_number"])
        assert row.height == 1
        return row

    for play in ft_plays:
        row = _row(play)
        assert row.get_column("coordinate_x_raw")[0] == 25.0
        assert row.get_column("coordinate_y_raw")[0] == 13.75

    for play in home_ft:
        row = _row(play)
        assert row.get_column("coordinate_x")[0] == 28.0
        assert row.get_column("coordinate_y")[0] == 0.0

    for play in away_ft:
        row = _row(play)
        assert row.get_column("coordinate_x")[0] == -28.0
        assert row.get_column("coordinate_y")[0] == 0.0

    # And the pin didn't leak: an untouched play keeps its exact source
    # coordinates. (Can't use x==25 as the tell -- ESPN's feed natively
    # centers some shot x at 25.)
    witness = next(
        p
        for p in final["plays"]
        if "Free Throw" not in str(p.get("type.text", ""))
        and p.get("coordinate.x") is not None
        and p.get("coordinate.x") != 25
        and p.get("coordinate.x") != -214748340
    )
    row = _row(witness)
    assert row.get_column("coordinate_x_raw")[0] == float(witness["coordinate.x"])
    assert row.get_column("coordinate_y_raw")[0] == float(witness["coordinate.y"])
