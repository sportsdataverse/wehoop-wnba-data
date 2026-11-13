"""Generated dataset pages: the "Load it" and "Join keys" sections."""

from __future__ import annotations

from wnba_data_build import docs


def test_every_page_names_both_loaders():
    for dataset in docs.REGISTRY:
        page = docs.dataset_page(dataset, live=False)
        r_loader, py_loader = docs.LOADERS[dataset]
        assert f"`{r_loader}`" in page and f"`{py_loader}`" in page


def test_join_keys_follow_the_schema():
    pbp = docs.join_table("pbp")
    assert "| `game_id` |" in pbp and "| `team_id` |" in pbp
    assert "| `type_id` |" not in pbp  # not a join key
    # provider-prefixed crosswalk ids match by prefix
    assert "| `wnba_game_id` | WNBA Stats" in docs.join_table("schedule_crosswalk")
