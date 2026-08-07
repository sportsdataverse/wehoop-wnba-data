"""Typed schema declarations for every released WNBA dataset.

These declare the SCHEMA, not the rows. ``polars_schema()`` converts a model to
a ``pl.Schema`` that callers can assert frame-level; row-level pydantic
validation over a full-season pbp frame is a performance trap and is not the
build path.

Generated from the real released parquets (the committed release-parity
fixtures plus the published ``player_core`` / ``wnba_crosswalk`` assets), then
kept by hand. Every ``*_id`` field is ``int`` because ids are canonicalized to
Int64 at the write boundary; check_frame tolerates the Int32 the historical
assets carry (losslessly widenable), never a type change.

Strict mode is deliberate: without it pydantic coerces "401736112" to int and
5 to 5.0, which is exactly the id-dtype bug class this pipeline guards against.
"""

from __future__ import annotations

# Aliased: released columns may legitimately be NAMED `date`/`datetime`, and a
# field named `date` shadows the import inside the class namespace, silently
# turning every later `Optional[date]` annotation into NoneType.
from datetime import date as _date
from datetime import datetime as _datetime
from typing import Optional

import polars as pl
from pydantic import BaseModel, ConfigDict

_PL_TYPES: dict[type, pl.DataType] = {
    str: pl.Utf8,
    int: pl.Int64,
    float: pl.Float64,
    bool: pl.Boolean,
    _date: pl.Date,
    _datetime: pl.Datetime(time_unit="us", time_zone="America/New_York"),
}


class WnbaDataset(BaseModel):
    """Base: strict types, unknown fields ignored."""

    model_config = ConfigDict(strict=True, extra="ignore")


class Pbp(WnbaDataset):
    """Released as ``espn_wnba_pbp`` / ``play_by_play_{season}``."""

    game_play_number: Optional[int] = None
    id: Optional[float] = None
    sequence_number: Optional[int] = None
    type_id: Optional[int] = None
    type_text: Optional[str] = None
    text: Optional[str] = None
    away_score: Optional[int] = None
    home_score: Optional[int] = None
    period_number: Optional[int] = None
    period_display_value: Optional[str] = None
    clock_display_value: Optional[str] = None
    scoring_play: Optional[bool] = None
    score_value: Optional[int] = None
    team_id: Optional[int] = None
    athlete_id_1: Optional[int] = None
    athlete_id_2: Optional[int] = None
    athlete_id_3: Optional[int] = None
    wallclock: Optional[str] = None
    shooting_play: Optional[bool] = None
    coordinate_x_raw: Optional[float] = None
    coordinate_y_raw: Optional[float] = None
    points_attempted: Optional[int] = None
    short_description: Optional[str] = None
    game_id: Optional[int] = None
    season: Optional[int] = None
    season_type: Optional[int] = None
    home_team_id: Optional[int] = None
    home_team_name: Optional[str] = None
    home_team_mascot: Optional[str] = None
    home_team_abbrev: Optional[str] = None
    home_team_name_alt: Optional[str] = None
    away_team_id: Optional[int] = None
    away_team_name: Optional[str] = None
    away_team_mascot: Optional[str] = None
    away_team_abbrev: Optional[str] = None
    away_team_name_alt: Optional[str] = None
    game_spread: Optional[float] = None
    home_favorite: Optional[bool] = None
    game_spread_available: Optional[bool] = None
    home_team_spread: Optional[float] = None
    qtr: Optional[int] = None
    time: Optional[str] = None
    clock_minutes: Optional[int] = None
    clock_seconds: Optional[float] = None
    home_timeout_called: Optional[bool] = None
    away_timeout_called: Optional[bool] = None
    half: Optional[int] = None
    game_half: Optional[int] = None
    lead_qtr: Optional[int] = None
    lead_half: Optional[int] = None
    start_quarter_seconds_remaining: Optional[float] = None
    start_half_seconds_remaining: Optional[float] = None
    start_game_seconds_remaining: Optional[float] = None
    end_quarter_seconds_remaining: Optional[float] = None
    end_half_seconds_remaining: Optional[float] = None
    end_game_seconds_remaining: Optional[float] = None
    period: Optional[int] = None
    lag_qtr: Optional[int] = None
    lag_half: Optional[int] = None
    coordinate_x: Optional[float] = None
    coordinate_y: Optional[float] = None
    game_date: Optional[_date] = None
    game_date_time: Optional[_datetime] = None
    type_abbreviation: Optional[str] = None


class Schedules(WnbaDataset):
    """Released as ``espn_wnba_schedules`` / ``wnba_schedule_{season}``."""

    id: Optional[int] = None
    uid: Optional[str] = None
    date: Optional[str] = None
    attendance: Optional[float] = None
    time_valid: Optional[bool] = None
    neutral_site: Optional[bool] = None
    conference_competition: Optional[bool] = None
    play_by_play_available: Optional[bool] = None
    recent: Optional[bool] = None
    start_date: Optional[str] = None
    broadcast: Optional[str] = None
    highlights: Optional[str] = None
    notes_type: Optional[str] = None
    notes_headline: Optional[str] = None
    broadcast_market: Optional[str] = None
    broadcast_name: Optional[str] = None
    type_id: Optional[int] = None
    type_abbreviation: Optional[str] = None
    venue_id: Optional[int] = None
    venue_full_name: Optional[str] = None
    venue_address_city: Optional[str] = None
    venue_address_state: Optional[str] = None
    venue_indoor: Optional[bool] = None
    status_clock: Optional[float] = None
    status_display_clock: Optional[str] = None
    status_period: Optional[float] = None
    status_type_id: Optional[int] = None
    status_type_name: Optional[str] = None
    status_type_state: Optional[str] = None
    status_type_completed: Optional[bool] = None
    status_type_description: Optional[str] = None
    status_type_detail: Optional[str] = None
    status_type_short_detail: Optional[str] = None
    format_regulation_periods: Optional[float] = None
    home_id: Optional[int] = None
    home_uid: Optional[str] = None
    home_location: Optional[str] = None
    home_name: Optional[str] = None
    home_abbreviation: Optional[str] = None
    home_display_name: Optional[str] = None
    home_short_display_name: Optional[str] = None
    home_color: Optional[str] = None
    home_alternate_color: Optional[str] = None
    home_is_active: Optional[bool] = None
    home_venue_id: Optional[int] = None
    home_logo: Optional[str] = None
    home_score: Optional[int] = None
    home_winner: Optional[bool] = None
    home_linescores: Optional[str] = None
    home_records: Optional[str] = None
    away_id: Optional[int] = None
    away_uid: Optional[str] = None
    away_location: Optional[str] = None
    away_name: Optional[str] = None
    away_abbreviation: Optional[str] = None
    away_display_name: Optional[str] = None
    away_short_display_name: Optional[str] = None
    away_color: Optional[str] = None
    away_alternate_color: Optional[str] = None
    away_is_active: Optional[bool] = None
    away_venue_id: Optional[int] = None
    away_logo: Optional[str] = None
    away_score: Optional[int] = None
    away_winner: Optional[bool] = None
    away_linescores: Optional[str] = None
    away_records: Optional[str] = None
    game_id: Optional[int] = None
    season: Optional[int] = None
    season_type: Optional[int] = None
    status_type_alt_detail: Optional[str] = None
    game_json: Optional[bool] = None
    game_json_url: Optional[str] = None
    game_date_time: Optional[_datetime] = None
    game_date: Optional[_date] = None
    PBP: Optional[bool] = None
    team_box: Optional[bool] = None
    player_box: Optional[bool] = None


class Shots(WnbaDataset):
    """Released as ``espn_wnba_shots`` / ``shots_{season}``."""

    game_id: Optional[int] = None
    season: Optional[int] = None
    period_number: Optional[int] = None
    clock_display_value: Optional[str] = None
    team_id: Optional[int] = None
    athlete_id_1: Optional[int] = None
    athlete_id_2: Optional[int] = None
    type_id: Optional[int] = None
    type_text: Optional[str] = None
    scoring_play: Optional[bool] = None
    score_value: Optional[int] = None
    coordinate_x: Optional[float] = None
    coordinate_y: Optional[float] = None
    coordinate_x_raw: Optional[float] = None
    coordinate_y_raw: Optional[float] = None


class TeamBox(WnbaDataset):
    """Released as ``espn_wnba_team_boxscores`` / ``team_box_{season}``."""

    game_id: Optional[int] = None
    season: Optional[int] = None
    season_type: Optional[int] = None
    game_date: Optional[_date] = None
    game_date_time: Optional[_datetime] = None
    team_id: Optional[int] = None
    team_uid: Optional[str] = None
    team_slug: Optional[str] = None
    team_location: Optional[str] = None
    team_name: Optional[str] = None
    team_abbreviation: Optional[str] = None
    team_display_name: Optional[str] = None
    team_short_display_name: Optional[str] = None
    team_color: Optional[str] = None
    team_alternate_color: Optional[str] = None
    team_logo: Optional[str] = None
    team_home_away: Optional[str] = None
    team_score: Optional[int] = None
    team_winner: Optional[bool] = None
    assists: Optional[int] = None
    blocks: Optional[int] = None
    defensive_rebounds: Optional[int] = None
    fast_break_points: Optional[str] = None
    field_goal_pct: Optional[float] = None
    field_goals_made: Optional[int] = None
    field_goals_attempted: Optional[int] = None
    flagrant_fouls: Optional[int] = None
    fouls: Optional[int] = None
    free_throw_pct: Optional[float] = None
    free_throws_made: Optional[int] = None
    free_throws_attempted: Optional[int] = None
    largest_lead: Optional[str] = None
    offensive_rebounds: Optional[int] = None
    points_in_paint: Optional[str] = None
    steals: Optional[int] = None
    team_turnovers: Optional[int] = None
    technical_fouls: Optional[int] = None
    three_point_field_goal_pct: Optional[float] = None
    three_point_field_goals_made: Optional[int] = None
    three_point_field_goals_attempted: Optional[int] = None
    total_rebounds: Optional[int] = None
    total_technical_fouls: Optional[int] = None
    total_turnovers: Optional[int] = None
    turnover_points: Optional[str] = None
    turnovers: Optional[int] = None
    opponent_team_id: Optional[int] = None
    opponent_team_uid: Optional[str] = None
    opponent_team_slug: Optional[str] = None
    opponent_team_location: Optional[str] = None
    opponent_team_name: Optional[str] = None
    opponent_team_abbreviation: Optional[str] = None
    opponent_team_display_name: Optional[str] = None
    opponent_team_short_display_name: Optional[str] = None
    opponent_team_color: Optional[str] = None
    opponent_team_alternate_color: Optional[str] = None
    opponent_team_logo: Optional[str] = None
    opponent_team_score: Optional[int] = None


class PlayerBox(WnbaDataset):
    """Released as ``espn_wnba_player_boxscores`` / ``player_box_{season}``."""

    game_id: Optional[int] = None
    season: Optional[int] = None
    season_type: Optional[int] = None
    game_date: Optional[_date] = None
    game_date_time: Optional[_datetime] = None
    athlete_id: Optional[int] = None
    athlete_display_name: Optional[str] = None
    team_id: Optional[int] = None
    team_name: Optional[str] = None
    team_location: Optional[str] = None
    team_short_display_name: Optional[str] = None
    minutes: Optional[float] = None
    field_goals_made: Optional[int] = None
    field_goals_attempted: Optional[int] = None
    three_point_field_goals_made: Optional[int] = None
    three_point_field_goals_attempted: Optional[int] = None
    free_throws_made: Optional[int] = None
    free_throws_attempted: Optional[int] = None
    offensive_rebounds: Optional[int] = None
    defensive_rebounds: Optional[int] = None
    rebounds: Optional[int] = None
    assists: Optional[int] = None
    steals: Optional[int] = None
    blocks: Optional[int] = None
    turnovers: Optional[int] = None
    fouls: Optional[int] = None
    plus_minus: Optional[str] = None
    points: Optional[int] = None
    starter: Optional[bool] = None
    ejected: Optional[bool] = None
    did_not_play: Optional[bool] = None
    reason: Optional[str] = None
    active: Optional[bool] = None
    athlete_jersey: Optional[str] = None
    athlete_short_name: Optional[str] = None
    athlete_headshot_href: Optional[str] = None
    athlete_position_name: Optional[str] = None
    athlete_position_abbreviation: Optional[str] = None
    team_display_name: Optional[str] = None
    team_uid: Optional[str] = None
    team_slug: Optional[str] = None
    team_logo: Optional[str] = None
    team_abbreviation: Optional[str] = None
    team_color: Optional[str] = None
    team_alternate_color: Optional[str] = None
    home_away: Optional[str] = None
    team_winner: Optional[bool] = None
    team_score: Optional[int] = None
    opponent_team_id: Optional[int] = None
    opponent_team_name: Optional[str] = None
    opponent_team_location: Optional[str] = None
    opponent_team_display_name: Optional[str] = None
    opponent_team_abbreviation: Optional[str] = None
    opponent_team_logo: Optional[str] = None
    opponent_team_color: Optional[str] = None
    opponent_team_alternate_color: Optional[str] = None
    opponent_team_score: Optional[int] = None


class Rosters(WnbaDataset):
    """Released as ``espn_wnba_rosters`` / ``rosters_{season}``."""

    season: Optional[int] = None
    team_id: Optional[int] = None
    team_slug: Optional[str] = None
    team_abbreviation: Optional[str] = None
    team_display_name: Optional[str] = None
    team_short_display_name: Optional[str] = None
    team_color: Optional[str] = None
    team_alternate_color: Optional[str] = None
    team_logo: Optional[str] = None
    athlete_id: Optional[str] = None
    uid: Optional[str] = None
    guid: Optional[str] = None
    full_name: Optional[str] = None
    display_name: Optional[str] = None
    short_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    jersey: Optional[str] = None
    position_abbreviation: Optional[str] = None
    position_name: Optional[str] = None
    position_id: Optional[str] = None
    height: Optional[str] = None
    weight: Optional[str] = None
    age: Optional[str] = None
    date_of_birth: Optional[str] = None
    birth_place_city: Optional[str] = None
    birth_place_state: Optional[str] = None
    birth_place_country: Optional[str] = None
    experience_years: Optional[str] = None
    experience_display_value: Optional[str] = None
    headshot_href: Optional[str] = None
    headshot_alt: Optional[str] = None
    link_web: Optional[str] = None
    status_id: Optional[str] = None
    status_name: Optional[str] = None
    status_type: Optional[str] = None


class PlayerSeasonStats(WnbaDataset):
    """Released as ``espn_wnba_player_season_stats`` / ``player_season_stats_{season}``."""

    season: Optional[int] = None
    athlete_id: Optional[int] = None
    athlete_display_name: Optional[str] = None
    athlete_first_name: Optional[str] = None
    athlete_last_name: Optional[str] = None
    athlete_position_abbreviation: Optional[str] = None
    athlete_jersey: Optional[str] = None
    team_id: Optional[int] = None
    team_display_name: Optional[str] = None
    category: Optional[str] = None
    stat_label: Optional[str] = None
    stat_name: Optional[str] = None
    stat_display_name: Optional[str] = None
    stat_description: Optional[str] = None
    display_value: Optional[str] = None
    value: Optional[float] = None


class PlayerCore(WnbaDataset):
    """Released as ``espn_wnba_player_core`` / ``player_core_{season}``."""

    season: Optional[int] = None
    athlete_id: Optional[int] = None
    guid: Optional[str] = None
    uid: Optional[str] = None
    slug: Optional[str] = None
    type: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    display_name: Optional[str] = None
    short_name: Optional[str] = None
    height: Optional[float] = None
    display_height: Optional[str] = None
    weight: Optional[float] = None
    display_weight: Optional[str] = None
    age: Optional[int] = None
    date_of_birth: Optional[str] = None
    birth_city: Optional[str] = None
    birth_state: Optional[str] = None
    birth_country: Optional[str] = None
    jersey: Optional[str] = None
    position_id: Optional[int] = None
    position_name: Optional[str] = None
    position_abbreviation: Optional[str] = None
    position_display_name: Optional[str] = None
    college_id: Optional[int] = None
    current_team_id: Optional[int] = None
    headshot_href: Optional[str] = None
    experience_years: Optional[int] = None
    status_id: Optional[int] = None
    status_name: Optional[str] = None
    status_type: Optional[str] = None
    draft_year: Optional[int] = None
    draft_round: Optional[int] = None
    draft_selection: Optional[int] = None
    active: Optional[bool] = None


class TeamSeasonStats(WnbaDataset):
    """Released as ``espn_wnba_team_season_stats`` / ``team_season_stats_{season}``."""

    season: Optional[int] = None
    team_id: Optional[int] = None
    team_slug: Optional[str] = None
    team_abbreviation: Optional[str] = None
    team_display_name: Optional[str] = None
    team_short_display_name: Optional[str] = None
    team_color: Optional[str] = None
    team_alternate_color: Optional[str] = None
    team_logo: Optional[str] = None
    category: Optional[str] = None
    stat_label: Optional[str] = None
    stat_name: Optional[str] = None
    stat_display_name: Optional[str] = None
    stat_description: Optional[str] = None
    display_value: Optional[str] = None
    value: Optional[float] = None


class Standings(WnbaDataset):
    """Released as ``espn_wnba_standings`` / ``standings_{season}``."""

    season: Optional[int] = None
    group_id: Optional[str] = None
    group_name: Optional[str] = None
    group_abbreviation: Optional[str] = None
    group_short_name: Optional[str] = None
    team_id: Optional[int] = None
    team_uid: Optional[str] = None
    team_slug: Optional[str] = None
    team_location: Optional[str] = None
    team_name: Optional[str] = None
    team_abbreviation: Optional[str] = None
    team_display_name: Optional[str] = None
    team_short_display_name: Optional[str] = None
    team_color: Optional[str] = None
    team_alternate_color: Optional[str] = None
    team_logo: Optional[str] = None
    stat_name: Optional[str] = None
    stat_display_name: Optional[str] = None
    stat_short_display_name: Optional[str] = None
    stat_description: Optional[str] = None
    stat_abbreviation: Optional[str] = None
    stat_type: Optional[str] = None
    display_value: Optional[str] = None
    value: Optional[float] = None


class GameRosters(WnbaDataset):
    """Released as ``espn_wnba_game_rosters`` / ``game_rosters_{season}``."""

    season: Optional[int] = None
    game_id: Optional[str] = None
    team_id: Optional[int] = None
    team_slug: Optional[str] = None
    team_abbreviation: Optional[str] = None
    team_display_name: Optional[str] = None
    home_away: Optional[str] = None
    athlete_id: Optional[int] = None
    athlete_uid: Optional[str] = None
    athlete_guid: Optional[str] = None
    athlete_display_name: Optional[str] = None
    athlete_short_name: Optional[str] = None
    athlete_first_name: Optional[str] = None
    athlete_last_name: Optional[str] = None
    athlete_jersey: Optional[str] = None
    athlete_position: Optional[str] = None
    athlete_headshot: Optional[str] = None
    starter: Optional[bool] = None
    did_not_play: Optional[bool] = None
    active: Optional[bool] = None
    ejected: Optional[bool] = None
    reason: Optional[str] = None


class Officials(WnbaDataset):
    """Released as ``espn_wnba_officials`` / ``officials_{season}``."""

    season: Optional[int] = None
    game_id: Optional[str] = None
    official_id: Optional[int] = None
    official_uid: Optional[str] = None
    official_full_name: Optional[str] = None
    official_display_name: Optional[str] = None
    official_first_name: Optional[str] = None
    official_last_name: Optional[str] = None
    official_order: Optional[int] = None
    position_name: Optional[str] = None
    position_display_name: Optional[str] = None


class Draft(WnbaDataset):
    """Released as ``espn_wnba_draft`` / ``draft_{season}``."""

    season: Optional[int] = None
    round: Optional[int] = None
    round_display_name: Optional[str] = None
    pick: Optional[int] = None
    overall_pick: Optional[int] = None
    pick_traded: Optional[str] = None
    pick_notes: Optional[str] = None
    athlete_id: Optional[int] = None
    athlete_uid: Optional[str] = None
    athlete_guid: Optional[str] = None
    athlete_first_name: Optional[str] = None
    athlete_last_name: Optional[str] = None
    athlete_full_name: Optional[str] = None
    athlete_display_name: Optional[str] = None
    athlete_short_name: Optional[str] = None
    athlete_height: Optional[str] = None
    athlete_weight: Optional[str] = None
    athlete_position_abbreviation: Optional[str] = None
    athlete_position_name: Optional[str] = None
    athlete_headshot_href: Optional[str] = None
    college_id: Optional[int] = None
    college_name: Optional[str] = None
    college_short_name: Optional[str] = None
    college_abbreviation: Optional[str] = None
    team_id: Optional[int] = None
    team_uid: Optional[str] = None
    team_slug: Optional[str] = None
    team_location: Optional[str] = None
    team_name: Optional[str] = None
    team_abbreviation: Optional[str] = None
    team_display_name: Optional[str] = None
    team_short_display_name: Optional[str] = None
    team_color: Optional[str] = None
    team_alternate_color: Optional[str] = None
    team_logo: Optional[str] = None


class TeamCrosswalk(WnbaDataset):
    """Released as ``wnba_crosswalk`` / ``wnba_team_crosswalk_{season}``."""

    season: Optional[int] = None
    espn_team_id: Optional[int] = None
    espn_abbreviation: Optional[str] = None
    espn_display_name: Optional[str] = None
    espn_short_name: Optional[str] = None
    espn_location: Optional[str] = None
    espn_mascot: Optional[str] = None
    wnba_team_id: Optional[str] = None
    wnba_team_tricode: Optional[str] = None
    wnba_team_name: Optional[str] = None
    wnba_team_city: Optional[str] = None
    wnba_team_slug: Optional[str] = None
    fox_team_id: Optional[str] = None
    fox_team_name: Optional[str] = None
    yahoo_team_id: Optional[str] = None
    yahoo_team_abbreviation: Optional[str] = None
    yahoo_team_name: Optional[str] = None
    match_method: Optional[str] = None
    match_confidence: Optional[float] = None


class ScheduleCrosswalk(WnbaDataset):
    """Released as ``wnba_crosswalk`` / ``wnba_schedule_crosswalk_{season}``."""

    season: Optional[int] = None
    season_type: Optional[str] = None
    game_date: Optional[_date] = None
    home_espn_team_id: Optional[int] = None
    away_espn_team_id: Optional[int] = None
    espn_game_id: Optional[str] = None
    wnba_game_id: Optional[str] = None
    wnba_game_code: Optional[str] = None
    wnba_home_team_id: Optional[str] = None
    wnba_away_team_id: Optional[str] = None
    fox_game_id: Optional[str] = None
    fox_home_team_id: Optional[str] = None
    fox_away_team_id: Optional[str] = None
    yahoo_game_id: Optional[str] = None
    match_method: Optional[str] = None
    match_confidence: Optional[float] = None


class PlayerCrosswalk(WnbaDataset):
    """Released as ``wnba_crosswalk`` / ``wnba_player_crosswalk_{season}``."""

    season: Optional[int] = None
    espn_team_id: Optional[int] = None
    team_abbreviation: Optional[str] = None
    player_name: Optional[str] = None
    espn_athlete_id: Optional[str] = None
    espn_full_name: Optional[str] = None
    espn_jersey: Optional[str] = None
    espn_position: Optional[str] = None
    wnba_player_id: Optional[str] = None
    wnba_player_name: Optional[str] = None
    wnba_jersey_num: Optional[str] = None
    wnba_position: Optional[str] = None
    fox_athlete_id: Optional[str] = None
    fox_player: Optional[str] = None
    fox_jersey: Optional[str] = None
    fox_position_group: Optional[str] = None
    yahoo_player_id: Optional[str] = None
    yahoo_player_name: Optional[str] = None
    match_method: Optional[str] = None
    match_confidence: Optional[float] = None
    match_keys: Optional[str] = None


#: dataset key (``config.REGISTRY``) -> model.
MODELS: dict[str, type[WnbaDataset]] = {
    "pbp": Pbp,
    "schedules": Schedules,
    "shots": Shots,
    "team_box": TeamBox,
    "player_box": PlayerBox,
    "rosters": Rosters,
    "player_season_stats": PlayerSeasonStats,
    "player_core": PlayerCore,
    "team_season_stats": TeamSeasonStats,
    "standings": Standings,
    "game_rosters": GameRosters,
    "officials": Officials,
    "draft": Draft,
    "team_crosswalk": TeamCrosswalk,
    "schedule_crosswalk": ScheduleCrosswalk,
    "player_crosswalk": PlayerCrosswalk,
}


def polars_schema(dataset: str) -> pl.Schema:
    """The dataset's declared columns and dtypes as a polars Schema."""
    model = MODELS[dataset]
    fields: dict[str, pl.DataType] = {}
    for name, info in model.model_fields.items():
        annotation = info.annotation
        args = getattr(annotation, "__args__", None)
        base = next((a for a in args if a is not type(None)), str) if args else annotation
        fields[name] = _PL_TYPES.get(base, pl.Utf8)
    return pl.Schema(fields)


def check_frame(dataset: str, frame: pl.DataFrame) -> list[str]:
    """Frame-level schema check. Returns problems; empty means it matches.

    Widening is tolerated (an Int32 id read back from an older asset is
    losslessly an Int64); narrowing and type changes are not.
    """
    declared = polars_schema(dataset)
    widenable = {pl.Int8, pl.Int16, pl.Int32, pl.UInt8, pl.UInt16, pl.UInt32}
    problems: list[str] = []
    for name, dtype in declared.items():
        if name not in frame.columns:
            problems.append(f"{dataset}: missing column {name!r}")
            continue
        actual = frame.schema[name]
        if actual == dtype:
            continue
        if dtype == pl.Int64 and actual in widenable:
            continue
        problems.append(f"{dataset}: {name!r} is {actual}, declared {dtype}")
    return problems
