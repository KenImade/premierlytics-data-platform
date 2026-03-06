from pydantic import BaseModel, field_validator, model_validator
from typing import Literal, Optional

StatusType = Literal["a", "d", "i", "n", "s", "u"]
# a=available, d=doubtful, i=injured, n=not available, s=suspended, u=unavailable


class PlayerStatsV1(BaseModel):
    id: int
    status: StatusType
    gw: int

    # Availability — null when not applicable (e.g. unavailable players)
    chance_of_playing_next_round: Optional[int] = None
    chance_of_playing_this_round: Optional[int] = None

    # Pricing
    now_cost: float
    now_cost_rank: int
    now_cost_rank_type: int
    cost_change_event: int
    cost_change_event_fall: int
    cost_change_start: int
    cost_change_start_fall: int

    # Ownership
    selected_by_percent: float
    selected_rank: int
    selected_rank_type: int

    # Points
    total_points: int
    event_points: int
    points_per_game: float
    points_per_game_rank: int
    points_per_game_rank_type: int
    bonus: int
    bps: int

    # Form
    form: float
    form_rank: int
    form_rank_type: int
    value_form: float
    value_season: float
    dreamteam_count: int

    # Transfers
    transfers_in: int
    transfers_in_event: int
    transfers_out: int
    transfers_out_event: int

    # Expected points
    ep_next: Optional[float] = None
    ep_this: Optional[float] = None

    # Expected stats (season totals)
    expected_goals: float
    expected_assists: float
    expected_goal_involvements: float
    expected_goals_conceded: float

    # Expected stats (per 90)
    expected_goals_per_90: float
    expected_assists_per_90: float
    expected_goal_involvements_per_90: float
    expected_goals_conceded_per_90: float

    # ICT index
    influence: float
    influence_rank: int
    influence_rank_type: int
    creativity: float
    creativity_rank: int
    creativity_rank_type: int
    threat: float
    threat_rank: int
    threat_rank_type: int
    ict_index: float
    ict_index_rank: int
    ict_index_rank_type: int

    # Set pieces — null for players not assigned a role
    corners_and_indirect_freekicks_order: Optional[int] = None
    direct_freekicks_order: Optional[int] = None
    penalties_order: Optional[int] = None
    set_piece_threat: Optional[float] = None

    @field_validator(
        "chance_of_playing_next_round",
        "chance_of_playing_this_round",
        "ep_next",
        "ep_this",
        "corners_and_indirect_freekicks_order",
        "direct_freekicks_order",
        "penalties_order",
        "set_piece_threat",
        mode="before",
    )
    @classmethod
    def empty_string_to_none(cls, v):
        if v == "" or v is None:
            return None
        return v


class PlayerStatsV2(BaseModel):
    """Pydantic model for a player stats row."""

    # Identity
    id: int
    first_name: str
    second_name: str
    web_name: str

    # Availability
    status: str  # 'a' = available, 'u' = unavailable, 'd' = doubtful
    chance_of_playing_next_round: Optional[float] = None
    chance_of_playing_this_round: Optional[float] = None
    news: Optional[str] = None
    news_added: Optional[str] = None

    # Cost
    now_cost: float
    now_cost_rank: int
    now_cost_rank_type: Optional[float] = None
    cost_change_event: int
    cost_change_event_fall: float
    cost_change_start: int
    cost_change_start_fall: float

    # Selection
    selected_by_percent: float
    selected_rank: int
    selected_rank_type: Optional[float] = None

    # Points
    total_points: int
    event_points: int
    points_per_game: float
    points_per_game_rank: Optional[float] = None
    points_per_game_rank_type: Optional[float] = None
    bonus: int
    bps: int

    # Form & Value
    form: float
    form_rank: Optional[float] = None
    form_rank_type: Optional[float] = None
    value_form: float
    value_season: float
    dreamteam_count: float

    # Transfers
    transfers_in: int
    transfers_in_event: int
    transfers_out: int
    transfers_out_event: int

    # Expected points
    ep_next: float
    ep_this: float

    # Expected stats
    expected_goals: float
    expected_assists: float
    expected_goal_involvements: float
    expected_goals_conceded: float
    expected_goals_per_90: float
    expected_assists_per_90: float
    expected_goal_involvements_per_90: float
    expected_goals_conceded_per_90: float

    # ICT Index
    influence: float
    influence_rank: Optional[float] = None
    influence_rank_type: Optional[float] = None
    creativity: float
    creativity_rank: Optional[float] = None
    creativity_rank_type: Optional[float] = None
    threat: float
    threat_rank: Optional[float] = None
    threat_rank_type: Optional[float] = None
    ict_index: float
    ict_index_rank: Optional[float] = None
    ict_index_rank_type: Optional[float] = None

    # Set pieces
    corners_and_indirect_freekicks_order: Optional[float] = None
    corners_and_indirect_freekicks_text: Optional[str] = None
    direct_freekicks_order: Optional[float] = None
    direct_freekicks_text: Optional[str] = None
    penalties_order: Optional[float] = None
    penalties_text: Optional[str] = None
    set_piece_threat: Optional[float] = None

    # Gameweek
    gw: int

    # Match stats
    minutes: int
    goals_scored: int
    assists: int
    clean_sheets: int
    goals_conceded: int
    own_goals: int
    penalties_saved: int
    penalties_missed: int
    yellow_cards: int
    red_cards: int
    saves: int
    starts: int
    defensive_contribution: int
    tackles: int
    clearances_blocks_interceptions: int
    recoveries: int

    # Per-90 stats
    saves_per_90: float
    clean_sheets_per_90: float
    goals_conceded_per_90: float
    starts_per_90: float
    defensive_contribution_per_90: float

    @model_validator(mode="before")
    @classmethod
    def coerce_empty_strings(cls, values: dict) -> dict:
        return {
            k: None if isinstance(v, str) and v.strip() == "" else v
            for k, v in values.items()
        }
