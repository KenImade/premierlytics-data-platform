from pydantic import BaseModel
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
    now_cost: Optional[float] = None
    now_cost_rank: int
    now_cost_rank_type: Optional[int] = None
    cost_change_event: int
    cost_change_event_fall: Optional[int] = None
    cost_change_start: Optional[int] = None
    cost_change_start_fall: Optional[int] = None

    # Ownership
    selected_by_percent: float
    selected_rank: int
    selected_rank_type: Optional[int] = None

    # Points
    total_points: int
    event_points: int
    points_per_game: Optional[float] = None
    points_per_game_rank: Optional[int] = None
    points_per_game_rank_type: Optional[int] = None
    bonus: int
    bps: int

    # Form
    form: float
    form_rank: Optional[int] = None
    form_rank_type: Optional[int] = None
    value_form: float
    value_season: float
    dreamteam_count: Optional[int] = None

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
    expected_goals_per_90: Optional[float] = None
    expected_assists_per_90: Optional[float] = None
    expected_goal_involvements_per_90: Optional[float] = None
    expected_goals_conceded_per_90: Optional[float] = None

    # ICT index
    influence: float
    influence_rank: Optional[int] = None
    influence_rank_type: Optional[int] = None
    creativity: float
    creativity_rank: Optional[int] = None
    creativity_rank_type: Optional[int] = None
    threat: Optional[float] = None
    threat_rank: Optional[int] = None
    threat_rank_type: Optional[int] = None
    ict_index: float
    ict_index_rank: Optional[int] = None
    ict_index_rank_type: Optional[int] = None

    # Set pieces — null for players not assigned a role
    corners_and_indirect_freekicks_order: Optional[int] = None
    direct_freekicks_order: Optional[int] = None
    penalties_order: Optional[int] = None
    set_piece_threat: Optional[float] = None


class PlayerStatsV2(PlayerStatsV1):
    """Pydantic model for a player stats row."""

    first_name: str
    second_name: str
    web_name: str

    news: Optional[str] = None
    news_added: Optional[str] = None

    # Set pieces
    corners_and_indirect_freekicks_text: Optional[str] = None
    direct_freekicks_text: Optional[str] = None
    penalties_text: Optional[str] = None
    set_piece_threat: Optional[float] = None

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
    saves_per_90: Optional[float] = None
    clean_sheets_per_90: Optional[float] = None
    goals_conceded_per_90: Optional[float] = None
    starts_per_90: Optional[float] = None
    defensive_contribution_per_90: Optional[float] = None
