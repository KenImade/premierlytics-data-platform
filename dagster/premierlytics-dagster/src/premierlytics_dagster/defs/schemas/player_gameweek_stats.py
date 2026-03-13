from typing import Optional, Literal
from pydantic import BaseModel

StatusType = Literal["a", "d", "i", "n", "s", "u"]


class PlayerGameweekStatsV1(BaseModel):
    id: int
    first_name: str
    second_name: str
    web_name: str
    status: StatusType

    news: Optional[str] = None
    news_added: Optional[str] = None

    now_cost: float
    now_cost_rank: int
    now_cost_rank_type: Optional[float]

    total_points: int
    event_points: int
    points_per_game: Optional[float] = None
    selected_by_percent: float
    transfers_in: int
    transfers_out: int

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
    bonus: int
    bps: int

    influence: float
    creativity: float
    threat: float
    ict_index: float

    expected_goals: float
    expected_assists: float
    expected_goal_involvements: float
    expected_goals_conceded: float

    tackles: int
    clearances_blocks_interceptions: int
    recoveries: int
    defensive_contribution: int
