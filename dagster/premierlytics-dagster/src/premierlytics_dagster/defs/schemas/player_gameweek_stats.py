from typing import Optional
from pydantic import BaseModel, model_validator


class PlayerGameweekStatsV1(BaseModel):
    id: int
    first_name: str
    second_name: str
    web_name: str
    status: str

    news: Optional[str] = None
    news_added: Optional[str] = None

    now_cost: float
    now_cost_rank: int
    now_cost_rank_type: Optional[float]

    total_points: int
    event_points: int
    points_per_game: float
    selected_by_percent: float
    transfers_in: int
    transfers_out: int
    transfers_balance: int

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

    @model_validator(mode="before")
    @classmethod
    def coerce_empty_strings(cls, values: dict) -> dict:
        return {
            k: None if isinstance(v, str) and v.strip() == "" else v
            for k, v in values.items()
        }
