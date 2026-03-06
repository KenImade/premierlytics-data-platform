from pydantic import BaseModel, field_validator, model_validator
from typing import Optional


class PlayerMatchStatsV1(BaseModel):
    player_id: int
    match_id: str
    minutes_played: int
    goals: int
    assists: int
    total_shots: int
    xg: float
    xa: float
    xgot: float
    shots_on_target: int
    successful_dribbles: int
    big_chances_missed: int
    touches_opposition_box: int
    touches: int
    accurate_passes: int
    chances_created: int
    final_third_passes: int
    accurate_crosses: int
    accurate_long_balls: int
    tackles_won: int
    interceptions: int
    recoveries: int
    blocks: int
    clearances: int
    headed_clearances: int
    dribbled_past: int
    duels_won: int
    duels_lost: int
    ground_duels_won: int
    aerial_duels_won: int
    was_fouled: int
    fouls_committed: int

    # Goalkeeper fields — null for outfield players
    saves: Optional[int] = None
    goals_conceded: Optional[int] = None
    xgot_faced: Optional[float] = None
    goals_prevented: Optional[float] = None
    sweeper_actions: Optional[int] = None
    gk_accurate_passes: Optional[int] = None
    gk_accurate_long_balls: Optional[int] = None

    offsides: int
    high_claim: int
    tackles: int

    # Percentage fields — can be 0 for players with no attempts
    accurate_passes_percent: Optional[float] = None
    accurate_crosses_percent: Optional[float] = None
    accurate_long_balls_percent: Optional[float] = None
    ground_duels_won_percent: Optional[float] = None
    aerial_duels_won_percent: Optional[float] = None
    successful_dribbles_percent: Optional[float] = None
    tackles_won_percent: Optional[float] = None

    start_min: int
    finish_min: int
    team_goals_conceded: int
    penalties_scored: int
    penalties_missed: int

    @model_validator(mode="before")
    @classmethod
    def coerce_empty_strings(cls, values: dict) -> dict:
        return {
            k: None if isinstance(v, str) and v.strip() == "" else v
            for k, v in values.items()
        }


class PlayerMatchStatsV2(BaseModel):
    player_id: int
    match_id: str
    minutes_played: int
    goals: int
    assists: int
    total_shots: int
    xg: float
    xa: float
    shots_on_target: int
    successful_dribbles: int
    big_chances_missed: int
    touches_opposition_box: int
    touches: int
    accurate_passes: int
    accurate_passes_percent: float
    chances_created: float
    final_third_passes: int
    accurate_crosses: int
    accurate_crosses_percent: float
    accurate_long_balls: int
    accurate_long_balls_percent: float
    tackles_won: int
    interceptions: int
    recoveries: int
    blocks: int
    clearances: int
    headed_clearances: int
    dribbled_past: int
    duels_won: int
    duels_lost: int
    ground_duels_won: int
    ground_duels_won_percent: float
    aerial_duels_won: int
    aerial_duels_won_percent: float
    was_fouled: int
    fouls_committed: int

    # Goalkeeper fields (zero for outfield players)
    saves: int
    goals_conceded: int
    xgot_faced: float
    goals_prevented: float
    sweeper_actions: int
    gk_accurate_passes: int
    gk_accurate_long_balls: int

    dispossessed: float  # occasionally non-integer in source
    high_claim: int
    corners: int
    saves_inside_box: int
    offsides: int

    successful_dribbles_percent: float
    tackles_won_percent: float
    xgot: float

    tackles: int
    start_min: int
    finish_min: int
    team_goals_conceded: int

    penalties_scored: int
    penalties_missed: int

    # Physical/tracking data (all zero in current data)
    top_speed: float
    distance_covered: float
    walking_distance: float
    running_distance: float
    sprinting_distance: float
    number_of_sprints: float
    defensive_contributions: float

    @field_validator(
        "accurate_passes_percent",
        "chances_created",
        "accurate_crosses_percent",
        "accurate_long_balls_percent",
        "ground_duels_won_percent",
        "aerial_duels_won_percent",
        "successful_dribbles_percent",
        "tackles_won_percent",
        "xgot_faced",
        "goals_prevented",
        "xgot",
        "dispossessed",
        mode="before",
    )
    @classmethod
    def empty_string_to_none_or_zero(cls, v):
        if v == "" or v is None:
            return 0.0
        return v
