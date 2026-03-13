from pydantic import BaseModel


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
    saves: int
    goals_conceded: int
    xgot_faced: float
    goals_prevented: float
    sweeper_actions: int
    gk_accurate_passes: int
    gk_accurate_long_balls: int

    offsides: int
    high_claim: int
    tackles: int

    # Percentage fields — can be 0 for players with no attempts
    accurate_passes_percent: float
    accurate_crosses_percent: float
    accurate_long_balls_percent: float
    ground_duels_won_percent: float
    aerial_duels_won_percent: float
    successful_dribbles_percent: float
    tackles_won_percent: float

    start_min: int
    finish_min: int
    team_goals_conceded: int
    penalties_scored: int
    penalties_missed: int


class PlayerMatchStatsV2(PlayerMatchStatsV1):
    dispossessed: float  # occasionally non-integer in source
    corners: int
    saves_inside_box: int

    # Physical/tracking data (all zero in current data)
    top_speed: float
    distance_covered: float
    walking_distance: float
    running_distance: float
    sprinting_distance: float
    number_of_sprints: float
    defensive_contributions: float
