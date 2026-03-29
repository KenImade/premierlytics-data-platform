from typing import Optional
from pydantic import BaseModel


class MatchesV1(BaseModel):
    gameweek: int
    kickoff_time: str
    home_team: int
    home_team_elo: float
    home_score: int
    away_score: int
    away_team: int
    away_team_elo: float
    finished: bool
    match_id: str
    match_url: str

    # Possession
    home_possession: int
    away_possession: int

    # Expected goals
    home_expected_goals_xg: float
    away_expected_goals_xg: float
    home_xg_open_play: float
    away_xg_open_play: float
    home_xg_set_play: float
    away_xg_set_play: float
    home_non_penalty_xg: float
    away_non_penalty_xg: float
    home_xg_on_target_xgot: float
    away_xg_on_target_xgot: float

    # Shots
    home_total_shots: int
    away_total_shots: int
    home_shots_on_target: int
    away_shots_on_target: int
    home_shots_off_target: int
    away_shots_off_target: int
    home_blocked_shots: int
    away_blocked_shots: int
    home_shots_inside_box: int
    away_shots_inside_box: int
    home_shots_outside_box: int
    away_shots_outside_box: int
    home_big_chances: int
    away_big_chances: int
    home_big_chances_missed: int
    away_big_chances_missed: int
    home_hit_woodwork: int
    away_hit_woodwork: int

    # Passes
    home_passes: int
    away_passes: int
    home_accurate_passes: int
    away_accurate_passes: int
    home_accurate_passes_pct: int
    away_accurate_passes_pct: int
    home_own_half: int
    away_own_half: int
    home_opposition_half: int
    away_opposition_half: int
    home_accurate_long_balls: int
    away_accurate_long_balls: int
    home_accurate_long_balls_pct: int
    away_accurate_long_balls_pct: int
    home_accurate_crosses: int
    away_accurate_crosses: int
    home_accurate_crosses_pct: int
    away_accurate_crosses_pct: int

    # Set pieces / general
    home_corners: int
    away_corners: int
    home_throws: int
    away_throws: int
    home_fouls_committed: int
    away_fouls_committed: int
    home_offsides: int
    away_offsides: int
    home_yellow_cards: int
    away_yellow_cards: int
    home_red_cards: int
    away_red_cards: int
    home_touches_in_opposition_box: int
    away_touches_in_opposition_box: int

    # Defensive
    home_tackles_won: int
    away_tackles_won: int
    home_tackles_won_pct: Optional[float] = None
    away_tackles_won_pct: Optional[float] = None
    home_interceptions: int
    away_interceptions: int
    home_blocks: int
    away_blocks: int
    home_clearances: int
    away_clearances: int
    home_keeper_saves: int
    away_keeper_saves: int

    # Duels
    home_duels_won: int
    away_duels_won: int
    home_ground_duels_won: int
    away_ground_duels_won: int
    home_ground_duels_won_pct: int
    away_ground_duels_won_pct: int
    home_aerial_duels_won: int
    away_aerial_duels_won: int
    home_aerial_duels_won_pct: int
    away_aerial_duels_won_pct: int

    # Dribbles
    home_successful_dribbles: int
    away_successful_dribbles: int
    home_successful_dribbles_pct: int
    away_successful_dribbles_pct: int

    # Metadata
    fotmob_id: int
    stats_processed: bool
    player_stats_processed: bool


class MatchesV2(MatchesV1):
    tournament: str

    # Physical / tracking data
    away_distance_covered: Optional[int] = None
    home_distance_covered: Optional[int] = None
    home_walking_distance: Optional[int] = None
    away_walking_distance: Optional[int] = None
    home_running_distance: Optional[int] = None
    away_running_distance: Optional[int] = None
    home_sprinting_distance: Optional[int] = None
    away_sprinting_distance: Optional[int] = None
    home_number_of_sprints: Optional[int] = None
    away_number_of_sprints: Optional[int] = None
    home_top_speed: Optional[float] = None
    away_top_speed: Optional[float] = None
