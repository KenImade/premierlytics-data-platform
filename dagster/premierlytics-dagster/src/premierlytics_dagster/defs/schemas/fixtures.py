from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class FixturesV1(BaseModel):
    # Always present
    gameweek: float
    kickoff_time: datetime
    home_team: float
    away_team: float
    home_team_elo: float
    away_team_elo: float
    finished: bool
    match_id: str
    match_url: str
    tournament: str
    fotmob_id: float
    stats_processed: bool
    player_stats_processed: bool

    # Only present after match is played
    home_score: Optional[float] = None
    away_score: Optional[float] = None

    # Possession
    home_possession: Optional[float] = None
    away_possession: Optional[float] = None

    # Expected goals
    home_expected_goals_xg: Optional[float] = None
    away_expected_goals_xg: Optional[float] = None
    home_xg_open_play: Optional[float] = None
    away_xg_open_play: Optional[float] = None
    home_xg_set_play: Optional[float] = None
    away_xg_set_play: Optional[float] = None
    home_non_penalty_xg: Optional[float] = None
    away_non_penalty_xg: Optional[float] = None
    home_xg_on_target_xgot: Optional[float] = None
    away_xg_on_target_xgot: Optional[float] = None

    # Shots
    home_total_shots: Optional[int] = None
    away_total_shots: Optional[int] = None
    home_shots_on_target: Optional[int] = None
    away_shots_on_target: Optional[int] = None
    home_shots_off_target: Optional[int] = None
    away_shots_off_target: Optional[int] = None
    home_blocked_shots: Optional[int] = None
    away_blocked_shots: Optional[int] = None
    home_shots_inside_box: Optional[int] = None
    away_shots_inside_box: Optional[int] = None
    home_shots_outside_box: Optional[int] = None
    away_shots_outside_box: Optional[int] = None
    home_hit_woodwork: Optional[int] = None
    away_hit_woodwork: Optional[int] = None
    home_big_chances: Optional[int] = None
    away_big_chances: Optional[int] = None
    home_big_chances_missed: Optional[int] = None
    away_big_chances_missed: Optional[int] = None

    # Passing
    home_passes: Optional[int] = None
    away_passes: Optional[int] = None
    home_accurate_passes: Optional[int] = None
    away_accurate_passes: Optional[int] = None
    home_accurate_passes_pct: Optional[float] = None
    away_accurate_passes_pct: Optional[float] = None
    home_own_half: Optional[int] = None
    away_own_half: Optional[int] = None
    home_opposition_half: Optional[int] = None
    away_opposition_half: Optional[int] = None
    home_accurate_long_balls: Optional[int] = None
    away_accurate_long_balls: Optional[int] = None
    home_accurate_long_balls_pct: Optional[float] = None
    away_accurate_long_balls_pct: Optional[float] = None
    home_accurate_crosses: Optional[int] = None
    away_accurate_crosses: Optional[int] = None
    home_accurate_crosses_pct: Optional[float] = None
    away_accurate_crosses_pct: Optional[float] = None
    home_throws: Optional[int] = None
    away_throws: Optional[int] = None

    # Discipline
    home_fouls_committed: Optional[int] = None
    away_fouls_committed: Optional[int] = None
    home_yellow_cards: Optional[int] = None
    away_yellow_cards: Optional[int] = None
    home_red_cards: Optional[int] = None
    away_red_cards: Optional[int] = None
    home_offsides: Optional[int] = None
    away_offsides: Optional[int] = None
    home_corners: Optional[int] = None
    away_corners: Optional[int] = None

    # Attacking
    home_touches_in_opposition_box: Optional[int] = None
    away_touches_in_opposition_box: Optional[int] = None

    # Defensive
    home_tackles_won: Optional[int] = None
    away_tackles_won: Optional[int] = None
    home_tackles_won_pct: Optional[float] = None
    away_tackles_won_pct: Optional[float] = None
    home_interceptions: Optional[int] = None
    away_interceptions: Optional[int] = None
    home_blocks: Optional[int] = None
    away_blocks: Optional[int] = None
    home_clearances: Optional[int] = None
    away_clearances: Optional[int] = None
    home_keeper_saves: Optional[int] = None
    away_keeper_saves: Optional[int] = None

    # Duels
    home_duels_won: Optional[int] = None
    away_duels_won: Optional[int] = None
    home_ground_duels_won: Optional[int] = None
    away_ground_duels_won: Optional[int] = None
    home_ground_duels_won_pct: Optional[float] = None
    away_ground_duels_won_pct: Optional[float] = None
    home_aerial_duels_won: Optional[int] = None
    away_aerial_duels_won: Optional[int] = None
    home_aerial_duels_won_pct: Optional[float] = None
    away_aerial_duels_won_pct: Optional[float] = None
    home_successful_dribbles: Optional[int] = None
    away_successful_dribbles: Optional[int] = None
    home_successful_dribbles_pct: Optional[float] = None
    away_successful_dribbles_pct: Optional[float] = None

    # Physical metrics
    home_distance_covered: Optional[float] = None
    away_distance_covered: Optional[float] = None
    home_walking_distance: Optional[float] = None
    away_walking_distance: Optional[float] = None
    home_running_distance: Optional[float] = None
    away_running_distance: Optional[float] = None
    home_sprinting_distance: Optional[float] = None
    away_sprinting_distance: Optional[float] = None
    home_number_of_sprints: Optional[int] = None
    away_number_of_sprints: Optional[int] = None
    home_top_speed: Optional[float] = None
    away_top_speed: Optional[float] = None
