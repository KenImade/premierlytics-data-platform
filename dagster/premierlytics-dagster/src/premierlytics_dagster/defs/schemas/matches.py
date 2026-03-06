from datetime import datetime
from typing import Optional
from pydantic import BaseModel, field_validator, model_validator


class MatchesV1(BaseModel):
    gameweek: int
    kickoff_time: datetime
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
    home_tackles_won_pct: int
    away_tackles_won_pct: int
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
    fotmob_id: float
    stats_processed: bool
    player_stats_processed: bool

    @field_validator("kickoff_time", mode="before")
    @classmethod
    def parse_kickoff_time(cls, v: str) -> datetime:
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")

    @field_validator(
        "finished", "stats_processed", "player_stats_processed", mode="before"
    )
    @classmethod
    def parse_bool(cls, v: str) -> bool:
        if isinstance(v, bool):
            return v
        return v.strip().lower() == "true"

    @model_validator(mode="before")
    @classmethod
    def coerce_empty_strings(cls, values: dict) -> dict:
        return {
            k: None if isinstance(v, str) and v.strip() == "" else v
            for k, v in values.items()
        }


class MatchesV2(BaseModel):
    gameweek: float
    kickoff_time: datetime
    match_id: str
    match_url: str
    tournament: str
    finished: bool
    fotmob_id: float

    # Teams
    home_team: float
    home_team_elo: float
    away_team: float
    away_team_elo: float

    # Score
    home_score: float
    away_score: float

    # Possession
    home_possession: float
    away_possession: float

    # xG
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
    home_total_shots: float
    away_total_shots: float
    home_shots_on_target: float
    away_shots_on_target: float
    home_shots_off_target: float
    away_shots_off_target: float
    home_blocked_shots: float
    away_blocked_shots: float
    home_shots_inside_box: float
    away_shots_inside_box: float
    home_shots_outside_box: float
    away_shots_outside_box: float
    home_hit_woodwork: float
    away_hit_woodwork: float

    # Chances
    home_big_chances: float
    away_big_chances: float
    home_big_chances_missed: float
    away_big_chances_missed: float

    # Passes
    home_passes: float
    away_passes: float
    home_accurate_passes: float
    away_accurate_passes: float
    home_accurate_passes_pct: float
    away_accurate_passes_pct: float
    home_accurate_long_balls: float
    away_accurate_long_balls: float
    home_accurate_long_balls_pct: float
    away_accurate_long_balls_pct: float
    home_accurate_crosses: float
    away_accurate_crosses: float
    home_accurate_crosses_pct: float
    away_accurate_crosses_pct: float
    home_own_half: float
    away_own_half: float
    home_opposition_half: float
    away_opposition_half: float

    # Attacking
    home_corners: float
    away_corners: float
    home_touches_in_opposition_box: float
    away_touches_in_opposition_box: float
    home_offsides: float
    away_offsides: float
    home_throws: float
    away_throws: float

    # Defending
    home_tackles_won: float
    away_tackles_won: float
    home_tackles_won_pct: Optional[float] = None
    away_tackles_won_pct: Optional[float] = None
    home_interceptions: float
    away_interceptions: float
    home_blocks: float
    away_blocks: float
    home_clearances: float
    away_clearances: float
    home_keeper_saves: float
    away_keeper_saves: float
    home_fouls_committed: float
    away_fouls_committed: float
    home_yellow_cards: float
    away_yellow_cards: float
    home_red_cards: float
    away_red_cards: float

    # Duels
    home_duels_won: float
    away_duels_won: float
    home_ground_duels_won: float
    away_ground_duels_won: float
    home_ground_duels_won_pct: float
    away_ground_duels_won_pct: float
    home_aerial_duels_won: float
    away_aerial_duels_won: float
    home_aerial_duels_won_pct: float
    away_aerial_duels_won_pct: float
    home_successful_dribbles: float
    away_successful_dribbles: float
    home_successful_dribbles_pct: float
    away_successful_dribbles_pct: float

    # Physical / tracking data
    home_distance_covered: float
    away_distance_covered: float
    home_walking_distance: float
    away_walking_distance: float
    home_running_distance: float
    away_running_distance: float
    home_sprinting_distance: float
    away_sprinting_distance: float
    home_number_of_sprints: float
    away_number_of_sprints: float
    home_top_speed: Optional[float] = None
    away_top_speed: Optional[float] = None

    # Processing flags
    stats_processed: bool
    player_stats_processed: bool

    @model_validator(mode="before")
    @classmethod
    def coerce_empty_strings(cls, values: dict) -> dict:
        return {
            k: None if isinstance(v, str) and v.strip() == "" else v
            for k, v in values.items()
        }
