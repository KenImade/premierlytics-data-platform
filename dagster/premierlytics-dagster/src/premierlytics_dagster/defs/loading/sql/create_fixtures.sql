-- DDL script for Fixtures Table

CREATE TABLE IF NOT EXISTS fixtures_bronze (
    -- Identity
    gameweek                                   DOUBLE NOT NULL,
    season                                     VARCHAR NOT NULL,
    match_id                                   VARCHAR NOT NULL,
    match_url                                  VARCHAR NOT NULL,
    fotmob_id                                  DOUBLE NOT NULL,
    kickoff_time                               TIMESTAMP NOT NULL,
    tournament                                 VARCHAR NOT NULL,
    finished                                   BOOLEAN NOT NULL,
    stats_processed                            BOOLEAN NOT NULL,
    player_stats_processed                     BOOLEAN NOT NULL,

    -- Teams
    home_team                                  DOUBLE NOT NULL,
    away_team                                  DOUBLE NOT NULL,
    home_team_elo                              DOUBLE NOT NULL,
    away_team_elo                              DOUBLE NOT NULL,

    -- Score
    home_score                                 DOUBLE,
    away_score                                 DOUBLE,

    -- Possession
    home_possession                            DOUBLE,
    away_possession                            DOUBLE,

    -- Expected Goals
    home_expected_goals_xg                     DOUBLE,
    away_expected_goals_xg                     DOUBLE,
    home_xg_open_play                          DOUBLE,
    away_xg_open_play                          DOUBLE,
    home_xg_set_play                           DOUBLE,
    away_xg_set_play                           DOUBLE,
    home_non_penalty_xg                        DOUBLE,
    away_non_penalty_xg                        DOUBLE,
    home_xg_on_target_xgot                     DOUBLE,
    away_xg_on_target_xgot                     DOUBLE,

    -- Shots
    home_total_shots                           INTEGER,
    away_total_shots                           INTEGER,
    home_shots_on_target                       INTEGER,
    away_shots_on_target                       INTEGER,
    home_shots_off_target                      INTEGER,
    away_shots_off_target                      INTEGER,
    home_blocked_shots                         INTEGER,
    away_blocked_shots                         INTEGER,
    home_shots_inside_box                      INTEGER,
    away_shots_inside_box                      INTEGER,
    home_shots_outside_box                     INTEGER,
    away_shots_outside_box                     INTEGER,
    home_hit_woodwork                          INTEGER,
    away_hit_woodwork                          INTEGER,
    home_big_chances                           INTEGER,
    away_big_chances                           INTEGER,
    home_big_chances_missed                    INTEGER,
    away_big_chances_missed                    INTEGER,

    -- Passing
    home_passes                                INTEGER,
    away_passes                                INTEGER,
    home_accurate_passes                       INTEGER,
    away_accurate_passes                       INTEGER,
    home_accurate_passes_pct                   DOUBLE,
    away_accurate_passes_pct                   DOUBLE,
    home_own_half                              INTEGER,
    away_own_half                              INTEGER,
    home_opposition_half                       INTEGER,
    away_opposition_half                       INTEGER,
    home_accurate_long_balls                   INTEGER,
    away_accurate_long_balls                   INTEGER,
    home_accurate_long_balls_pct               DOUBLE,
    away_accurate_long_balls_pct               DOUBLE,
    home_accurate_crosses                      INTEGER,
    away_accurate_crosses                      INTEGER,
    home_accurate_crosses_pct                  DOUBLE,
    away_accurate_crosses_pct                  DOUBLE,
    home_throws                                INTEGER,
    away_throws                                INTEGER,

    -- Discipline
    home_fouls_committed                       INTEGER,
    away_fouls_committed                       INTEGER,
    home_yellow_cards                          INTEGER,
    away_yellow_cards                          INTEGER,
    home_red_cards                             INTEGER,
    away_red_cards                             INTEGER,
    home_offsides                              INTEGER,
    away_offsides                              INTEGER,
    home_corners                               INTEGER,
    away_corners                               INTEGER,

    -- Attacking
    home_touches_in_opposition_box             INTEGER,
    away_touches_in_opposition_box             INTEGER,

    -- Defensive
    home_tackles_won                           INTEGER,
    away_tackles_won                           INTEGER,
    home_tackles_won_pct                       DOUBLE,
    away_tackles_won_pct                       DOUBLE,
    home_interceptions                         INTEGER,
    away_interceptions                         INTEGER,
    home_blocks                                INTEGER,
    away_blocks                                INTEGER,
    home_clearances                            INTEGER,
    away_clearances                            INTEGER,
    home_keeper_saves                          INTEGER,
    away_keeper_saves                          INTEGER,

    -- Duels
    home_duels_won                             INTEGER,
    away_duels_won                             INTEGER,
    home_ground_duels_won                      INTEGER,
    away_ground_duels_won                      INTEGER,
    home_ground_duels_won_pct                  DOUBLE,
    away_ground_duels_won_pct                  DOUBLE,
    home_aerial_duels_won                      INTEGER,
    away_aerial_duels_won                      INTEGER,
    home_aerial_duels_won_pct                  DOUBLE,
    away_aerial_duels_won_pct                  DOUBLE,
    home_successful_dribbles                   INTEGER,
    away_successful_dribbles                   INTEGER,
    home_successful_dribbles_pct               DOUBLE,
    away_successful_dribbles_pct               DOUBLE,

    -- Physical
    home_distance_covered                      DOUBLE,
    away_distance_covered                      DOUBLE,
    home_walking_distance                      DOUBLE,
    away_walking_distance                      DOUBLE,
    home_running_distance                      DOUBLE,
    away_running_distance                      DOUBLE,
    home_sprinting_distance                    DOUBLE,
    away_sprinting_distance                    DOUBLE,
    home_number_of_sprints                     INTEGER,
    away_number_of_sprints                     INTEGER,
    home_top_speed                             DOUBLE,
    away_top_speed                             DOUBLE,
    ingested_at                                TIMESTAMP NOT NULL,

    PRIMARY KEY (match_id)
);