-- DDL for player_gameweek_stats_bronze table

DROP TYPE IF EXISTS status_type;

CREATE TYPE status_type AS ENUM ('a', 'd', 'i', 'n', 's', 'u');

CREATE TABLE IF NOT EXISTS player_gameweek_stats_bronze (
    id INTEGER NOT NULL,
    first_name VARCHAR NOT NULL,
    second_name VARCHAR NOT NULL,
    web_name VARCHAR NOT NULL,
    status status_type,

    news VARCHAR,
    news_added VARCHAR,

    now_cost DOUBLE NOT NULL,
    now_cost_rank INTEGER NOT NULL,
    now_cost_rank_type DOUBLE,

    total_points INTEGER NOT NULL,
    event_points INTEGER NOT NULL,
    points_per_game DOUBLE,
    selected_by_percent DOUBLE NOT NULL,
    transfers_in INTEGER NOT NULL,
    transfers_out INTEGER NOT NULL,

    minutes INTEGER NOT NULL,
    goals_scored INTEGER NOT NULL,
    assists INTEGER NOT NULL,
    clean_sheets INTEGER NOT NULL,
    goals_conceded INTEGER NOT NULL,
    own_goals INTEGER NOT NULL,
    penalties_saved INTEGER NOT NULL,
    penalties_missed INTEGER NOT NULL,
    yellow_cards INTEGER NOT NULL,
    red_cards INTEGER NOT NULL,
    saves INTEGER NOT NULL,
    bonus INTEGER NOT NULL,
    bps INTEGER NOT NULL,

    influence DOUBLE NOT NULL,
    creativity DOUBLE NOT NULL,
    threat DOUBLE NOT NULL,
    ict_index DOUBLE NOT NULL,

    expected_goals DOUBLE NOT NULL,
    expected_assists DOUBLE NOT NULL,
    expected_goal_involvements DOUBLE NOT NULL,
    expected_goals_conceded DOUBLE NOT NULL,

    tackles INTEGER NOT NULL,
    clearances_blocks_interceptions INTEGER NOT NULL,
    recoveries INTEGER NOT NULL,
    defensive_contribution INTEGER NOT NULL,

    -- Metadata
    season VARCHAR NOT NULL,
    gameweek INTEGER NOT NULL,
    ingested_at TIMESTAMP NOT NULL
)