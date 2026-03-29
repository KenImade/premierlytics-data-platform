-- DDL for players_bronze table

DROP TYPE IF EXISTS position_type;
CREATE TYPE position_type AS ENUM ('Goalkeeper', 'Defender', 'Midfielder', 'Forward', 'Unknown');

CREATE TABLE IF NOT EXISTS players_bronze (
    player_unique_id UUID DEFAULT gen_random_uuid(),
    season VARCHAR NOT NULL,
    gameweek INTEGER NOT NULL,
    player_code INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    first_name VARCHAR NOT NULL,
    second_name VARCHAR NOT NULL,
    web_name VARCHAR NOT NULL,
    team_code INTEGER NOT NULL,
    position position_type,
    ingested_at TIMESTAMP NOT NULL,

    PRIMARY KEY (player_unique_id)
)