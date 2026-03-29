-- DDL for teams_bronze table

DROP TYPE IF EXISTS strength_rating;
CREATE TYPE strength_rating AS ENUM ('1', '2', '3', '4', '5');

CREATE TABLE IF NOT EXISTS teams_bronze (
    code INTEGER NOT NULL,
    id INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    short_name VARCHAR NOT NULL,
    strength strength_rating,
    strength_overall_home INTEGER NOT NULL,
    strength_overall_away INTEGER NOT NULL,
    strength_attack_home INTEGER NOT NULL,
    strength_attack_away INTEGER NOT NULL,
    strength_defence_home INTEGER NOT NULL,
    strength_defence_away INTEGER NOT NULL,
    pulse_id INTEGER NOT NULL,

    -- V2
    fotmob_name VARCHAR,

    -- metadata
    season VARCHAR NOT NULL,
    ingested_at TIMESTAMP NOT NULL
);

