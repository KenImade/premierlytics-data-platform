import dagster as dg
from .defs.resources import MinioResource
from .defs.raw.assets import (
    raw_matches_data,
    raw_playermatchstats_data,
    raw_players_data,
    raw_playerstats_data,
    raw_teams_data,
    raw_player_gameweek_stats_data,
    raw_fixtures_data,
)

from .defs.transformation.assets import transformed_fixtures

defs = dg.Definitions(
    assets=[
        raw_matches_data,
        raw_playermatchstats_data,
        raw_players_data,
        raw_playerstats_data,
        raw_teams_data,
        raw_player_gameweek_stats_data,
        raw_fixtures_data,
        transformed_fixtures,
    ],
    resources={
        "minio": MinioResource(
            endpoint=dg.EnvVar("MINIO_ENDPOINT"),
            access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
            secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
        )
    },
)
