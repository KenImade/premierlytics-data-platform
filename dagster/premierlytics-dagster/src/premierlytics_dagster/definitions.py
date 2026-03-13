import dagster as dg
from .defs.resources import MinioResource, DuckDBResource
from .defs.raw.assets import (
    raw_matches,
    raw_playermatchstats,
    raw_players,
    raw_playerstats,
    raw_teams,
    raw_player_gameweek_stats,
    raw_fixtures,
)

from .defs.transformation.assets import (
    transformed_fixtures,
    transformed_matches,
    transformed_teams,
    transformed_player_gameweek_stats,
    transformed_playermatchstats,
    transformed_players,
    transformed_playerstats,
)
from .defs.loading.assets import (
    loaded_fixtures,
    loaded_matches,
    loaded_playermatchstats,
    loaded_players,
)

defs = dg.Definitions(
    assets=[
        raw_matches,
        raw_playermatchstats,
        raw_players,
        raw_playerstats,
        raw_teams,
        raw_player_gameweek_stats,
        raw_fixtures,
        transformed_fixtures,
        transformed_matches,
        transformed_teams,
        transformed_player_gameweek_stats,
        transformed_players,
        transformed_playerstats,
        transformed_playermatchstats,
        loaded_matches,
        loaded_fixtures,
        loaded_playermatchstats,
        loaded_players,
    ],
    resources={
        "minio": MinioResource(
            endpoint=dg.EnvVar("MINIO_ENDPOINT"),
            access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
            secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
        ),
        "duckdb": DuckDBResource(),
    },
)
