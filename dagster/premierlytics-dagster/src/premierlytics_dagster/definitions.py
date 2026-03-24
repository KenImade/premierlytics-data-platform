import dagster as dg
from dagster_dbt import DbtCliResource
from .defs.resources import MinioResource, DuckDBResource
from .defs.dbt.assets import premierlytics_dbt_assets, dbt_project
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
    loaded_playerstats,
    loaded_teams,
    loaded_player_gameweek_stats,
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
        loaded_playerstats,
        loaded_teams,
        loaded_player_gameweek_stats,
        premierlytics_dbt_assets,
    ],
    resources={
        "minio": MinioResource(
            endpoint=dg.EnvVar("MINIO_ENDPOINT"),
            access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
            secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
        ),
        "duckdb": DuckDBResource(),
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=dbt_project.profiles_dir,
        ),
    },
)
