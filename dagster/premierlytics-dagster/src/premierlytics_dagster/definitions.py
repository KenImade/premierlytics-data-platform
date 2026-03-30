import dagster as dg
from dagster_dbt import DbtCliResource
from .defs.resources import MinioResource, DuckDBResource
from .defs.dbt.assets import dbt_project
from .defs.raw.assets import build_raw_asset
from .defs.transformation.assets import build_transformed_asset
from .defs.transformation.checks import build_transformed_checks
from .defs.loading.assets import build_loaded_asset
from .defs.loading.checks import build_loaded_checks
from .defs.dbt.assets import premierlytics_dbt_assets
from .defs.schedules import fpl_refresh_schedule
from .defs.jobs import fpl_pipeline_job

DATASETS = [
    "matches",
    "playermatchstats",
    "players",
    "playerstats",
    "teams",
    "player_gameweek_stats",
    "fixtures",
]

raw_assets = [build_raw_asset(d) for d in DATASETS]
transformed_assets = [build_transformed_asset(d) for d in DATASETS]
loaded_assets = [build_loaded_asset(d) for d in DATASETS]

all_checks = []
for dataset in DATASETS:
    all_checks.extend(build_transformed_checks(dataset))
    all_checks.extend(build_loaded_checks(dataset))

defs = dg.Definitions(
    assets=[*raw_assets, *transformed_assets, *loaded_assets, premierlytics_dbt_assets],
    asset_checks=all_checks,
    schedules=[fpl_refresh_schedule],
    jobs=[fpl_pipeline_job],
    resources={
        "minio": MinioResource(
            endpoint=dg.EnvVar("MINIO_ENDPOINT"),
            access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
            secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
            bucket=dg.EnvVar("MINIO_BUCKET_NAME"),
            secure=False,
        ),
        "duckdb": DuckDBResource(),
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=dbt_project.profiles_dir,
        ),
    },
)
