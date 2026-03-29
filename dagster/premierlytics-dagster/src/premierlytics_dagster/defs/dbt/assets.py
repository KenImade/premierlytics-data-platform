import os
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

# Path to the dbt project inside the container
DBT_PROJECT_DIR = Path("/opt/dagster/dbt")


dbt_project = DbtProject(
    project_dir=os.environ.get("DBT_PROJECT_DIR", DBT_PROJECT_DIR),
    profiles_dir=Path(__file__).parent,  # profiles.yml lives next to this file
)

# This will fail at import time if manifest doesn't exist yet
dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path, op_tags={"dagster/concurrency_key": "duckdb"}
)
def premierlytics_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
