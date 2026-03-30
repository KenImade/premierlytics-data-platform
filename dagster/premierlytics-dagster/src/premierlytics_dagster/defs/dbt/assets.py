import os
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

DBT_PROJECT_DIR = Path("/opt/dagster/dbt")

dbt_project = DbtProject(
    project_dir=os.environ.get("DBT_PROJECT_DIR", str(DBT_PROJECT_DIR)),
    # profiles.yml must live alongside this file
    profiles_dir=Path(__file__).parent,
)

dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    op_tags={"dagster/concurrency_key": "duckdb"},
)
def premierlytics_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
