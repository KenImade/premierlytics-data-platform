# defs/jobs.py
import dagster as dg

fpl_pipeline_job = dg.define_asset_job(
    name="fpl_pipeline_job",
    selection="*",
    description="Full pipeline for a single partition: raw → transformed → loaded → dbt",
)

fpl_backfill_job = dg.define_asset_job(
    name="fpl_backfill_job",
    selection=dg.AssetSelection.groups("raw_data", "transformed_data", "loaded_data"),
    description=(
        "Backfill job: raw → transformed → loaded across multiple partitions. "
        "Excludes dbt — run fpl_dbt_job separately after all partitions are loaded."
    ),
)

fpl_dbt_job = dg.define_asset_job(
    name="fpl_dbt_job",
    selection=dg.AssetSelection.tag("dagster/kind", "dbt"),
    description=(
        "Runs all dbt models. Trigger manually after a backfill to rebuild "
        "the full dimensional model from the loaded bronze tables."
    ),
)
