# defs/jobs.py
import dagster as dg

fpl_pipeline_job = dg.define_asset_job(
    name="fpl_pipeline_job",
    selection="*",
    description="Full pipeline: raw → transformed → loaded → dbt",
)
