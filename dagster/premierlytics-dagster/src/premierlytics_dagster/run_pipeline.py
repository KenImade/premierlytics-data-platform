"""
Entrypoint for running the pipeline in production.
Executes the FPL pipeline job for the current gameweek, then exits.
"""

import sys
from premierlytics_dagster.definitions import defs
from premierlytics_dagster.helpers.current_gameweek import get_current_gameweek
import dagster as dg

CURRENT_SEASON = "2025-2026"


def main():
    current_gw = get_current_gameweek()
    gameweek = f"GW{current_gw}"

    print(f"Running pipeline for {CURRENT_SEASON} {gameweek}")

    partition_key = dg.MultiPartitionKey(
        {
            "season": CURRENT_SEASON,
            "gameweek": gameweek,
        }
    )

    job = defs.get_job_def("fpl_pipeline_job")
    result = job.execute_in_process(
        partition_key=partition_key,
    )

    if result.success:
        print("Pipeline completed successfully")
        sys.exit(0)
    else:
        print("Pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
