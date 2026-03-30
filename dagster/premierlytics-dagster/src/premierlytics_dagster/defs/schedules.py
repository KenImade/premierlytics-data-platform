import dagster as dg
from premierlytics_dagster.helpers.current_gameweek import get_current_gameweek
from .jobs import fpl_pipeline_job

CURRENT_SEASON = "2025-2026"


@dg.schedule(
    cron_schedule="30 17 * * *",
    job=fpl_pipeline_job,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def fpl_refresh_schedule(context: dg.ScheduleEvaluationContext):
    """
    Triggers a full pipeline run daily at 5:30 PM UTC,
    after both FPL Core Insights daily updates have completed.
    """
    current_gw = get_current_gameweek()
    gameweek = f"GW{current_gw}"

    partition_key = dg.MultiPartitionKey(
        {
            "season": CURRENT_SEASON,
            "gameweek": gameweek,
        }
    )

    return dg.RunRequest(
        run_key=f"{CURRENT_SEASON}_{gameweek}_{context.scheduled_execution_time}",
        partition_key=partition_key,
    )
