# defs/loading/checks.py
import dagster as dg
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckExecutionContext,
    asset_check,
)
from ..resources import DuckDBResource
from ..config import get_dataset_config
from premierlytics_dagster.helpers.checks import (
    check_rows_loaded,
    check_table_row_count_matches,
)


def build_loaded_checks(dataset_name: str):
    table_name = f"{dataset_name}_bronze"

    @asset_check(
        asset=f"loaded_{dataset_name}",
        name="rows_loaded",
        description=f"Verify {dataset_name} load produced rows",
    )
    def _check_rows_loaded(context: AssetCheckExecutionContext):
        event = context.instance.get_latest_materialization_event(
            dg.AssetKey(f"loaded_{dataset_name}")
        )
        if event is None:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": "No materialization found"},
            )

        rows_loaded = event.asset_materialization.metadata["rows_loaded"].value

        return AssetCheckResult(
            passed=check_rows_loaded(rows_loaded),
            metadata={"rows_loaded": rows_loaded},
        )

    @asset_check(
        asset=f"loaded_{dataset_name}",
        name="table_row_count",
        description=f"Verify {table_name} row count matches loaded rows",
    )
    def _check_table_row_count(
        context: AssetCheckExecutionContext, duckdb: DuckDBResource
    ):
        event = context.instance.get_latest_materialization_event(
            dg.AssetKey(f"loaded_{dataset_name}")
        )
        if event is None:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": "No materialization found"},
            )

        metadata = event.asset_materialization.metadata
        rows_loaded = metadata["rows_loaded"].value
        season = metadata["season"].value
        gameweek = metadata["gameweek"].value
        gameweek_num = int(gameweek.replace("GW", ""))

        try:
            data_cfg = get_dataset_config(season, dataset_name)
            delete_keys = data_cfg.delete_keys
        except ValueError:
            delete_keys = ["season", "gameweek"]

        where_clause = " AND ".join(f"{key} = ?" for key in delete_keys)
        params = []
        for key in delete_keys:
            if key == "gameweek":
                params.append(gameweek_num)
            elif key == "season":
                params.append(season)

        with duckdb.read_only_connection() as conn:
            result = conn.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}",
                params,
            ).fetchone()
            table_count = result[0] if result else 0

        check_result = check_table_row_count_matches(rows_loaded, table_count)

        return AssetCheckResult(
            passed=check_result["passed"],
            severity=AssetCheckSeverity.ERROR,
            metadata={
                "rows_loaded": rows_loaded,
                "table_count": table_count,
                "season": season,
                "gameweek": gameweek,
            },
        )

    return [_check_rows_loaded, _check_table_row_count]
