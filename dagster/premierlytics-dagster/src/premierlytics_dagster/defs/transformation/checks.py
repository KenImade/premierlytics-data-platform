# defs/transformed/checks.py

import dagster as dg
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check
from premierlytics_dagster.helpers.checks import check_not_empty, check_quarantine_rate


def build_transformed_checks(dataset_name: str):
    @asset_check(
        asset=f"transformed_{dataset_name}",
        name="not_empty",
    )
    def _check_not_empty(context: dg.AssetExecutionContext):
        event = context.instance.get_latest_materialization_event(
            dg.AssetKey(f"transformed_{dataset_name}")
        )
        row_count = event.asset_materialization.metadata["row_count"].value
        return AssetCheckResult(
            passed=check_not_empty(row_count),
            metadata={"row_count": row_count},
        )

    @asset_check(
        asset=f"transformed_{dataset_name}",
        name="quarantine_rate_acceptable",
    )
    def _check_quarantine_rate(context: dg.AssetExecutionContext):
        event = context.instance.get_latest_materialization_event(
            dg.AssetKey(f"transformed_{dataset_name}")
        )
        metadata = event.asset_materialization.metadata
        row_count = metadata["row_count"].value
        quarantined = metadata["quarantined_rows"].value

        result = check_quarantine_rate(row_count, quarantined)
        return AssetCheckResult(
            passed=result["passed"],
            severity=AssetCheckSeverity.WARN,
            metadata={"quarantine_rate": f"{result['rate']:.2%}"},
        )

    return [_check_not_empty, _check_quarantine_rate]
