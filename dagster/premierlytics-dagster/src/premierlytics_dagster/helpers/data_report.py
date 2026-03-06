import io
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.compute as pc


def data_report(csv_text: str) -> dict:
    """
    Generates a metadata report for a CSV dataset.

    Args:
        csv_text: Raw CSV content as a string

    Returns:
        A dict suitable for use as Dagster metadata
    """
    table = pa_csv.read_csv(io.BytesIO(csv_text.encode()))
    return _build_report(table)


def _build_report(table: pa.Table) -> dict:
    total_rows = table.num_rows
    null_summary = {}
    numeric_stats = {}

    for col_name in table.schema.names:
        col = table.column(col_name)

        null_count = col.null_count
        null_summary[col_name] = {
            "null_count": null_count,
            "null_pct": round((null_count / total_rows) * 100, 2) if total_rows else 0,
        }

        if pa.types.is_floating(col.type) or pa.types.is_integer(col.type):
            non_null = col.drop_null()
            if len(non_null) > 0:
                numeric_stats[col_name] = {
                    "min": pc.min(col).as_py(),
                    "max": pc.max(col).as_py(),
                    "mean": round(pc.mean(col).as_py(), 4),
                }

    return {
        "row_count": total_rows,
        "column_count": table.num_columns,
        "null_summary": null_summary,
        "numeric_stats": numeric_stats,
    }
