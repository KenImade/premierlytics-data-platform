# helpers/checks.py


def check_not_empty(row_count: int) -> bool:
    return row_count > 0


def check_quarantine_rate(
    valid_count: int, quarantined_count: int, threshold: float = 0.05
) -> dict:
    total = valid_count + quarantined_count
    if total == 0:
        return {"passed": True, "rate": 0.0}

    rate = quarantined_count / total
    return {"passed": rate <= threshold, "rate": rate}


def check_rows_loaded(rows_loaded: int) -> bool:
    return rows_loaded > 0


def check_table_row_count_matches(rows_loaded: int, table_count: int) -> dict:
    """Verify the rows loaded metadata matches what's actually in the table."""
    return {
        "passed": rows_loaded == table_count,
        "rows_loaded": rows_loaded,
        "table_count": table_count,
    }
