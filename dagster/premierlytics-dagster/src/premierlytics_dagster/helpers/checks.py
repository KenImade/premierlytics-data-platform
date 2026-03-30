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
