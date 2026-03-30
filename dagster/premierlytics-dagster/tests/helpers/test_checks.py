from premierlytics_dagster.helpers.checks import check_not_empty, check_quarantine_rate


class TestCheckNotEmpty:
    def test_passes_with_rows(self):
        assert check_not_empty(10) is True

    def test_fails_with_zero(self):
        assert check_not_empty(0) is False


class TestCheckQuarantineRate:
    def test_passes_under_threshold(self):
        result = check_quarantine_rate(95, 5)
        assert result["passed"] is True

    def test_fails_over_threshold(self):
        result = check_quarantine_rate(90, 11)
        assert result["passed"] is False

    def test_exact_threshold_passes(self):
        result = check_quarantine_rate(95, 5)
        assert result["passed"] is True
        assert result["rate"] == 0.05

    def test_zero_total(self):
        result = check_quarantine_rate(0, 0)
        assert result["passed"] is True

    def test_custom_threshold(self):
        result = check_quarantine_rate(90, 10, threshold=0.15)
        assert result["passed"] is True

    def test_all_quarantined(self):
        result = check_quarantine_rate(0, 100)
        assert result["passed"] is False
        assert result["rate"] == 1.0
