import pytest
import httpx

from premierlytics_dagster.helpers.download_csv import (
    download_csv,
    build_retry_policy,
    DownloadError,
    RetryConfig,
)


def _make_transport(status_code: int, content: str, content_type: str = "text/csv"):
    """Creates a mock HTTP transport returning a fixed response."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=status_code,
            text=content,
            headers={"content-type": content_type},
        )

    return httpx.MockTransport(handler)


@pytest.fixture
def _patch_client(monkeypatch):
    """Returns a function that patches httpx.Client with a mock transport."""

    _OriginalClient = httpx.Client

    def _patch(status_code: int, content: str, content_type: str = "text/csv"):
        transport = _make_transport(status_code, content, content_type)
        monkeypatch.setattr(
            httpx,
            "Client",
            lambda **kwargs: _OriginalClient(transport=transport),
        )

    return _patch


# --- download_csv tests ---


class TestDownloadCsv:
    def test_successful_download(self, _patch_client):
        csv_content = "id,name\n1,alice\n2,bob\n"
        _patch_client(200, csv_content)
        result = download_csv("https://example.com/data.csv")
        assert result == csv_content

    def test_http_error_raises_download_error(self, _patch_client):
        _patch_client(404, "Not Found")
        with pytest.raises(DownloadError) as exc_info:
            download_csv("https://example.com/missing.csv")
        assert "example.com/missing.csv" in str(exc_info.value)

    def test_server_error_raises_download_error(self, _patch_client):
        _patch_client(500, "Internal Server Error")
        with pytest.raises(DownloadError):
            download_csv("https://example.com/data.csv")

    def test_html_response_raises_download_error(self, _patch_client):
        _patch_client(200, "<html><body>Error</body></html>", "text/html")
        with pytest.raises(DownloadError) as exc_info:
            download_csv("https://example.com/data.csv")
        assert "HTML" in str(exc_info.value)

    def test_doctype_html_raises_download_error(self, _patch_client):
        _patch_client(200, "<!DOCTYPE html><html>", "text/html")
        with pytest.raises(DownloadError):
            download_csv("https://example.com/data.csv")

    def test_unexpected_content_type_still_returns(self, _patch_client):
        csv_content = "id,name\n1,alice\n"
        _patch_client(200, csv_content, "application/octet-stream")
        result = download_csv("https://example.com/data.csv")
        assert result == csv_content

    def test_timeout_raises_download_error(self, monkeypatch):
        _OriginalClient = httpx.Client

        def timeout_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectTimeout("Connection timed out")

        transport = httpx.MockTransport(timeout_handler)
        monkeypatch.setattr(
            httpx,
            "Client",
            lambda **kwargs: _OriginalClient(transport=transport),
        )
        with pytest.raises(DownloadError):
            download_csv("https://example.com/data.csv")


# --- build_retry_policy tests ---


class TestBuildRetryPolicy:
    def test_default_config(self):
        policy = build_retry_policy(RetryConfig())
        assert policy.max_retries == 3
        assert policy.delay == 1.0

    def test_custom_config(self):
        policy = build_retry_policy(RetryConfig(max_retries=5, delay_seconds=2.0))
        assert policy.max_retries == 5
        assert policy.delay == 2.0
