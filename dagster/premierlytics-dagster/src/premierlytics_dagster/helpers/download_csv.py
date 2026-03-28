import logging
import httpx

from dagster import Config, RetryPolicy

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 30


class DownloadError(Exception):
    """Raise when a CSV download fails."""

    def __init__(self, url: str, cause: Exception):
        self.url = url
        super().__init__(f"Failed to download csv from {url}: {cause}")


class RetryConfig(Config):
    max_retries: int = 3
    delay_seconds: float = 1.0


def build_retry_policy(config: RetryConfig) -> RetryPolicy:
    return RetryPolicy(
        max_retries=config.max_retries,
        delay=config.delay_seconds,
        backoff="EXPONENTIAL",
        jitter="FULL",
    )


def download_csv(url: str, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> str:
    """
    Retrieves a CSV file from a specified HTTPS URL.

    Raises DownloadError on failure, allowing Dagster's RetryPolicy
    to handle retries.
    """
    logger.info("Downloading CSV from: %s", url)
    try:
        with httpx.Client(timeout=timeout_seconds) as client:
            response = client.get(url)
            response.raise_for_status()
            content = response.text
    except httpx.HTTPError as e:
        logger.error("Download failed from %s: %s", url, e)
        raise DownloadError(url, e) from e

    content_type = response.headers.get("content-type", "")
    if "text/csv" not in content_type and "text/plain" not in content_type:
        logger.warning("Unexpected content-type '%s' from %s", content_type, url)

    first_line = content.split("\n", 1)[0]
    if "<html" in first_line.lower() or "<!" in first_line.lower():
        raise DownloadError(
            url,
            ValueError(f"Response appears to be HTML, not CSV: {first_line[:100]}"),
        )

    logger.info("Downloaded %s characters from %s", len(content), url)
    return content
