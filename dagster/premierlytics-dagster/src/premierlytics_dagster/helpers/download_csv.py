import logging
import httpx

from dagster import Config, RetryPolicy

logger = logging.getLogger(__name__)


class RetryConfig(Config):
    max_retries: int = 3
    delay_seconds: float = 1.0


class DownloadConfig(Config):
    url: str
    max_retries: int = 3
    delay_seconds: float = 1.0


def build_retry_policy(config: RetryConfig) -> RetryPolicy:
    return RetryPolicy(
        max_retries=config.max_retries,
        delay=config.delay_seconds,
        backoff="EXPONENTIAL",
        jitter="FULL",
    )


def download_csv(url: str):
    """
    Retrieves a CSV file from a specified HTTPS URL.

    Raises error on failure, allowing Dagster's RetryPolicy to handle retries.
    """
    logger.debug(f"Downloading CSV from: {url}")
    with httpx.Client(timeout=10) as client:
        response = client.get(url)
        response.raise_for_status()
    logger.debug(f"Downloaded {len(response.content)} bytes from {url}")
    return response.text
