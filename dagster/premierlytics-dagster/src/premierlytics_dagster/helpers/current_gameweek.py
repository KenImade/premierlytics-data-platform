# helpers/current_gameweek.py
import httpx
import logging

logger = logging.getLogger(__name__)

FPL_BOOTSTRAP_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"


def get_current_gameweek() -> int:
    """Fetches the current gameweek from the FPL API."""
    response = httpx.get(FPL_BOOTSTRAP_URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    for event in data["events"]:
        if event["is_current"]:
            return event["id"]

    # If no current event, return the next one (pre-season or between gameweeks)
    for event in data["events"]:
        if event["is_next"]:
            return event["id"]

    raise ValueError("Could not determine current gameweek from FPL API")
