from pydantic import BaseModel
from typing import Type, Any
from .schemas.matches import MatchesV1, MatchesV2
from .schemas.playermatchstats import PlayerMatchStatsV1, PlayerMatchStatsV2
from .schemas.players import PlayersV1
from .schemas.playerstats import PlayerStatsV1, PlayerStatsV2
from .schemas.teams import TeamsV1, TeamsV2
from .schemas.player_gameweek_stats import PlayerGameweekStatsV1
from .schemas.fixtures import FixturesV1

BASE_URL = (
    "https://raw.githubusercontent.com/olbauday/FPL-Core-Insights/refs/heads/main/data/"
)

SEASON_CONFIG: dict[str, dict[str, dict[str, Type[BaseModel]]]] = {
    "2024-2025": {
        "matches": {
            "url_template": BASE_URL + "{season}/matches/{gameweek}/matches.csv",
            "schema": MatchesV1,
        },
        "playermatchstats": {
            "url_template": BASE_URL
            + "{season}/playermatchstats/{gameweek}/playermatchstats.csv",
            "schema": PlayerMatchStatsV1,
        },
        "players": {
            "url_template": BASE_URL + "{season}/players/players.csv",
            "schema": PlayersV1,
        },
        "playerstats": {
            "url_template": BASE_URL + "{season}/playerstats/playerstats.csv",
            "schema": PlayerStatsV1,
        },
        "teams": {
            "url_template": BASE_URL + "{season}/teams/teams.csv",
            "schema": TeamsV1,
        },
    },
    "2025-2026": {
        "matches": {
            "url_template": BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/matches.csv",
            "schema": MatchesV2,
        },
        "playermatchstats": {
            "url_template": BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/playermatchstats.csv",
            "schema": PlayerMatchStatsV2,
        },
        "players": {
            "url_template": BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/players.csv",
            "schema": PlayersV1,
        },
        "playerstats": {
            "url_template": BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/playerstats.csv",
            "schema": PlayerStatsV2,
        },
        "teams": {
            "url_template": BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/teams.csv",
            "schema": TeamsV2,
        },
        "player_gameweek_stats": {
            "url_template": BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/player_gameweek_stats.csv",
            "schema": PlayerGameweekStatsV1,
        },
        "fixtures": {
            "url_template": BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/fixtures.csv",
            "schema": FixturesV1,
        },
    },
}


def get_dataset_config(season: str, dataset_name: str) -> dict[str, dict[str, Any]]:
    if season not in SEASON_CONFIG:
        raise ValueError(
            f"No configuration found for season '{season}'. Known seasons: {list(SEASON_CONFIG.keys())}"
        )

    if not SEASON_CONFIG[season][dataset_name]:
        raise ValueError(f"No configuration found for dataset '{dataset_name}'.")

    return SEASON_CONFIG[season][dataset_name]
