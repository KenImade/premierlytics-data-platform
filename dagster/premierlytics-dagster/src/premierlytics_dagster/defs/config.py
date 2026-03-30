from pydantic import BaseModel, ConfigDict
from typing import Type
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


class DatasetConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    url_template: str
    validation_schema: Type[BaseModel]
    add_gameweek_column: bool = False
    rename_columns: dict[str, str] = {}

    @property
    def is_per_gameweek(self) -> bool:
        return "{gameweek}" in self.url_template


SEASON_CONFIG: dict[str, dict[str, DatasetConfig]] = {
    "2024-2025": {
        "matches": DatasetConfig(
            url_template=BASE_URL + "{season}/matches/{gameweek}/matches.csv",
            validation_schema=MatchesV1,
        ),
        "playermatchstats": DatasetConfig(
            url_template=BASE_URL
            + "{season}/playermatchstats/{gameweek}/playermatchstats.csv",
            validation_schema=PlayerMatchStatsV1,
            add_gameweek_column=True,
        ),
        "players": DatasetConfig(
            url_template=BASE_URL + "{season}/players/players.csv",
            validation_schema=PlayersV1,
        ),
        "playerstats": DatasetConfig(
            url_template=BASE_URL + "{season}/playerstats/playerstats.csv",
            validation_schema=PlayerStatsV1,
            rename_columns={"gw": "gameweek"},
        ),
        "teams": DatasetConfig(
            url_template=BASE_URL + "{season}/teams/teams.csv",
            validation_schema=TeamsV1,
        ),
    },
    "2025-2026": {
        "matches": DatasetConfig(
            url_template=BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/matches.csv",
            validation_schema=MatchesV2,
        ),
        "playermatchstats": DatasetConfig(
            url_template=BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/playermatchstats.csv",
            validation_schema=PlayerMatchStatsV2,
            add_gameweek_column=True,
        ),
        "players": DatasetConfig(
            url_template=BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/players.csv",
            validation_schema=PlayersV1,
        ),
        "playerstats": DatasetConfig(
            url_template=BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/playerstats.csv",
            validation_schema=PlayerStatsV2,
            rename_columns={"gw": "gameweek"},
        ),
        "teams": DatasetConfig(
            url_template=BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/teams.csv",
            validation_schema=TeamsV2,
        ),
        "player_gameweek_stats": DatasetConfig(
            url_template=BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/player_gameweek_stats.csv",
            validation_schema=PlayerGameweekStatsV1,
            add_gameweek_column=True,
        ),
        "fixtures": DatasetConfig(
            url_template=BASE_URL
            + "{season}/By Tournament/Premier League/{gameweek}/fixtures.csv",
            validation_schema=FixturesV1,
            add_gameweek_column=True,
        ),
    },
}


def get_dataset_config(season: str, dataset_name: str) -> DatasetConfig:
    if season not in SEASON_CONFIG:
        raise ValueError(
            f"No configuration found for season '{season}'. "
            f"Known seasons: {list(SEASON_CONFIG.keys())}"
        )

    if dataset_name not in SEASON_CONFIG[season]:
        raise ValueError(
            f"No configuration found for dataset '{dataset_name}' "
            f"in season '{season}'. "
            f"Known datasets: {list(SEASON_CONFIG[season].keys())}"
        )

    return SEASON_CONFIG[season][dataset_name]
