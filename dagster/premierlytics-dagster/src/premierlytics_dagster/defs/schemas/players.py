from pydantic import BaseModel, model_validator
from typing import Literal

PositionType = Literal["Goalkeeper", "Defender", "Midfielder", "Forward", "Unknown"]


class PlayersV1(BaseModel):
    player_code: int
    player_id: int
    first_name: str
    second_name: str
    web_name: str
    team_code: int
    position: PositionType

    @model_validator(mode="before")
    @classmethod
    def coerce_empty_strings(cls, values: dict) -> dict:
        return {
            k: None if isinstance(v, str) and v.strip() == "" else v
            for k, v in values.items()
        }
