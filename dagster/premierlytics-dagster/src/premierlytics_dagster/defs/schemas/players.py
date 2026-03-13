from pydantic import BaseModel
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
