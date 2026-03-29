from pydantic import BaseModel
from typing import Literal

StrengthRating = Literal["1", "2", "3", "4", "5"]


class TeamsV1(BaseModel):
    code: int
    id: int
    name: str
    short_name: str
    strength: StrengthRating
    strength_overall_home: int
    strength_overall_away: int
    strength_attack_home: int
    strength_attack_away: int
    strength_defence_home: int
    strength_defence_away: int
    pulse_id: int


class TeamsV2(TeamsV1):
    fotmob_name: str
