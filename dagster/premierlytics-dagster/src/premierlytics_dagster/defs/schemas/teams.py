from pydantic import BaseModel, model_validator
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
    elo: int

    @model_validator(mode="before")
    @classmethod
    def coerce_empty_strings(cls, values: dict) -> dict:
        return {
            k: None if isinstance(v, str) and v.strip() == "" else v
            for k, v in values.items()
        }


class TeamsV2(TeamsV1):
    fotmob_name: str

    @model_validator(mode="before")
    @classmethod
    def coerce_empty_strings(cls, values: dict) -> dict:
        return {
            k: None if isinstance(v, str) and v.strip() == "" else v
            for k, v in values.items()
        }
