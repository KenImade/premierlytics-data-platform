import polars as pl
from pydantic import BaseModel
from typing import Optional
import datetime

from premierlytics_dagster.helpers.clean_data import clean_csv, _get_type_mapping


class SampleModel(BaseModel):
    id: int
    name: str
    score: float
    active: bool


class DateModel(BaseModel):
    id: int
    created_at: datetime.datetime


class OptionalModel(BaseModel):
    id: int
    nickname: Optional[str] = None


class TestCleanCsv:
    def test_basic_cleaning(self):
        csv = "id,name,score,active\n1,alice,9.5,true\n2,bob,8.0,false\n"
        df = clean_csv(csv, SampleModel)
        assert df.shape == (2, 4)
        assert df.schema == {
            "id": pl.Int64,
            "name": pl.Utf8,
            "score": pl.Float64,
            "active": pl.Boolean,
        }

    def test_drops_extra_columns(self):
        csv = "id,name,score,active,extra_col\n1,alice,9.5,true,junk\n"
        df = clean_csv(csv, SampleModel)
        assert "extra_col" not in df.columns
        assert df.shape == (1, 4)

    def test_handles_missing_columns(self):
        csv = "id,name\n1,alice\n"
        df = clean_csv(csv, SampleModel)
        assert set(df.columns) == {"id", "name"}
        assert df.shape == (1, 2)

    def test_drops_duplicates(self):
        csv = "id,name,score,active\n1,alice,9.5,true\n1,alice,9.5,true\n"
        df = clean_csv(csv, SampleModel)
        assert df.shape == (1, 4)

    def test_accepts_bytes(self):
        csv = b"id,name,score,active\n1,alice,9.5,true\n"
        df = clean_csv(csv, SampleModel)
        assert df.shape == (1, 4)

    def test_accepts_string(self):
        csv = "id,name,score,active\n1,alice,9.5,true\n"
        df = clean_csv(csv, SampleModel)
        assert df.shape == (1, 4)

    def test_datetime_casting(self):
        csv = "id,created_at\n1,2025-01-15 10:30:00\n"
        df = clean_csv(csv, DateModel)
        assert df.schema["created_at"] == pl.Datetime
        assert df["created_at"][0] == datetime.datetime(2025, 1, 15, 10, 30, 0)

    def test_custom_datetime_format(self):
        csv = "id,created_at\n1,15/01/2025 10:30\n"
        df = clean_csv(csv, DateModel, datetime_format="%d/%m/%Y %H:%M")
        assert df["created_at"][0] == datetime.datetime(2025, 1, 15, 10, 30, 0)

    def test_invalid_datetime_returns_null(self):
        csv = "id,created_at\n1,not-a-date\n"
        df = clean_csv(csv, DateModel)
        assert df["created_at"][0] is None

    def test_invalid_cast_returns_null(self):
        csv = "id,name,score,active\n1,alice,not_a_number,true\n"
        df = clean_csv(csv, SampleModel)
        assert df["score"][0] is None

    def test_optional_field_handling(self):
        csv = "id,nickname\n1,ally\n2,\n"
        df = clean_csv(csv, OptionalModel)
        assert df.schema["nickname"] == pl.Utf8
        ally_row = df.filter(pl.col("id") == 1)
        assert ally_row["nickname"][0] == "ally"


class TestGetTypeMapping:
    def test_basic_mapping(self):
        mapping = _get_type_mapping(SampleModel)
        assert mapping == {
            "id": pl.Int64,
            "name": pl.Utf8,
            "score": pl.Float64,
            "active": pl.Boolean,
        }

    def test_optional_unwrapping(self):
        mapping = _get_type_mapping(OptionalModel)
        assert mapping["nickname"] == pl.Utf8

    def test_datetime_mapping(self):
        mapping = _get_type_mapping(DateModel)
        assert mapping["created_at"] == pl.Datetime
