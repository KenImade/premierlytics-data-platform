import polars as pl
from pydantic import BaseModel
from typing import Optional

from premierlytics_dagster.helpers.validation import (
    validate_required_fields,
)


class PlayerModel(BaseModel):
    id: int
    name: str
    team: str
    nickname: Optional[str] = None


class AllOptionalModel(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None


class AllRequiredModel(BaseModel):
    id: int
    name: str
    score: float


class TestValidateRequiredFields:
    def test_all_rows_valid(self):
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["alice", "bob"],
                "team": ["arsenal", "chelsea"],
                "nickname": ["ally", None],
            }
        )
        valid_df, invalid_df = validate_required_fields(df, PlayerModel)
        assert valid_df.shape[0] == 2
        assert invalid_df.shape[0] == 0

    def test_null_required_field_caught(self):
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["alice", None],
                "team": ["arsenal", "chelsea"],
                "nickname": [None, None],
            }
        )
        valid_df, invalid_df = validate_required_fields(df, PlayerModel)
        assert valid_df.shape[0] == 1
        assert invalid_df.shape[0] == 1
        assert "name is null" in invalid_df["_validation_error"][0]

    def test_multiple_null_required_fields(self):
        df = pl.DataFrame(
            {
                "id": [1],
                "name": [None],
                "team": [None],
                "nickname": [None],
            }
        )
        valid_df, invalid_df = validate_required_fields(df, PlayerModel)
        assert valid_df.shape[0] == 0
        assert invalid_df.shape[0] == 1
        error = invalid_df["_validation_error"][0]
        assert "name is null" in error
        assert "team is null" in error

    def test_optional_nulls_not_flagged(self):
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["alice", "bob"],
                "team": ["arsenal", "chelsea"],
                "nickname": [None, None],
            }
        )
        valid_df, invalid_df = validate_required_fields(df, PlayerModel)
        assert valid_df.shape[0] == 2
        assert invalid_df.shape[0] == 0

    def test_all_optional_model(self):
        df = pl.DataFrame(
            {
                "id": [None, 1],
                "name": [None, None],
            }
        )
        valid_df, invalid_df = validate_required_fields(df, AllOptionalModel)
        assert valid_df.shape[0] == 2
        assert invalid_df.shape[0] == 0

    def test_all_required_all_null(self):
        df = pl.DataFrame(
            {
                "id": [None],
                "name": [None],
                "score": [None],
            }
        )
        valid_df, invalid_df = validate_required_fields(df, AllRequiredModel)
        assert valid_df.shape[0] == 0
        assert invalid_df.shape[0] == 1
        error = invalid_df["_validation_error"][0]
        assert "id is null" in error
        assert "name is null" in error
        assert "score is null" in error

    def test_empty_dataframe(self):
        df = pl.DataFrame(
            {
                "id": [],
                "name": [],
                "team": [],
                "nickname": [],
            }
        ).cast({"id": pl.Int64, "name": pl.Utf8, "team": pl.Utf8, "nickname": pl.Utf8})
        valid_df, invalid_df = validate_required_fields(df, PlayerModel)
        assert valid_df.shape[0] == 0
        assert invalid_df.shape[0] == 0

    def test_missing_column_ignored(self):
        df = pl.DataFrame(
            {
                "id": [1],
                "name": ["alice"],
            }
        )
        valid_df, invalid_df = validate_required_fields(df, PlayerModel)
        assert valid_df.shape[0] == 1
        assert invalid_df.shape[0] == 0

    def test_invalid_df_has_error_column(self):
        df = pl.DataFrame(
            {
                "id": [1],
                "name": [None],
                "team": ["arsenal"],
                "nickname": [None],
            }
        )
        _, invalid_df = validate_required_fields(df, PlayerModel)
        assert "_validation_error" in invalid_df.columns

    def test_valid_df_preserves_all_columns(self):
        df = pl.DataFrame(
            {
                "id": [1],
                "name": ["alice"],
                "team": ["arsenal"],
                "nickname": ["ally"],
            }
        )
        valid_df, _ = validate_required_fields(df, PlayerModel)
        assert set(valid_df.columns) == {"id", "name", "team", "nickname"}
