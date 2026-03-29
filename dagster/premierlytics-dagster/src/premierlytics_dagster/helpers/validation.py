import polars as pl
from pydantic import BaseModel, ValidationError
from typing import Type, Tuple


def validate_csv(
    df: pl.DataFrame, model: Type[BaseModel]
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Parses and validates every row in the CSV against the Pydantic model.

    Returns a validated csv file and report on the dataset
    """
    valid_rows = []
    invalid_rows = []

    for row in df.to_dicts():
        try:
            model(**row)
            valid_rows.append(row)
        except ValidationError as e:
            row["_validation_error"] = str(e)
            invalid_rows.append(row)

    valid_df = pl.DataFrame(valid_rows) if valid_rows else pl.DataFrame()
    invalid_df = pl.DataFrame(invalid_rows) if invalid_rows else pl.DataFrame()

    return valid_df, invalid_df
