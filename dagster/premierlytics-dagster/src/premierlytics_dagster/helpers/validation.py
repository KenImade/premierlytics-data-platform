import polars as pl
from pydantic import BaseModel
from typing import Type, get_origin, get_args, Union


def validate_required_fields(
    df: pl.DataFrame, model: Type[BaseModel]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Validates that required (non-Optional) fields have no null values.

    Returns:
        - valid_df: rows where all required fields are present
        - invalid_df: rows with at least one null required field,
          with a _validation_error column describing the issue
    """
    required_fields = []
    for field_name, field_info in model.model_fields.items():
        if field_name not in df.columns:
            continue

        annotation = field_info.annotation
        origin = get_origin(annotation)
        is_optional = origin is Union and type(None) in get_args(annotation)

        if not is_optional and field_info.is_required():
            required_fields.append(field_name)

    if not required_fields:
        return df, pl.DataFrame(schema={**df.schema, "_validation_error": pl.Utf8})

    null_mask = pl.lit(False)
    for col in required_fields:
        null_mask = null_mask | pl.col(col).is_null()

    error_expr = (
        pl.concat_str(
            [
                pl.when(pl.col(col).is_null())
                .then(pl.lit(f"{col} is null"))
                .otherwise(pl.lit(""))
                for col in required_fields
            ],
            separator=", ",
        )
        .str.replace_all(r"^[, ]+|[, ]+$", "")
        .str.replace_all(r", ,", ",")
    )

    invalid_df = df.filter(null_mask).with_columns(
        error_expr.alias("_validation_error")
    )
    valid_df = df.filter(~null_mask)

    return valid_df, invalid_df
