import io
import logging
import polars as pl
from pydantic import BaseModel
from typing import Type, get_args, get_origin, Union
import datetime

logger = logging.getLogger(__name__)

TYPE_MAP = {
    int: pl.Int64,
    float: pl.Float64,
    str: pl.Utf8,
    bool: pl.Boolean,
    datetime.date: pl.Date,
    datetime.datetime: pl.Datetime,
}

DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def _get_type_mapping(model: Type[BaseModel]) -> dict[str, pl.DataType]:
    mapping = {}
    for field_name, field_info in model.model_fields.items():
        annotation = field_info.annotation
        origin = get_origin(annotation)

        if origin is Union:
            args = [a for a in get_args(annotation) if a is not type(None)]
            annotation = args[0] if args else str

        pl_type = TYPE_MAP.get(annotation)
        if pl_type is None:
            logger.warning(
                "No Polars type mapping for field '%s' with type %s, defaulting to Utf8",
                field_name,
                annotation,
            )
            pl_type = pl.Utf8

        mapping[field_name] = pl_type
    return mapping


def clean_csv(
    csv_data: str | bytes,
    model: Type[BaseModel],
    datetime_format: str = DEFAULT_DATETIME_FORMAT,
) -> pl.DataFrame:
    """
    Cleans a CSV file using a Pydantic model as the schema reference.

    The cleaning steps:
        - Drop columns not defined in the model
        - Log any schema columns missing from the CSV
        - Drop duplicate rows
        - Cast columns to the types defined in the model

    Returns a Polars DataFrame.
    """
    if isinstance(csv_data, str):
        csv_data = csv_data.encode("utf-8")

    df = pl.read_csv(io.BytesIO(csv_data))

    type_mapping = _get_type_mapping(model)

    csv_columns = set(df.columns)
    schema_columns = set(type_mapping.keys())
    extra = csv_columns - schema_columns
    missing = schema_columns - csv_columns

    if extra:
        logger.info("Dropping columns not in schema: %s", extra)
    if missing:
        logger.warning("Schema columns missing from CSV: %s", missing)

    columns_to_keep = [col for col in type_mapping if col in df.columns]
    df = df.select(columns_to_keep)

    df = df.unique()

    cast_exprs = []
    for col, dtype in type_mapping.items():
        if col not in df.columns:
            continue
        if dtype == pl.Datetime:
            cast_exprs.append(
                pl.col(col).str.to_datetime(format=datetime_format, strict=False)
            )
        else:
            cast_exprs.append(pl.col(col).cast(dtype, strict=False))

    df = df.with_columns(cast_exprs)

    expected_columns = [col for col in type_mapping if col in df.columns]
    if df.columns != expected_columns:
        logger.warning(
            "Column order mismatch — expected %s, got %s", expected_columns, df.columns
        )

    return df
