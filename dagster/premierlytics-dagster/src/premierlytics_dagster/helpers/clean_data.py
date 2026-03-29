import io
import polars as pl
from pydantic import BaseModel
from typing import Type, get_args, get_origin, Union
import datetime


TYPE_MAP = {
    int: pl.Int64,
    float: pl.Float64,
    str: pl.Utf8,
    bool: pl.Boolean,
    datetime.date: pl.Date,
    datetime.datetime: pl.Datetime,
}


def _get_type_mapping(model: Type[BaseModel]) -> dict:
    mapping = {}
    for field_name, field_info in model.model_fields.items():
        annotation = field_info.annotation
        origin = get_origin(annotation)

        if origin is Union:
            args = [a for a in get_args(annotation) if a is not type(None)]
            annotation = args[0] if args else str

        mapping[field_name] = TYPE_MAP.get(annotation, pl.Utf8)
    return mapping


def clean_csv(csv_data: bytes, model: Type[BaseModel]) -> pl.DataFrame:
    """Cleans a CSV file converting datatypes.

    Returns a Polars DataFrame.
    """

    if isinstance(csv_data, str):
        csv_data = csv_data.encode("utf-8")

    df = pl.read_csv(io.BytesIO(csv_data))

    type_mapping = _get_type_mapping(model)

    # Drop columns not in schema
    schema_columns = list(type_mapping.keys())
    df = df.select([col for col in schema_columns if col in df.columns])

    # Cast columns to correct types
    cast_exprs = []
    for col, dtype in type_mapping.items():
        if col not in df.columns:
            continue
        if dtype == pl.Datetime:
            cast_exprs.append(
                pl.col(col).str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False)
            )
        else:
            cast_exprs.append(pl.col(col).cast(dtype, strict=False))

    return df.with_columns(cast_exprs)
