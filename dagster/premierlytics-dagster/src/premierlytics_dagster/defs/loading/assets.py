import dagster as dg
import io
import polars as pl
import datetime

from ..partitions import matches_partitions
from ..resources import MinioResource, DuckDBResource

from premierlytics_dagster.helpers.sql import load_sql


@dg.asset(partitions_def=matches_partitions, deps=["transformed_fixtures"])
def loaded_fixtures(
    context: dg.AssetExecutionContext, minio: MinioResource, duckdb: DuckDBResource
):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    # Read parquet from MinIO
    transformed_path = f"transformed/{season}/gameweek_{gameweek}/fixtures.parquet"
    parquet_bytes = minio.get_parquet(bucket=minio.bucket, key=transformed_path)

    # Load into DuckDB
    df = pl.read_parquet(io.BytesIO(parquet_bytes))

    # Add additional columns season, ingestion timestamp, etc.
    df = df.with_columns(
        [
            pl.lit(str(season)).alias("season"),
            pl.lit(datetime.datetime.utcnow()).alias("ingested_at"),
        ]
    )

    with duckdb.connection() as conn:
        conn.execute(load_sql("create_fixtures.sql", __file__))

        conn.execute(
            """
            DELETE FROM fixtures
            WHERE season = ? AND gameweek = ?
        """,
            [season, float(gameweek.replace("GW", ""))],
        )

        columns = ", ".join(df.columns)
        conn.execute(
            f"INSERT INTO fixtures_bronze ({columns}) SELECT {columns} FROM df"
        )

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "rows_loaded": len(df),
            "table": "fixtures",
        }
    )
