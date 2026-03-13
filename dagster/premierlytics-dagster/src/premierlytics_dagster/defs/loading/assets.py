import dagster as dg
import io
import polars as pl
import datetime

from ..partitions import matches_partitions
from ..resources import MinioResource, DuckDBResource

from premierlytics_dagster.helpers.sql import load_sql


@dg.asset(
    partitions_def=matches_partitions,
    deps=["transformed_matches"],
    group_name="loaded_data",
    op_tags={"dagster/concurrency_key": "duckdb", "dagster/max_concurrent": 1},
)
def loaded_matches(
    context: dg.AssetExecutionContext, minio: MinioResource, duckdb: DuckDBResource
):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    transformed_path = f"transformed/{season}/gameweek_{gameweek}/matches.parquet"
    parquet_bytes = minio.get_parquet(bucket=minio.bucket, key=transformed_path)

    df = pl.read_parquet(io.BytesIO(parquet_bytes))

    df = df.with_columns(
        [
            pl.lit(str(season)).alias("season"),
            pl.lit(datetime.datetime.utcnow()).alias("ingested_at"),
        ]
    )

    with duckdb.connection() as conn:
        conn.execute(load_sql("create_matches.sql", __file__))

        conn.execute(
            """
            DELETE FROM matches_bronze
            WHERE season = ? AND gameweek = ?
        """,
            [season, float(gameweek.replace("GW", ""))],
        )

        columns = ", ".join(df.columns)
        conn.execute(
            f"""INSERT INTO matches_bronze ({columns}) SELECT {columns} from df"""
        )

    context.log.info(f"Loaded matches data into DuckDB database {duckdb.db_path}")

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "rows_loaded": len(df),
            "table": "matches_bronze",
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["transformed_playermatchstats"],
    group_name="loaded_data",
    op_tags={"dagster/concurrency_key": "duckdb", "dagster/max_concurrent": 1},
)
def loaded_playermatchstats(
    context: dg.AssetExecutionContext, minio: MinioResource, duckdb: DuckDBResource
):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    transformed_path = (
        f"transformed/{season}/gameweek_{gameweek}/playermatchstats.parquet"
    )
    parquet_bytes = minio.get_parquet(bucket=minio.bucket, key=transformed_path)

    # Load into DuckDB
    df = pl.read_parquet(io.BytesIO(parquet_bytes))

    # Add additional columns season, ingestion timestamp, etc.
    df = df.with_columns(
        [
            pl.lit(str(season)).alias("season"),
            pl.lit(int(gameweek.replace("GW", ""))).alias("gameweek"),
            pl.lit(datetime.datetime.utcnow()).alias("ingested_at"),
        ]
    )

    with duckdb.connection() as conn:
        conn.execute(load_sql("create_playermatchstats.sql", __file__))

        conn.execute(
            """
            DELETE FROM playermatchstats_bronze
            WHERE season = ? AND gameweek = ?
        """,
            [season, float(gameweek.replace("GW", ""))],
        )

        columns = ", ".join(df.columns)
        conn.execute(
            f"INSERT INTO playermatchstats_bronze ({columns}) SELECT {columns} FROM df"
        )

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "rows_loaded": len(df),
            "table": "playermatchstats_bronze",
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["transformed_players"],
    group_name="loaded_data",
    op_tags={"dagster/concurrency_key": "duckdb", "dagster/max_concurrent": 1},
)
def loaded_players(
    context: dg.AssetExecutionContext, minio: MinioResource, duckdb: DuckDBResource
):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    transformed_data_path = f"transformed/{season}/gameweek_{gameweek}/players.parquet"

    parquet_bytes = minio.get_parquet(bucket=minio.bucket, key=transformed_data_path)

    df = pl.read_parquet(io.BytesIO(parquet_bytes))

    df = df.with_columns(
        [
            pl.lit(str(season)).alias("season"),
            pl.lit(int(gameweek.replace("GW", ""))).alias("gameweek"),
            pl.lit(datetime.datetime.utcnow()).alias("ingested_at"),
        ]
    )

    with duckdb.connection() as conn:
        conn.execute(load_sql("create_players.sql", __file__))

        conn.execute(
            """
            DELETE FROM players_bronze
            WHERE season = ? AND gameweek = ?
        """,
            [season, int(gameweek.replace("GW", ""))],
        )

        columns = ", ".join(df.columns)

        conn.execute(f"INSERT INTO players_bronze ({columns}) SELECT {columns} FROM df")

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "rows_loaded": len(df),
            "table": "players_bronze",
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["transformed_playerstats"],
    group_name="loaded_data",
    op_tags={"dagster/concurrency_key": "duckdb", "dagster/max_concurrent": 1},
)
def loaded_playerstats(
    context: dg.AssetExecutionContext, minio: MinioResource, duckdb: DuckDBResource
):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    transformed_data_path = (
        f"transformed/{season}/gameweek_{gameweek}/playerstats.parquet"
    )

    parquet_bytes = minio.get_parquet(bucket=minio.bucket, key=transformed_data_path)

    df = pl.read_parquet(io.BytesIO(parquet_bytes))

    # rename gw to gameweek
    df = df.rename({"gw": "gameweek"})

    df = df.with_columns(
        [
            pl.lit(str(season)).alias("season"),
            pl.lit(datetime.datetime.utcnow()).alias("ingested_at"),
        ]
    )

    with duckdb.connection() as conn:
        conn.execute(load_sql("create_playerstats.sql", __file__))

        conn.execute(
            """
            DELETE FROM playerstats_bronze
            WHERE season = ? AND gameweek = ?
        """,
            [season, int(gameweek.replace("GW", ""))],
        )

        columns = ", ".join(df.columns)

        conn.execute(
            f"INSERT INTO playerstats_bronze ({columns}) SELECT {columns} FROM df"
        )

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "rows_loaded": len(df),
            "table": "playerstats_bronze",
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["transformed_teams"],
    group_name="loaded_data",
    op_tags={"dagster/concurrency_key": "duckdb", "dagster/max_concurrent": 1},
)
def loaded_teams(
    context: dg.AssetExecutionContext, minio: MinioResource, duckdb: DuckDBResource
):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    transformed_path = f"transformed/{season}/gameweek_{gameweek}/teams.parquet"
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
        conn.execute(load_sql("create_teams.sql", __file__))

        conn.execute(
            """
            DELETE FROM teams_bronze
            WHERE season = ?
        """,
            [season],
        )

        columns = ", ".join(df.columns)
        conn.execute(f"INSERT INTO teams_bronze ({columns}) SELECT {columns} FROM df")

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "rows_loaded": len(df),
            "table": "teams_bronze",
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["transformed_player_gameweek_stats"],
    group_name="loaded_data",
    op_tags={"dagster/concurrency_key": "duckdb", "dagster/max_concurrent": 1},
)
def loaded_player_gameweek_stats(
    context: dg.AssetExecutionContext, minio: MinioResource, duckdb: DuckDBResource
):
    partitions: dg.MultiPartitionKey = context.partition_key
    season = partitions.keys_by_dimension["season"]
    gameweek = partitions.keys_by_dimension["gameweek"]

    transformed_path = (
        f"transformed/{season}/gameweek_{gameweek}/player_gameweek_stats.parquet"
    )
    parquet_bytes = minio.get_parquet(bucket=minio.bucket, key=transformed_path)

    # Load into DuckDB
    df = pl.read_parquet(io.BytesIO(parquet_bytes))

    # Add additional columns season, ingestion timestamp, etc.
    df = df.with_columns(
        [
            pl.lit(str(season)).alias("season"),
            pl.lit(int(gameweek.replace("GW", ""))).alias("gameweek"),
            pl.lit(datetime.datetime.utcnow()).alias("ingested_at"),
        ]
    )

    with duckdb.connection() as conn:
        conn.execute(load_sql("create_player_gameweek_stats.sql", __file__))

        conn.execute(
            """
            DELETE FROM player_gameweek_stats_bronze
            WHERE season = ? AND gameweek = ?
        """,
            [season, int(gameweek.replace("GW", ""))],
        )

        columns = ", ".join(df.columns)
        conn.execute(
            f"INSERT INTO player_gameweek_stats_bronze ({columns}) SELECT {columns} FROM df"
        )

    context.add_output_metadata(
        {
            "season": season,
            "gameweek": gameweek,
            "rows_loaded": len(df),
            "table": "player_gameweek_stats_bronze",
        }
    )


@dg.asset(
    partitions_def=matches_partitions,
    deps=["transformed_fixtures"],
    group_name="loaded_data",
    op_tags={"dagster/concurrency_key": "duckdb", "dagster/max_concurrent": 1},
)
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
            DELETE FROM fixtures_bronze
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
            "table": "fixtures_bronze",
        }
    )
