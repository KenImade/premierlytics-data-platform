import dagster as dg
import io
import polars as pl
import datetime

from ..partitions import matches_partitions
from ..config import get_dataset_config
from ..resources import MinioResource, DuckDBResource
from premierlytics_dagster.helpers.sql import load_sql


def build_loaded_asset(dataset_name: str):
    @dg.asset(
        name=f"loaded_{dataset_name}",
        partitions_def=matches_partitions,
        deps=[f"transformed_{dataset_name}"],
        group_name="loaded_data",
        op_tags={"dagster/concurrency_key": "duckdb", "dagster/max_concurrent": 1},
        description=(
            f"Loads transformed {dataset_name} parquet data from MinIO into "
            f"DuckDB {dataset_name}_bronze table. Uses delete-then-insert "
            f"to ensure idempotent loads. Runs sequentially to respect "
            f"DuckDB's single-writer constraint."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext, minio: MinioResource, duckdb: DuckDBResource
    ) -> None:
        partition: dg.MultiPartitionKey = context.partition_key  # type: ignore[assignment]
        season = partition.keys_by_dimension["season"]
        gameweek = partition.keys_by_dimension["gameweek"]

        try:
            data_cfg = get_dataset_config(season, dataset_name)
        except ValueError:
            context.log.info(
                "No %s config for season %s, skipping", dataset_name, season
            )
            return

        if not data_cfg.is_per_gameweek and gameweek != "GW1":
            context.log.info(
                "Skipping gameweek %s — %s is a single-file dataset for season %s",
                gameweek,
                dataset_name,
                season,
            )
            return

        transformed_path = f"transformed/{season}/{gameweek}/{dataset_name}.parquet"
        parquet_bytes = minio.get_bytes(key=transformed_path)
        df = pl.read_parquet(io.BytesIO(parquet_bytes))

        context.log.info("Retrieved %d rows from MinIO: %s", len(df), transformed_path)

        # Apply column renames from config
        if data_cfg.rename_columns:
            df = df.rename(data_cfg.rename_columns)

        # Add enrichment columns
        gameweek_num = int(gameweek.replace("GW", ""))
        enrichment = [
            pl.lit(season).alias("season"),
            pl.lit(datetime.datetime.now(datetime.timezone.utc)).alias("ingested_at"),
        ]
        if data_cfg.add_gameweek_column:
            enrichment.append(pl.lit(gameweek_num).alias("gameweek"))

        df = df.with_columns(enrichment)

        table_name = f"{dataset_name}_bronze"
        sql_file = f"create_{dataset_name}.sql"

        with duckdb.connection() as conn:
            conn.execute(load_sql(sql_file, __file__))
            conn.execute(
                f"DELETE FROM {table_name} WHERE season = ? AND gameweek = ?",
                [season, gameweek_num],
            )
            columns = ", ".join(df.columns)
            conn.execute(
                f"INSERT INTO {table_name} ({columns}) SELECT {columns} FROM df"
            )

        context.log.info("Loaded %d rows into %s", len(df), table_name)

        context.add_output_metadata(
            {
                "season": season,
                "gameweek": gameweek,
                "rows_loaded": dg.MetadataValue.int(len(df)),
                "table": table_name,
            }
        )

    return _asset


loaded_matches = build_loaded_asset("matches")
loaded_playermatchstats = build_loaded_asset("playermatchstats")
loaded_players = build_loaded_asset("players")
loaded_playerstats = build_loaded_asset("playerstats")
loaded_teams = build_loaded_asset("teams")
loaded_player_gameweek_stats = build_loaded_asset("player_gameweek_stats")
loaded_fixtures = build_loaded_asset("fixtures")
