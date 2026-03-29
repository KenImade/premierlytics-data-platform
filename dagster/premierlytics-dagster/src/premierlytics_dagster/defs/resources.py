import dagster as dg
import duckdb
from minio import Minio
from contextlib import contextmanager
from collections.abc import Generator
from pathlib import Path
import io
import logging

logger = logging.getLogger(__name__)


class MinioResource(dg.ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    bucket: str

    def _client(self) -> Minio:
        if not hasattr(self, "_minio_client"):
            self._minio_client = Minio(
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
            )
        return self._minio_client

    def put_object(
        self,
        key: str,
        data: str | bytes,
        bucket: str | None = None,
        content_type: str = "text/csv",
    ) -> None:
        """Upload an object (csv, json, parquet, etc) to MinIO."""
        bucket = bucket or self.bucket

        if isinstance(data, str):
            encoded = data.encode("utf-8")
        else:
            encoded = data

        stream = io.BytesIO(encoded)

        try:
            self._client().put_object(
                bucket_name=bucket,
                object_name=key,
                data=stream,
                length=len(encoded),
                content_type=content_type,
            )
            logger.info("Uploaded %d bytes to %s/%s", len(encoded), bucket, key)
        except Exception as e:
            logger.error("Failed to upload to %s/%s: %s", bucket, key, e)
            raise

    def get_object(self, key: str, bucket: str | None = None) -> str:
        """Download an object and return its content as a UTF-8 string."""
        bucket = bucket or self.bucket
        response = self._client().get_object(bucket_name=bucket, object_name=key)
        try:
            return response.read().decode("utf-8")
        except Exception as e:
            logger.error("Failed to retrieve %s from %s: %s", key, bucket, e)
            raise
        finally:
            response.close()
            response.release_conn()

    def get_bytes(self, key: str, bucket: str | None = None) -> bytes:
        """Download an object and return raw bytes (e.g. for parquet)."""
        bucket = bucket or self.bucket
        response = self._client().get_object(bucket_name=bucket, object_name=key)
        try:
            return response.read()
        except Exception as e:
            logger.error("Failed to retrieve %s from %s: %s", key, bucket, e)
            raise
        finally:
            response.close()
            response.release_conn()

    def list_objects(self, prefix: str = "", bucket: str | None = None) -> list[str]:
        """List object keys in a bucket, optionally filtered by prefix."""
        bucket = bucket or self.bucket
        try:
            objects = self._client().list_objects(
                bucket_name=bucket, prefix=prefix, recursive=True
            )
            keys = [obj.object_name for obj in objects]
            logger.info("Retrieved %d objects from bucket: %s", len(keys), bucket)
            return keys
        except Exception as e:
            logger.error("Failed to retrieve objects in %s bucket: %s", bucket, e)
            raise


class DuckDBResource(dg.ConfigurableResource):
    """
    Dagster resource wrapping a DuckDB connection.

    Note: DuckDB enforces a single-writer constraint. Only one connection
    can write at a time. Use Dagster's op_tags with a concurrency key
    to prevent concurrent write operations.

    Read-only connections bypass this constraint and can run concurrently.
    """

    db_path: str = dg.EnvVar("DUCKDB_PATH")  # "/data/duckdb/premierlytics.duckdb"

    def _ensure_directory(self) -> None:
        parent = Path(self.db_path).parent
        if not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
            logger.info("Created directory: %s", parent)

    @contextmanager
    def connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Yields a read-write DuckDB connection."""
        self._ensure_directory()
        conn = duckdb.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def read_only_connection(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Yields a read-only DuckDB connection.

        Safe to use concurrently — not subject to the single-writer constraint.
        """
        conn = duckdb.connect(self.db_path, read_only=True)
        try:
            yield conn
        finally:
            conn.close()
