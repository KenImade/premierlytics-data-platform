import dagster as dg
from minio import Minio
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
