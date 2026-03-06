import dagster as dg
from minio import Minio
from minio.error import S3Error
import io


class MinioResource(dg.ConfigurableResource):
    endpoint: str = "localhost:9000"
    access_key: str
    secret_key: str
    secure: bool = False
    bucket: str = "premierlytics-dev-bucket"

    def _client(self) -> Minio:
        return Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def put_object(self, bucket: str, key: str, data: str) -> None:
        """Upload a string (e.g. CSV text) to MinIO."""
        encoded = data.encode("utf-8")
        self._client().put_object(
            bucket_name=bucket,
            object_name=key,
            data=io.BytesIO(encoded),
            length=len(encoded),
            content_type="text/csv",
        )

    def put_parquet(self, bucket: str, key: str, data: bytes) -> None:
        """Upload parquet bytes to MinIO."""
        self._client().put_object(
            bucket_name=bucket,
            object_name=key,
            data=io.BytesIO(data),
            length=len(data),
            content_type="application/octet-stream",
        )

    def get_object(self, bucket: str, key: str) -> str:
        """Download an object and return its content as a string."""
        response = self._client().get_object(bucket_name=bucket, object_name=key)
        try:
            return response.read().decode("utf-8")
        finally:
            response.close()
            response.release_conn()

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        """List object keys in a bucket, optionally filtered by prefix."""
        objects = self._client().list_objects(
            bucket_name=bucket, prefix=prefix, recursive=True
        )
        return [obj.object_name for obj in objects]

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete a single object from a bucket."""
        self._client().remove_object(bucket_name=bucket, object_name=key)
