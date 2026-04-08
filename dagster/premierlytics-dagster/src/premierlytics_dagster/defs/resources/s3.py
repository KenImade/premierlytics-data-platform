import dagster as dg
import boto3
import logging

logger = logging.getLogger(__name__)


class S3Resource(dg.ConfigurableResource):
    bucket: str
    region: str = "eu-west-2"
    endpoint_url: str | None = None  # set for local dev with MinIO

    def _client(self):
        kwargs = {"region_name": self.region}
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        return boto3.client("s3", **kwargs)

    def put_object(
        self,
        key: str,
        data: str | bytes,
        bucket: str | None = None,
        content_type: str = "text/csv",
    ) -> None:
        """Upload an object (csv, json, parquet, etc) to S3."""
        bucket = bucket or self.bucket

        if isinstance(data, str):
            encoded = data.encode("utf-8")
        else:
            encoded = data

        try:
            self._client().put_object(
                Bucket=bucket,
                Key=key,
                Body=encoded,
                ContentType=content_type,
            )
            logger.info("Uploaded %d bytes to s3://%s/%s", len(encoded), bucket, key)
        except Exception as e:
            logger.error("Failed to upload to s3://%s/%s: %s", bucket, key, e)
            raise

    def get_object(self, key: str, bucket: str | None = None) -> str:
        """Download an object and return its content as a UTF-8 string."""
        bucket = bucket or self.bucket
        try:
            response = self._client().get_object(Bucket=bucket, Key=key)
            return response["Body"].read().decode("utf-8")
        except Exception as e:
            logger.error("Failed to retrieve s3://%s/%s: %s", bucket, key, e)
            raise

    def get_bytes(self, key: str, bucket: str | None = None) -> bytes:
        """Download an object and return raw bytes (e.g. for parquet)."""
        bucket = bucket or self.bucket
        try:
            response = self._client().get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except Exception as e:
            logger.error("Failed to retrieve s3://%s/%s: %s", bucket, key, e)
            raise

    def list_objects(self, prefix: str = "", bucket: str | None = None) -> list[str]:
        """List object keys in a bucket, optionally filtered by prefix."""
        bucket = bucket or self.bucket
        try:
            paginator = self._client().get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            logger.info("Retrieved %d objects from s3://%s/%s", len(keys), bucket, prefix)
            return keys
        except Exception as e:
            logger.error("Failed to list objects in s3://%s/%s: %s", bucket, prefix, e)
            raise
