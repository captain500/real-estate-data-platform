"""MinIO storage backend implementation."""

import io
import json
import logging

import polars as pl
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOStorage:
    """MinIO S3-compatible storage backend."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str = "raw",
        secure: bool = False,
    ):
        """Initialize MinIO storage client.

        Args:
            endpoint: MinIO endpoint (e.g., 'localhost:9000')
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket_name: Base bucket name. Default: raw
            secure: Use HTTPS. Default: False (for local development)
        """
        self.bucket_name = bucket_name

        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

        # Ensure bucket exists
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        """Check if the bucket exists and create it if it doesn't."""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info("Created bucket: %s", self.bucket_name)
        except S3Error:
            logger.exception("Error checking/creating bucket '%s'", self.bucket_name)
            raise

    def _upload(self, buffer: io.BytesIO, object_name: str, content_type: str) -> None:
        """Upload a bytes buffer to MinIO.

        Args:
            buffer: BytesIO buffer with the content to upload
            object_name: S3 object path
            content_type: MIME type of the content
        """
        try:
            size = buffer.getbuffer().nbytes
            buffer.seek(0)
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=buffer,
                length=size,
                content_type=content_type,
            )
            logger.info("Saved %s/%s", self.bucket_name, object_name)
        except S3Error:
            logger.exception("S3 error uploading %s", object_name)
            raise

    def save_parquet(self, dataframe: pl.DataFrame, object_name: str) -> None:
        """Save a Polars DataFrame as Parquet to MinIO.

        Args:
            dataframe: Polars DataFrame to save
            object_name: S3 object path
        """
        buffer = io.BytesIO()
        dataframe.write_parquet(buffer)
        self._upload(buffer, object_name, content_type="application/octet-stream")

    def save_json(self, data: dict, object_name: str) -> None:
        """Save a dictionary as JSON to MinIO.

        Args:
            data: Dictionary to serialize and save
            object_name: S3 object path
        """
        json_bytes = json.dumps(data, default=str).encode("utf-8")
        self._upload(io.BytesIO(json_bytes), object_name, content_type="application/json")

    def list_objects(self, prefix: str, suffix: str | None = None) -> list[str]:
        """List object names under a prefix.

        Args:
            prefix: S3 prefix to filter by (e.g., 'listings/source=kijiji/')
            suffix: Optional file extension filter (e.g., '.parquet')

        Returns:
            List of matching object names
        """
        try:
            objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
            names = [obj.object_name for obj in objects if obj.object_name]
            if suffix:
                names = [n for n in names if n.endswith(suffix)]
            return names
        except S3Error:
            logger.exception("Error listing objects with prefix '%s'", prefix)
            raise

    def read_parquet(self, object_name: str) -> pl.DataFrame:
        """Read a Parquet file from MinIO into a Polars DataFrame.

        Args:
            object_name: S3 object path to the Parquet file

        Returns:
            Polars DataFrame with the file contents
        """
        try:
            response = self.client.get_object(self.bucket_name, object_name)
            try:
                data = response.read()
                return pl.read_parquet(io.BytesIO(data))
            finally:
                response.close()
                response.release_conn()
        except S3Error:
            logger.exception("Error reading parquet '%s'", object_name)
            raise
