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
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.secure = secure

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
                logger.info(f"Created bucket: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error checking/creating bucket: {e}")
            raise

    def save_parquet(
        self,
        dataframe: pl.DataFrame,
        object_name: str,
    ) -> None:
        """Save a Polars DataFrame as Parquet to MinIO.

        Args:
            dataframe: Polars DataFrame to save
            object_name: S3 object path (e.g., 'listings/source=kijiji/city=toronto/dt=2025-02-26/listings_20250226.parquet')
        """
        try:
            # Write DataFrame to Parquet buffer
            buffer = io.BytesIO()
            dataframe.write_parquet(buffer)
            file_size = buffer.getbuffer().nbytes
            buffer.seek(0)

            # Upload Parquet file to MinIO
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=buffer,
                length=file_size,
                content_type="application/octet-stream",
            )

            full_path = f"{self.bucket_name}/{object_name}"
            logger.info(f"Successfully saved Parquet file to {full_path}")

        except S3Error as e:
            logger.error(f"Error saving Parquet file to MinIO: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error saving Parquet file: {e}")
            raise

    def save_json(
        self,
        data: dict,
        object_name: str,
    ) -> None:
        """Save a dictionary as JSON to MinIO.

        Args:
            data: Dictionary to serialize and save
            object_name: S3 object path (e.g., 'listings/source=kijiji/city=toronto/dt=2025-02-26/_metadata.json')
        """
        try:
            # Serialize dict to JSON bytes
            json_bytes = json.dumps(data, default=str).encode("utf-8")
            json_buffer = io.BytesIO(json_bytes)
            file_size = len(json_bytes)

            # Upload JSON file to MinIO
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=json_buffer,
                length=file_size,
                content_type="application/json",
            )

            full_path = f"{self.bucket_name}/{object_name}"
            logger.info(f"Successfully saved JSON file to {full_path}")

        except S3Error as e:
            logger.error(f"Error saving JSON file to MinIO: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error saving JSON file: {e}")
            raise
