"""MinIO storage backend implementation."""

import io
import json
import logging
from datetime import UTC, datetime

import polars as pl
from minio import Minio
from minio.error import S3Error

from real_estate_data_platform.models.enums import OperationStatus
from real_estate_data_platform.models.listings import RentalsListing
from real_estate_data_platform.models.responses import StorageResult

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

    def _get_object_paths(
        self,
        source: str,
        city: str,
        partition_date: str,
    ) -> tuple[str, str, str]:
        """Generate object paths for the Parquet file and metadata.

        Structure:
        - listings/source={source}/city={city}/dt={date}/listings_{datestamp}.parquet
        - listings/source={source}/city={city}/dt={date}/_metadata.json

        Args:
            source: Data source name
            city: City name
            partition_date: Date string (YYYY-MM-DD)

        Returns:
            Tuple with (parquet_path, metadata_path, datestamp)
        """
        datestamp = partition_date.replace("-", "")
        base_dir = f"listings/source={source}/city={city}/dt={partition_date}"
        parquet_path = f"{base_dir}/listings_{datestamp}.parquet"
        metadata_path = f"{base_dir}/_metadata.json"
        return parquet_path, metadata_path, datestamp

    def save_listings(
        self,
        listings: list[RentalsListing],
        source: str,
        city: str,
        partition_date: str,
        environment: str | None = None,
        extra_metadata: dict | None = None,
    ) -> StorageResult:
        """Save listings to MinIO as Parquet.

        Args:
            listings: List of RentalsListing objects
            source: Data source name (e.g., 'kijiji')
            city: City name
            partition_date: Date string (YYYY-MM-DD)
            environment: Application environment label
            extra_metadata: Optional additional metadata to persist alongside the file

        Returns:
            StorageResult with operation metadata
        """
        if not listings:
            logger.warning("No listings to save, skipping...")
            return StorageResult(
                status=OperationStatus.SKIPPED,
                reason="Empty listings",
                count=0,
            )

        parquet_path, metadata_path, datestamp = self._get_object_paths(
            source, city, partition_date
        )

        metadata = {
            "bucket": self.bucket_name,
            "path": f"{self.bucket_name}/{parquet_path}",
            "record_count": len(listings),
            "source": source,
            "city": city,
            "partition_date": partition_date,
            "datestamp": datestamp,
            "environment": environment,
            "saved_at": datetime.now(UTC).isoformat(),
        }

        if extra_metadata:
            metadata.update({k: v for k, v in extra_metadata.items() if v is not None})

        try:
            # Convert RentalsListing objects to Polars DataFrame
            data = [listing.model_dump() for listing in listings]
            df = pl.DataFrame(data)

            # Write DataFrame to Parquet buffer
            buffer = io.BytesIO()
            df.write_parquet(buffer)
            file_size = buffer.getbuffer().nbytes
            buffer.seek(0)  # Reset pointer to start for upload

            # Upload Parquet file to MinIO
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=parquet_path,
                data=buffer,
                length=file_size,
                content_type="application/octet-stream",
            )

            # Upload metadata JSON alongside the parquet file
            metadata_buffer = io.BytesIO(json.dumps(metadata, default=str).encode("utf-8"))
            metadata_size = metadata_buffer.getbuffer().nbytes
            metadata_buffer.seek(0)

            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=metadata_path,
                data=metadata_buffer,
                length=metadata_size,
                content_type="application/json",
            )

            result = StorageResult(
                status=OperationStatus.SUCCESS,
                path=f"{self.bucket_name}/{parquet_path}",
                metadata_path=f"{self.bucket_name}/{metadata_path}",
                metadata=metadata,
                count=len(listings),
            )

            logger.info(f"Successfully saved {len(listings)} listings to {parquet_path}")
            return result

        except S3Error as e:
            logger.error(f"Error saving listings to MinIO: {e}")
            return StorageResult(
                status=OperationStatus.FAILED,
                path=parquet_path,
                metadata_path=metadata_path,
                reason=str(e),
                count=0,
            )
        except Exception as e:
            logger.error(f"Unexpected error saving listings: {e}")
            return StorageResult(
                status=OperationStatus.FAILED,
                path=parquet_path,
                metadata_path=metadata_path,
                reason=f"Unexpected error: {str(e)}",
                count=0,
            )
