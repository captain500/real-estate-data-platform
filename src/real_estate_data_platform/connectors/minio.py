"""MinIO storage backend implementation."""

import io
import logging

import polars as pl
from minio import Minio
from minio.error import S3Error

from real_estate_data_platform.models.enums import OperationStatus
from real_estate_data_platform.models.listings import RentalsListing
from real_estate_data_platform.models.responses import StorageResult
from real_estate_data_platform.utils.dates import (
    format_timestamp,
)

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

    def _get_object_path(
        self,
        source: str,
        city: str,
        partition_date: str,
    ) -> str:
        """Generate S3 object path with partitioning.

        Structure: listings/source={source}/city={city}/dt={date}/listings_{timestamp}.parquet

        Args:
            source: Data source name
            city: City name
            partition_date: Date string (YYYY-MM-DD)

        Returns:
            Object path in MinIO
        """
        timestamp = format_timestamp()
        return (
            f"listings/source={source}/city={city}/dt={partition_date}/listings_{timestamp}.parquet"
        )

    def save_listings(
        self, listings: list[RentalsListing], source: str, city: str, partition_date: str
    ) -> StorageResult:
        """Save listings to MinIO as Parquet.

        Args:
            listings: List of RentalsListing objects
            source: Data source name (e.g., 'kijiji')
            city: City name
            partition_date: Date string (YYYY-MM-DD)

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

        object_path = self._get_object_path(source, city, partition_date)

        try:
            # Convert RentalsListing objects to Polars DataFrame
            data = [listing.model_dump() for listing in listings]
            df = pl.DataFrame(data)

            # Write DataFrame to Parquet buffer
            buffer = io.BytesIO()
            df.write_parquet(buffer)
            file_size = buffer.getbuffer().nbytes
            buffer.seek(0)  # Reset pointer to start for upload

            # Upload to MinIO
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_path,
                data=buffer,
                length=file_size,
                content_type="application/octet-stream",
            )

            result = StorageResult(
                status=OperationStatus.SUCCESS,
                path=f"{self.bucket_name}/{object_path}",
                count=len(listings),
            )

            logger.info(f"Successfully saved {len(listings)} listings to {object_path}")
            return result

        except S3Error as e:
            logger.error(f"Error saving listings to MinIO: {e}")
            return StorageResult(
                status=OperationStatus.FAILED,
                path=object_path,
                reason=str(e),
                count=0,
            )
        except Exception as e:
            logger.error(f"Unexpected error saving listings: {e}")
            return StorageResult(
                status=OperationStatus.FAILED,
                reason=f"Unexpected error: {str(e)}",
                count=0,
            )
