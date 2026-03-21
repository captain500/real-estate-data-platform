from enum import StrEnum

from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    """Application environment."""

    DEV = "dev"
    PROD = "prod"


class MinIOSettings(BaseSettings):
    """Configuration settings for MinIO object storage."""

    model_config = SettingsConfigDict(env_prefix="MINIO_", case_sensitive=False)

    endpoint: str = Field(default="localhost:9000")
    access_key: str = Field(default="minioadmin")
    secret_key: SecretStr = Field(default=SecretStr("minioadmin"))
    bucket_name: str = Field(default="raw")


class PostgresSettings(BaseSettings):
    """Configuration settings for PostgreSQL database connection."""

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", case_sensitive=False, extra="ignore")

    host: str = Field(default="postgres")
    port: int = Field(default=5432, ge=1024, le=65535)
    user: str = Field(default="etl_user")
    password: SecretStr = Field(default=SecretStr("etl_pass"))
    db: str = Field(default="etl_db")
    silver_schema: str = Field(default="silver")
    silver_listings_table: str = Field(default="rentals_listings")
    silver_neighbourhoods_table: str = Field(default="neighbourhoods")

    @computed_field
    @property
    def dsn(self) -> str:
        """Return the PostgreSQL DSN connection string."""
        return (
            f"postgresql://{self.user}:"
            f"{self.password.get_secret_value()}@"
            f"{self.host}:{self.port}/{self.db}"
        )


class DbtSettings(BaseSettings):
    """Configuration settings for dbt."""

    model_config = SettingsConfigDict(env_prefix="DBT_", case_sensitive=False)

    project_dir: str = Field(default="src/real_estate_data_platform/dbt")
    profiles_dir: str = Field(default="src/real_estate_data_platform/dbt")


class ScraperSettings(BaseSettings):
    """Configuration settings for web scrapers."""

    model_config = SettingsConfigDict(env_prefix="SCRAPER_", case_sensitive=False)

    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            " (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
    )
    download_delay: float = Field(default=2.0, ge=0)


class Settings(BaseSettings):
    """Main application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
        env_nested_delimiter="__",
    )

    environment: Environment = Field(default=Environment.DEV)
    minio: MinIOSettings = Field(default_factory=MinIOSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    scraper: ScraperSettings = Field(default_factory=ScraperSettings)
    dbt: DbtSettings = Field(default_factory=DbtSettings)


settings = Settings()
