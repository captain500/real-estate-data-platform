from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MinIOSettings(BaseSettings):
    """Configuration settings for MinIO object storage."""

    model_config = SettingsConfigDict(env_prefix="MINIO_", case_sensitive=False)

    endpoint: str = Field(default="http://minio:9000")
    access_key: str = Field(default="minioadmin")
    secret_key: SecretStr = Field(default=SecretStr("minioadmin"))
    bucket_name: str = Field(default="raw")


class PostgresSettings(BaseSettings):
    """Configuration settings for PostgreSQL database connection."""

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", case_sensitive=False)

    host: str = Field(default="postgres")
    port: int = Field(default=5432, ge=1024, le=65535)
    user: str = Field(default="etl_user")
    password: SecretStr = Field(default=SecretStr("etl_pass"))
    db: str = Field(default="etl_db")

    @computed_field
    @property
    def dsn(self) -> str:
        """Return the PostgreSQL DSN connection string."""
        return (
            f"postgresql://{self.user}:"
            f"{self.password.get_secret_value()}@"
            f"{self.host}:{self.port}/{self.db}"
        )


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

    environment: str = Field(default="development")
    minio: MinIOSettings = Field(default_factory=MinIOSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    scraper: ScraperSettings = Field(default_factory=ScraperSettings)


settings = Settings()
