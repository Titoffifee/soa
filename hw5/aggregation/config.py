from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    clickhouse_host: str = Field(default="clickhouse")
    clickhouse_port: int = Field(default=8123)
    clickhouse_database: str = Field(default="cinema")

    postgres_host: str = Field(default="postgres")
    postgres_port: int = Field(default=5432)
    postgres_database: str = Field(default="cinema")
    postgres_user: str = Field(default="cinema")
    postgres_password: str = Field(default="cinema")

    s3_endpoint_url: str = Field(default="http://minio:9000")
    s3_access_key: str = Field(default="minioadmin")
    s3_secret_key: str = Field(default="minioadmin")
    s3_bucket: str = Field(default="movie-analytics")

    schedule_interval_seconds: int = Field(default=3600)

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8001)


settings = Settings()
