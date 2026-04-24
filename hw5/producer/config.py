from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    kafka_broker: str = Field(default="kafka:9092")
    kafka_topic: str = Field(default="movie-events")
    kafka_acks: str = Field(default="all")
    kafka_retries: int = Field(default=5, ge=0)
    kafka_retry_backoff_ms: int = Field(default=500, ge=0)

    schema_registry_url: str = Field(default="http://schema-registry:8081")
    schema_path: str = Field(default="/schemas/movie_event.avsc")

    generator_default_sessions: int = Field(default=10, ge=1)
    generator_default_delay_ms: int = Field(default=100, ge=0)

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000, ge=1, le=65535)

    @field_validator("kafka_acks")
    @classmethod
    def validate_acks(cls, v: str) -> str:
        allowed = {"all", "0", "1", "-1"}
        if v not in allowed:
            raise ValueError(f"kafka_acks must be one of {allowed}, got '{v}'")
        return v


settings = Settings()
