from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str
    rabbitmq_url: str
    api_key: str
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    outbox_batch_size: int = Field(default=100, gt=0)
    outbox_poll_interval: float = Field(default=1.0, gt=0, allow_inf_nan=False)
    outbox_publish_timeout: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    webhook_timeout: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    retry_base_delay: float = Field(default=1.0, gt=0, allow_inf_nan=False)
    consumer_prefetch_count: int = Field(default=5, gt=0)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    @field_validator("log_level", mode="before")
    @classmethod
    def normalize_log_level(cls, value: object) -> object:
        return value.strip().upper() if isinstance(value, str) else value


settings = Settings()  # type: ignore[call-arg]
