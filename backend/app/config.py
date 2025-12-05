"""Application configuration using Pydantic Settings."""

from functools import lru_cache
from typing import List

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Application
    app_name: str = "Quant Backtest API"
    debug: bool = False
    api_v1_prefix: str = "/api/v1"

    # Database
    database_url: str
    database_pool_size: int = 20
    database_max_overflow: int = 10

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Security
    secret_key: str
    jwt_secret_key: str = ""
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7
    bcrypt_rounds: int = 12

    # CORS
    cors_origins: List[str] = ["http://localhost:3000", "http://localhost:5173"]

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            import json
            return json.loads(v)
        return v

    # Rate limiting
    api_rate_limit_per_minute: int = 100

    # Backtest settings
    max_concurrent_backtests: int = 10
    backtest_timeout_seconds: int = 300

    @property
    def jwt_key(self) -> str:
        """Get JWT secret key, fallback to main secret key."""
        return self.jwt_secret_key or self.secret_key


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


settings = get_settings()
