from typing import Any

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: str
    REDIS_URL: str

    APP_ENV: str = "development"
    API_CORS_ORIGINS: list[str] = Field(
        default_factory=lambda: [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://localhost:3001",
        ]
    )
    SQL_ECHO: bool = False
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_RECYCLE_SECONDS: int = 1800
    AUTO_CREATE_SCHEMA: bool = True

    TELEGRAM_TOKEN: SecretStr | None = None
    TELEGRAM_CHAT_ID: str | None = None

    JWT_SECRET_KEY: SecretStr = SecretStr("change-me-in-production")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7

    ADMIN_EMAIL: str | None = None
    ADMIN_PASSWORD: SecretStr | None = None

    SMTP_HOST: str | None = None
    SMTP_PORT: int | None = None
    SMTP_USER: str | None = None
    SMTP_PASSWORD: SecretStr | None = None
    SMTP_FROM: str | None = None

    WEB_PUSH_VAPID_PUBLIC_KEY: str | None = None
    WEB_PUSH_VAPID_PRIVATE_KEY: SecretStr | None = None
    WEB_PUSH_VAPID_SUBJECT: str | None = None

    OZON_CLIENT_ID: str | None = None
    OZON_API_KEY: SecretStr | None = None
    OZON_RATE_LIMIT_PER_CLIENT_ID: int = 45
    OZON_BUNDLE_RATE_LIMIT_PER_CLIENT_ID: int = 4
    OZON_STOCKS_RATE_LIMIT_PER_CLIENT_ID: int = 2
    OZON_BUNDLE_MIN_INTERVAL_MS: int = 600
    OZON_STOCKS_MIN_INTERVAL_MS: int = 800
    OZON_STOCKS_RETRY_BASE_SECONDS: int = 3
    OZON_STOCKS_429_COOLDOWN_SECONDS: int = 12
    OZON_STOCKS_5XX_COOLDOWN_SECONDS: int = 20
    OZON_CLUSTER_REFRESH_TTL_SECONDS: int = 21600
    OZON_ONBOARDING_PREVIEW_TTL_SECONDS: int = 900
    OZON_REPORT_POLL_INTERVAL_SECONDS: int = 5
    OZON_REPORT_POLL_TIMEOUT_SECONDS: int = 180
    OZON_REPORT_REUSE_WINDOW_SECONDS: int = 10800
    OZON_REPORT_SNAPSHOT_TTL_SECONDS: int = 172800
    OZON_RATE_LIMIT_ALERT_THRESHOLD_PER_MINUTE: int = 10
    OZON_FINANCE_TRANSACTION_PAGE_SIZE: int = 200
    OZON_FINANCE_TRANSACTION_PAGE_DELAY_MS: int = 400

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @field_validator("API_CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @property
    def is_production(self) -> bool:
        return self.APP_ENV.lower() in {"prod", "production"}

    def validate_runtime_settings(self) -> None:
        if self.is_production and self.JWT_SECRET_KEY.get_secret_value() == "change-me-in-production":
            raise ValueError("JWT_SECRET_KEY must be changed before running in production")
        if self.is_production and self.AUTO_CREATE_SCHEMA:
            raise ValueError("AUTO_CREATE_SCHEMA must be disabled in production")
        if self.is_production and self.ADMIN_EMAIL and not self.ADMIN_PASSWORD:
            raise ValueError("ADMIN_PASSWORD must be explicitly set in production when ADMIN_EMAIL is configured")


settings = Settings()
