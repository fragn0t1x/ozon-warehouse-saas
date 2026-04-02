from pydantic import SecretStr

from app.config import Settings


def build_settings(**overrides) -> Settings:
    base = {
        "DATABASE_URL": "postgresql+asyncpg://user:pass@localhost/db",
        "REDIS_URL": "redis://localhost:6379/0",
        "APP_ENV": "development",
        "AUTO_CREATE_SCHEMA": True,
        "JWT_SECRET_KEY": SecretStr("dev-secret"),
    }
    base.update(overrides)
    return Settings(**base)


def test_validate_runtime_settings_allows_development_defaults():
    settings = build_settings()

    settings.validate_runtime_settings()


def test_validate_runtime_settings_rejects_default_jwt_secret_in_production():
    settings = build_settings(
        APP_ENV="production",
        AUTO_CREATE_SCHEMA=False,
        JWT_SECRET_KEY=SecretStr("change-me-in-production"),
    )

    try:
        settings.validate_runtime_settings()
        assert False, "Expected ValueError for default JWT secret"
    except ValueError as exc:
        assert "JWT_SECRET_KEY" in str(exc)


def test_validate_runtime_settings_rejects_auto_create_schema_in_production():
    settings = build_settings(
        APP_ENV="production",
        AUTO_CREATE_SCHEMA=True,
        JWT_SECRET_KEY=SecretStr("prod-secret"),
    )

    try:
        settings.validate_runtime_settings()
        assert False, "Expected ValueError for AUTO_CREATE_SCHEMA"
    except ValueError as exc:
        assert "AUTO_CREATE_SCHEMA" in str(exc)


def test_validate_runtime_settings_requires_admin_password_in_production():
    settings = build_settings(
        APP_ENV="production",
        AUTO_CREATE_SCHEMA=False,
        JWT_SECRET_KEY=SecretStr("prod-secret"),
        ADMIN_EMAIL="admin@example.com",
        ADMIN_PASSWORD=None,
    )

    try:
        settings.validate_runtime_settings()
        assert False, "Expected ValueError for missing ADMIN_PASSWORD"
    except ValueError as exc:
        assert "ADMIN_PASSWORD" in str(exc)
