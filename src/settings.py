import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_env: str
    log_level: str
    database_url: str
    inbound_dir: str
    processing_dir: str
    processed_dir: str
    rejected_dir: str
    logs_dir: str
    default_dry_run: bool
    archive_raw_files: bool
    sanitize_sensitive_fields: bool
    hubspot_access_token: str
    hubspot_base_url: str


def _to_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    return Settings(
        app_env=os.getenv("APP_ENV", "development"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        database_url=os.getenv("DATABASE_URL", ""),
        inbound_dir=os.getenv("INBOUND_DIR", "/app/data/inbound"),
        processing_dir=os.getenv("PROCESSING_DIR", "/app/data/processing"),
        processed_dir=os.getenv("PROCESSED_DIR", "/app/data/processed"),
        rejected_dir=os.getenv("REJECTED_DIR", "/app/data/rejected"),
        logs_dir=os.getenv("LOGS_DIR", "/app/data/logs"),
        default_dry_run=_to_bool(os.getenv("DEFAULT_DRY_RUN", "true"), True),
        archive_raw_files=_to_bool(os.getenv("ARCHIVE_RAW_FILES", "true"), True),
        sanitize_sensitive_fields=_to_bool(os.getenv("SANITIZE_SENSITIVE_FIELDS", "true"), True),
        hubspot_access_token=os.getenv("HUBSPOT_ACCESS_TOKEN", ""),
        hubspot_base_url=os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com"),
    )