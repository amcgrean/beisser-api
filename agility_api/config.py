from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env", override=False)


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default


def build_database_url() -> str:
    direct = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN") or os.getenv("TOOLBX_POSTGRES_DSN")
    if direct:
        return direct

    host = os.getenv("PGHOST")
    database = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    port = os.getenv("PGPORT", "5432")
    sslmode = os.getenv("PGSSLMODE", "require")

    if all([host, database, user, password]):
        return f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode={sslmode}"

    return ""
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(slots=True)
class Settings:
    database_url: str = build_database_url()
    sqlserver_dsn: str = os.getenv("SQLSERVER_DSN", "")
    sqlserver_server: str = os.getenv("SQLSERVER_SERVER", "")
    sqlserver_db: str = os.getenv("SQLSERVER_DB", "AgilitySQL")
    sqlserver_user: str = os.getenv("SQLSERVER_USER", "")
    sqlserver_password: str = os.getenv("SQLSERVER_PASSWORD", "")
    sqlserver_driver: str = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server")
    worker_name: str = os.getenv("SYNC_WORKER_NAME", "agility-pi-sync")
    worker_mode: str = os.getenv("SYNC_WORKER_MODE", "pi")
    heartbeat_interval_seconds: int = env_int("SYNC_HEARTBEAT_INTERVAL_SECONDS", 5)
    master_cadence_seconds: int = env_int("SYNC_MASTER_CADENCE_SECONDS", 300)
    operational_cadence_seconds: int = env_int("SYNC_OPERATIONAL_CADENCE_SECONDS", 5)
    ar_cadence_seconds: int = env_int("SYNC_AR_CADENCE_SECONDS", 300)
    document_cadence_seconds: int = env_int("SYNC_DOCUMENT_CADENCE_SECONDS", 300)
    batch_size: int = env_int("SYNC_BATCH_SIZE", 1000)
    staging_schema: str = os.getenv("MIRROR_STAGING_SCHEMA", "public")


def get_settings() -> Settings:
    return Settings()
