from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pyodbc

from .config import ROOT, get_settings


@dataclass(slots=True)
class SqlServerConfig:
    server: str
    database: str
    username: str
    password: str
    driver: str
    trust_cert: bool = True
    timeout: int = 30

    def to_connection_string(self) -> str:
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate={'yes' if self.trust_cert else 'no'};"
        )


def load_sqlserver_config() -> SqlServerConfig:
    settings = get_settings()
    if settings.sqlserver_server and settings.sqlserver_user and settings.sqlserver_password:
        return SqlServerConfig(
            server=settings.sqlserver_server,
            database=settings.sqlserver_db,
            username=settings.sqlserver_user,
            password=settings.sqlserver_password,
            driver=settings.sqlserver_driver,
        )

    config_path = ROOT / "db_config.json"
    if not config_path.exists():
        raise RuntimeError(
            "Missing SQL Server config. Set .env SQLSERVER_* values or provide db_config.json."
        )

    with config_path.open(encoding="utf-8") as handle:
        payload = json.load(handle)

    sql_cfg = payload["sql_server"]
    return SqlServerConfig(
        server=sql_cfg["server"],
        database=sql_cfg["database"],
        username=sql_cfg["username"],
        password=sql_cfg["password"],
        driver=sql_cfg.get("driver", "ODBC Driver 17 for SQL Server"),
        trust_cert=sql_cfg.get("trust_cert", True),
        timeout=sql_cfg.get("timeout", 30),
    )


def connect_sqlserver() -> pyodbc.Connection:
    cfg = load_sqlserver_config()
    return pyodbc.connect(cfg.to_connection_string(), timeout=cfg.timeout)
