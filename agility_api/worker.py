from __future__ import annotations

from .config import get_settings


def main() -> None:
    settings = get_settings()
    print(
        "agility-api worker scaffold ready:",
        {
            "worker_name": settings.worker_name,
            "worker_mode": settings.worker_mode,
            "database_url_configured": bool(settings.database_url),
            "sqlserver_configured": bool(settings.sqlserver_dsn or settings.sqlserver_server),
        },
    )


if __name__ == "__main__":
    main()
