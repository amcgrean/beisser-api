# Beisser Cloud Sync Service

Incremental SQL Server -> Cloud PostgreSQL sync service for Beisser Lumber.

## What this does

- Pulls only new/changed rows from Agility SQL Server.
- Upserts into cloud Postgres tables (`INSERT ... ON CONFLICT DO UPDATE`).
- Tracks per-table watermarks in a local state file.
- Logs to file + stdout for cron visibility.

## Files

- `beisser_sync.py`: main sync worker.
- `requirements.txt`: Python dependencies.

## Environment variables

Set these before running:

- `SQL_SERVER_CONN`: full SQL Server ODBC connection string.
- `CLOUD_DB_HOST`
- `CLOUD_DB_PORT` (default `5432`)
- `CLOUD_DB_NAME`
- `CLOUD_DB_USER`
- `CLOUD_DB_PASSWORD`
- `CLOUD_DB_SSLMODE` (default `require`)

Optional:

- `BEISSER_SYNC_STATE_FILE` (default `/var/lib/beisser_sync/state.json`)
- `BEISSER_SYNC_LOG_DIR` (default `/var/log/beisser_sync`)

## Install

```bash
pip install -r requirements.txt
```

## Run once

```bash
python beisser_sync.py
```

## Cron (every 10 minutes)

```bash
*/10 * * * * /usr/bin/python3 /home/pi/beisser-api/beisser_sync.py >> /var/log/beisser_sync/cron.log 2>&1
```

## Notes

- Replace placeholder queries/stored procedure names in `TABLE_CONFIGS` with production SQL.
- Confirm `prowid`/`updated_at` behavior for each table during the data gap audit.
