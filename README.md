# Beisser Cloud Sync Service

Incremental SQL Server -> Cloud PostgreSQL sync service for Beisser Lumber.

## What this does

- Pulls only new/changed rows from Agility SQL Server.
- Upserts into cloud Postgres tables (`INSERT ... ON CONFLICT DO UPDATE`).
- Tracks per-table watermarks in a local state file.
- Logs to file + stdout for cron visibility.
- Mirrors customer ship-to rows into `erp_mirror_cust_shipto` with geocode enrichment on the Pi.

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

Ship-to geocoding settings:

- `SHIPTO_GEOCODE_ENABLED` (default `true`)
- `SHIPTO_GEOJSON_PATH` (default empty; e.g. `/home/pi/beisser-sync-data/address_points.geojson.gz`)
- `SHIPTO_GEOCODE_FALLBACK_NOMINATIM` (default `false`)
- `SHIPTO_GEOCODE_BATCH_SIZE` (default `100`)
- `SHIPTO_GEOCODE_REQUIRE_MISSING_ONLY` (default `true`)
- `SHIPTO_GEOCODE_RETRY_FAILED` (default `false`)
- `SHIPTO_GEOCODE_NOMINATIM_USER_AGENT` (default `beisser-api-sync/1.0`)

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

## Ship-to geocoding (Pi-side enrichment)

`beisser_sync.py` now syncs Agility `CustomerShipTo` rows to cloud table `erp_mirror_cust_shipto` and enriches rows with `lat`, `lon`, `geocoded_at`, and `geocode_source` **during the Pi sync pipeline**.

Recommended Pi layout:

1. Place GeoJSON or GeoJSON.gz in a stable local path (for example `/home/pi/beisser-sync-data/address_points.geojson.gz`).
2. Set `SHIPTO_GEOJSON_PATH` to that file.
3. Keep `SHIPTO_GEOCODE_FALLBACK_NOMINATIM=false` unless you explicitly want online fallback.

Matching logic:

1. Exact normalized address match against local GeoJSON.
2. Fuzzy ZIP-scoped match (street-core + house number weighting).
3. Fuzzy city/state-scoped match.
4. Optional Nominatim fallback (if enabled).

`geocode_source` values include:

- `local_geojson_exact`
- `local_geojson_fuzzy_zip`
- `local_geojson_fuzzy_city`
- `nominatim`
- `nominatim_no_result`
- `failed`

Geocoding behavior:

- Geocodes new ship-to rows.
- Re-geocodes when address fields changed.
- Avoids reprocessing unchanged successful rows by default.
- Keeps sync robust by continuing when individual rows fail geocoding/upsert.

## Notes

- Replace placeholder queries/stored procedure names in `TABLE_CONFIGS` with production SQL.
- Confirm `prowid`/`updated_at` behavior for each table during the data gap audit.
- Confirm source table/column names for `CustomerShipTo` in your Agility SQL Server schema and adjust query aliases if needed.
