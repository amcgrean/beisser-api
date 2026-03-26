# Beisser API — Agent Guide

## Repository purpose

This repo contains `beisser_sync.py`, the single Pi-side sync worker that:

1. Pulls incremental changes from the Agility ERP SQL Server (all four active branches).
2. Upserts rows into the Supabase (cloud PostgreSQL) `erp_mirror_*` tables.
3. Geocodes `erp_mirror_cust_shipto` rows using a local GeoJSON file (with optional Nominatim fallback).
4. Records per-batch and per-table sync state in the `erp_sync_*` tracking tables.

---

## Architecture

```
Raspberry Pi 4B
  └── beisser_sync.py  (cron every 10 min)
        ├── pyodbc  → Agility SQL Server (all 4 branches in one DB)
        └── psycopg2 → Supabase PostgreSQL (erp_mirror_* tables)
```

There is **one worker** (`WORKER_NAME = "agility-pi-sync"`). A previously separate
`agility-pi-sync` worker was merged into `beisser_sync.py` in March 2026.

---

## Active branches

| system_id | Location   | Status  |
|-----------|------------|---------|
| 10FD      | Fond du Lac| active  |
| 20GR      | Green Bay  | active  |
| 25BW      | Brodhead   | active  |
| 40CV      | Chilton    | active  |
| 30CD      | —          | defunct |
| 00CO      | Corporate  | master tables only, no orders |

`system_id` is read from the `loc_id` column of each Agility source table — it
is **not** an environment variable. The Pi serves all branches from a single
SQL Server instance.

---

## SQL Server column name notes

These aliases are used in every `source_query` to map Agility names to the
cloud schema. Verify against the live SQL Server schema if queries fail.

| Agility column  | Cloud alias       | Tables                             |
|-----------------|-------------------|------------------------------------|
| `loc_id`        | `system_id`       | all tables                         |
| `so_num`        | `so_id`           | dispatch_orders, so_header, so_detail |
| `seq_num`       | `shipment_num`    | dispatch_orders                    |
| `seq_num`       | `sequence`        | so_detail                          |
| `cust_num`      | `cust_key`        | so_header, cust                    |
| `shipto_seq`    | `shipto_seq_num`  | so_header                          |
| `ent_date`      | `created_date`    | so_header, po_header, receiving_checkin |
| `cust_po_num`   | `po_number`       | so_header                          |
| `po_num`        | `po_id`           | po_header, receiving_checkin       |
| `vend_num`      | `supplier_key`    | po_header                          |
| `recv_seq`      | `receive_num`     | receiving_checkin                  |
| `recv_date`     | `receive_date`    | receiving_checkin                  |
| `delivery_status` | `status_flag_delivery` | dispatch_orders            |
| `route_id`      | `route_id_char`   | dispatch_orders                    |
| `prowid`        | `source_prowid`   | cust_shipto                        |
| `update_date`   | `source_updated_at` | all tables                       |

---

## Sync tables

| Config name        | Source table            | Cloud table                      | Family      |
|--------------------|-------------------------|----------------------------------|-------------|
| customer_shipto    | dbo.cust_shipto         | erp_mirror_cust_shipto           | master      |
| shipments_header   | dbo.dispatch_orders     | erp_mirror_shipments_header      | operational |
| so_header          | dbo.so_header           | erp_mirror_so_header             | operational |
| so_detail          | dbo.so_detail           | erp_mirror_so_detail             | operational |
| customers          | dbo.cust                | erp_mirror_cust                  | master      |
| po_header          | dbo.po_header           | erp_mirror_po_header             | operational |
| receiving_header   | dbo.receiving_checkin   | erp_mirror_receiving_header      | operational |
| print_transaction  | dbo.tag_print_queue     | erp_mirror_print_transaction     | operational |

All tables use `update_date`-based watermark incremental sync (`use_prowid: False`).

---

## Batch tracking tables (Supabase)

| Table                  | Purpose                                          |
|------------------------|--------------------------------------------------|
| `erp_sync_state`       | One row per worker; heartbeat + last status      |
| `erp_sync_batches`     | One row per sync run (UUID batch_id, timing, counts) |
| `erp_sync_table_state` | Per-table watermark, last status, timing         |

`batch_id` is a 32-char hex UUID generated at the start of each `main()` run
and injected as `sync_batch_id` into every upserted row.

---

## inject_columns pattern

`sync_table()` accepts an `inject_columns` dict in each config. Values not
present in the SQL Server result set are merged in before the upsert:

```python
"inject_columns": {
    "synced_at": _now_utc,   # callable — called per row
    "is_deleted": False,      # scalar
}
```

`sync_batch_id` is automatically injected by `sync_table()` and
`sync_customer_shipto()` when `batch_id` is provided.

---

## customer_shipto geocoding

`sync_customer_shipto()` does a full upsert of all shipto fields **and** enriches
rows with `lat`, `lon`, `geocoded_at`, `geocode_source` in the same pass.

Geocoding priority:
1. Exact normalized address match vs. local GeoJSON (`local_geojson_exact`)
2. Fuzzy ZIP-scoped match (`local_geojson_fuzzy_zip`)
3. Fuzzy city/state-scoped match (`local_geojson_fuzzy_city`)
4. Nominatim HTTP fallback — disabled by default (`nominatim`)

Geocoding is skipped for rows whose address is unchanged and already have
coordinates (controlled by `SHIPTO_GEOCODE_REQUIRE_MISSING_ONLY`).

---

## Environment variables

Required:

```
SQL_SERVER_CONN          # full pyodbc ODBC connection string
CLOUD_DB_HOST
CLOUD_DB_NAME
CLOUD_DB_USER
CLOUD_DB_PASSWORD
```

Optional:

```
CLOUD_DB_PORT            # default 5432
CLOUD_DB_SSLMODE         # default require
BEISSER_SYNC_STATE_FILE  # default /var/lib/beisser_sync/state.json
BEISSER_SYNC_LOG_DIR     # default /var/log/beisser_sync
```

Geocoding:

```
SHIPTO_GEOCODE_ENABLED                    # default true
SHIPTO_GEOJSON_PATH                       # path to .geojson or .geojson.gz
SHIPTO_GEOCODE_FALLBACK_NOMINATIM         # default false
SHIPTO_GEOCODE_BATCH_SIZE                 # default 100
SHIPTO_GEOCODE_REQUIRE_MISSING_ONLY       # default true
SHIPTO_GEOCODE_RETRY_FAILED               # default false
SHIPTO_GEOCODE_NOMINATIM_USER_AGENT       # default beisser-api-sync/1.0
SHIPTO_GEOCODE_NOMINATIM_MIN_INTERVAL_SECONDS  # default 1.1
```

> **Note:** `SYSTEM_ID` is no longer used. Branch identity comes from `loc_id`
> in the Agility SQL Server tables.

---

## Pi deployment

The Pi runs the sync as a cron job:

```
*/10 * * * * /usr/bin/python3 /home/pi/beisser-api/beisser_sync.py >> /var/log/beisser_sync/cron.log 2>&1
```

To deploy changes from this repo:

```bash
ssh pi@<pi-hostname>
cd /home/pi/beisser-api
git pull origin main   # or the relevant branch
```

Python dependencies:

```bash
pip install -r requirements.txt
```

---

## Two-worker merger (history)

Before March 2026, two workers ran on the Pi:

- `beisser_sync.py` — geocoding-only, updated `erp_mirror_cust_shipto` geocode columns
- `agility-pi-sync` (separate process) — all other erp_mirror tables

These were merged into a single `beisser_sync.py` that handles both full
upserts and geocoding in one cron job. The `erp_sync_state` row for
`agility-pi-sync` is now updated by `beisser_sync.py`.

---

## Known issues / TODOs

- `loc_id` is the best-guess Agility column name for branch identifier. Verify
  with `SELECT TOP 1 * FROM dbo.dispatch_orders` on the Pi before first run.
- `erp_mirror_aropen` (AR open items) is disabled. Source: `dbo.aropen`,
  unique key: `(system_id, ref_num, ref_num_seq)`. Re-enable once confirmed.
- The `erp_sync_state` row for worker `agility-pi-sync` must exist in Supabase
  before `update_sync_state()` will update it. Insert a seed row if missing:
  ```sql
  INSERT INTO erp_sync_state (worker_name, last_status)
  VALUES ('agility-pi-sync', 'pending')
  ON CONFLICT DO NOTHING;
  ```
