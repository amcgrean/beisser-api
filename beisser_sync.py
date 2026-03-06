"""
Beisser Lumber - SQL Server to Cloud DB Sync Service
Raspberry Pi 4B | Incremental sync using prowid / updated_at columns
"""

import json
import logging
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql
import pyodbc


LOG_DIR = Path(os.getenv("BEISSER_SYNC_LOG_DIR", "/var/log/beisser_sync"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "sync.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("beisser_sync")


STATE_FILE = Path(os.getenv("BEISSER_SYNC_STATE_FILE", "/var/lib/beisser_sync/state.json"))
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)


def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


SQL_SERVER_CONN = os.getenv("SQL_SERVER_CONN", "")

CLOUD_DB_CONN = {
    "host": os.getenv("CLOUD_DB_HOST", ""),
    "port": int(os.getenv("CLOUD_DB_PORT", "5432")),
    "dbname": os.getenv("CLOUD_DB_NAME", ""),
    "user": os.getenv("CLOUD_DB_USER", ""),
    "password": os.getenv("CLOUD_DB_PASSWORD", ""),
    "sslmode": os.getenv("CLOUD_DB_SSLMODE", "require"),
}


TABLE_CONFIGS = [
    {
        "name": "dispatch_orders",
        "cloud_table": "dispatch_orders",
        "source_query": """
            EXEC sp_GetDispatchOrders @LastProwid = {last_prowid}
            -- TODO: Replace with actual SP or:
            -- SELECT * FROM YourDispatchTable WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "dispatch_routes",
        "cloud_table": "dispatch_routes",
        "source_query": """
            SELECT * FROM Routes
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "tag_print_queue",
        "cloud_table": "tag_print_queue",
        "source_query": """
            SELECT * FROM TagPrintQueue
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "sales_orders",
        "cloud_table": "sales_orders",
        "source_query": """
            EXEC sp_GetSalesOrders @LastProwid = {last_prowid}
            -- TODO: Replace with actual SP or direct query
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "sales_order_lines",
        "cloud_table": "sales_order_lines",
        "source_query": """
            SELECT * FROM SalesOrderLines
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "customers",
        "cloud_table": "customers",
        "source_query": """
            SELECT * FROM Customers
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "customer_ar",
        "cloud_table": "customer_ar",
        "source_query": """
            SELECT * FROM CustomerAR
            WHERE updated_at > '{last_updated}'
        """,
        "pk": "prowid",
        "watermark_col": "updated_at",
        "use_prowid": False,
    },
    {
        "name": "inventory",
        "cloud_table": "inventory",
        "source_query": """
            SELECT * FROM Inventory
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "inventory_alerts",
        "cloud_table": "inventory_alerts",
        "source_query": """
            SELECT * FROM InventoryAlerts
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "receiving_checkin",
        "cloud_table": "receiving_checkin",
        "source_query": """
            SELECT * FROM ReceivingCheckIn
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "purchase_orders",
        "cloud_table": "purchase_orders",
        "source_query": """
            SELECT * FROM PurchaseOrders
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
]


def get_source_connection():
    conn = SQL_SERVER_CONN or get_required_env("SQL_SERVER_CONN")
    return pyodbc.connect(conn, timeout=30)


def get_cloud_connection():
    for required in ("host", "dbname", "user", "password"):
        if not CLOUD_DB_CONN.get(required):
            raise RuntimeError(f"Missing required cloud DB setting: {required}")
    return psycopg2.connect(**CLOUD_DB_CONN)


def sync_table(src_cur, cld_cur, config: dict, state: dict) -> int:
    name = config["name"]
    cloud_table = config["cloud_table"]
    pk = config["pk"]
    use_prowid = config["use_prowid"]

    if use_prowid:
        last_val = state.get(name, {}).get("last_prowid", 0)
        query = config["source_query"].format(last_prowid=last_val, last_updated="")
    else:
        last_val = state.get(name, {}).get("last_updated", "1970-01-01T00:00:00")
        query = config["source_query"].format(last_prowid=0, last_updated=last_val)

    log.info("[%s] Syncing from watermark: %s", name, last_val)

    try:
        src_cur.execute(query)
    except Exception as e:
        log.error("[%s] Source query failed: %s", name, e)
        return 0

    rows = src_cur.fetchall()
    if not rows:
        log.info("[%s] No new rows.", name)
        return 0

    columns = [col[0] for col in src_cur.description]
    row_count = 0
    new_watermark = last_val

    insert_cols = [sql.Identifier(c) for c in columns]
    updates = [
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in columns
        if c != pk
    ]
    insert_stmt = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(cloud_table),
        sql.SQL(", ").join(insert_cols),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        sql.Identifier(pk),
        sql.SQL(", ").join(updates),
    )

    for row in rows:
        row_dict = dict(zip(columns, row))

        try:
            cld_cur.execute(insert_stmt, [row_dict[c] for c in columns])
            row_count += 1

            if use_prowid and row_dict.get("prowid", 0) > new_watermark:
                new_watermark = row_dict["prowid"]
            elif not use_prowid and row_dict.get("updated_at"):
                updated_str = str(row_dict["updated_at"])
                if updated_str > str(new_watermark):
                    new_watermark = updated_str

        except Exception as e:
            log.warning("[%s] Upsert failed for row %s: %s", name, row_dict.get(pk), e)

    state.setdefault(name, {})
    if use_prowid:
        state[name]["last_prowid"] = new_watermark
    else:
        state[name]["last_updated"] = new_watermark

    log.info("[%s] Synced %s rows. New watermark: %s", name, row_count, new_watermark)
    return row_count


def main() -> None:
    log.info("=== Beisser Sync Starting ===")
    state = load_state()
    total_rows = 0
    errors = []

    try:
        src_conn = get_source_connection()
        cld_conn = get_cloud_connection()
    except Exception as e:
        log.critical("Connection failed: %s", e)
        return

    src_cur = src_conn.cursor()
    cld_cur = cld_conn.cursor()

    for config in TABLE_CONFIGS:
        try:
            count = sync_table(src_cur, cld_cur, config, state)
            total_rows += count
        except Exception as e:
            log.error("[%s] Unexpected error: %s", config["name"], e)
            errors.append(config["name"])
            cld_conn.rollback()

    cld_conn.commit()
    save_state(state)

    src_cur.close()
    cld_cur.close()
    src_conn.close()
    cld_conn.close()

    log.info("=== Sync Complete | %s rows | %s errors ===", total_rows, len(errors))
    if errors:
        log.warning("Tables with errors: %s", ", ".join(errors))


if __name__ == "__main__":
    main()
