"""
Beisser Lumber - SQL Server to Cloud DB Sync Service
Raspberry Pi 4B | Incremental sync using prowid / updated_at columns
"""

import gzip
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
import pyodbc

load_dotenv()


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


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        log.warning("Invalid integer for %s=%s. Falling back to %s.", name, raw, default)
        return default


SQL_SERVER_CONN = os.getenv("SQL_SERVER_CONN", "")

CLOUD_DB_CONN = {
    "host": os.getenv("CLOUD_DB_HOST", ""),
    "port": int(os.getenv("CLOUD_DB_PORT", "5432")),
    "dbname": os.getenv("CLOUD_DB_NAME", ""),
    "user": os.getenv("CLOUD_DB_USER", ""),
    "password": os.getenv("CLOUD_DB_PASSWORD", ""),
    "sslmode": os.getenv("CLOUD_DB_SSLMODE", "require"),
}


SHIPTO_GEOCODE_SETTINGS = {
    "enabled": env_bool("SHIPTO_GEOCODE_ENABLED", True),
    "geojson_path": os.getenv("SHIPTO_GEOJSON_PATH", ""),
    "fallback_nominatim": env_bool("SHIPTO_GEOCODE_FALLBACK_NOMINATIM", False),
    "batch_size": max(1, env_int("SHIPTO_GEOCODE_BATCH_SIZE", 100)),
    "require_missing_only": env_bool("SHIPTO_GEOCODE_REQUIRE_MISSING_ONLY", True),
    "retry_failed": env_bool("SHIPTO_GEOCODE_RETRY_FAILED", False),
    "nominatim_user_agent": os.getenv(
        "SHIPTO_GEOCODE_NOMINATIM_USER_AGENT", "beisser-api-sync/1.0"
    ),
    "nominatim_min_interval_seconds": max(
        0.0, float(os.getenv("SHIPTO_GEOCODE_NOMINATIM_MIN_INTERVAL_SECONDS", "1.1"))
    ),
}


# ---------------------------------------------------------------------------
# TABLE_CONFIGS — all Agility → erp_mirror_* sync jobs run by this Pi.
#
# Column aliases in source_query map Agility SQL Server names to the cloud
# schema.  Verify these against your SQL Server schema and adjust if needed.
#
# inject_columns adds values not present in the source result set:
#   - system_id  : set SYSTEM_ID env var to your branch code (e.g. '10FD').
#                  If the SQL Server table already exposes a branch/system
#                  column, include it in the SELECT instead and remove it here.
#   - synced_at  : stamped to NOW() on every upsert.
#   - is_deleted : always False on incremental sync (soft-delete logic TBD).
# ---------------------------------------------------------------------------

TABLE_CONFIGS = [
    # ------------------------------------------------------------------
    # customer_shipto: full upsert + Pi-side geocoding enrichment.
    # Master table — system_id injected as '00CO' (corporate); SELECT *
    # with column mapping handled by transform_shipto_row().
    # ------------------------------------------------------------------
    {
        "name": "customer_shipto",
        "cloud_table": "erp_mirror_cust_shipto",
        "family": "master",
        "source_query": """
            SELECT *
            FROM dbo.cust_shipto
            WHERE update_date > '{last_updated}'
        """,
        "pk": ["system_id", "cust_key", "seq_num"],
        "watermark_col": "source_updated_at",
        "use_prowid": False,
        "custom_sync": "customer_shipto",
        "inject_columns": {
            "system_id": "00CO",
            "synced_at": _now_utc,
            "is_deleted": False,
        },
    },
    # ------------------------------------------------------------------
    # shipments_header: dbo.dispatch_orders → erp_mirror_shipments_header
    # KEY FIX: so_num aliased to so_id; loc_id provides system_id.
    # ------------------------------------------------------------------
    {
        "name": "shipments_header",
        "cloud_table": "erp_mirror_shipments_header",
        "family": "operational",
        "source_query": """
            SELECT
                loc_id          AS system_id,
                so_num          AS so_id,
                seq_num         AS shipment_num,
                ship_date,
                billed_flag,
                status_flag,
                route_id        AS route_id_char,
                print_status,
                invoice_date,
                expect_date,
                loaded_date,
                loaded_time,
                driver,
                delivery_status AS status_flag_delivery,
                ship_via,
                update_date     AS source_updated_at
            FROM dbo.dispatch_orders
            WHERE update_date > '{last_updated}'
        """,
        "pk": ["system_id", "so_id", "shipment_num"],
        "watermark_col": "source_updated_at",
        "use_prowid": False,
        "inject_columns": {
            "synced_at": _now_utc,
            "is_deleted": False,
        },
    },
    # ------------------------------------------------------------------
    # so_header: dbo.so_header → erp_mirror_so_header
    # ------------------------------------------------------------------
    {
        "name": "so_header",
        "cloud_table": "erp_mirror_so_header",
        "family": "operational",
        "source_query": """
            SELECT
                loc_id          AS system_id,
                so_num          AS so_id,
                so_status,
                sale_type,
                cust_num        AS cust_key,
                shipto_seq      AS shipto_seq_num,
                reference,
                expect_date,
                ent_date        AS created_date,
                invoice_date,
                ship_date,
                promise_date,
                ship_via,
                terms,
                salesperson,
                cust_po_num     AS po_number,
                branch_code,
                update_date     AS source_updated_at
            FROM dbo.so_header
            WHERE update_date > '{last_updated}'
        """,
        "pk": ["system_id", "so_id"],
        "watermark_col": "source_updated_at",
        "use_prowid": False,
        "inject_columns": {
            "synced_at": _now_utc,
            "is_deleted": False,
        },
    },
    # ------------------------------------------------------------------
    # so_detail: dbo.so_detail → erp_mirror_so_detail
    # ------------------------------------------------------------------
    {
        "name": "so_detail",
        "cloud_table": "erp_mirror_so_detail",
        "family": "operational",
        "source_query": """
            SELECT
                loc_id          AS system_id,
                so_num          AS so_id,
                seq_num         AS sequence,
                item_ptr,
                qty_ord         AS qty_ordered,
                size_,
                so_desc,
                price,
                price_uom_ptr,
                bo,
                update_date     AS source_updated_at
            FROM dbo.so_detail
            WHERE update_date > '{last_updated}'
        """,
        "pk": ["system_id", "so_id", "sequence"],
        "watermark_col": "source_updated_at",
        "use_prowid": False,
        "inject_columns": {
            "synced_at": _now_utc,
            "is_deleted": False,
        },
    },
    # ------------------------------------------------------------------
    # customers: dbo.cust → erp_mirror_cust
    # Master table — system_id injected as '00CO' (corporate).
    # ------------------------------------------------------------------
    {
        "name": "customers",
        "cloud_table": "erp_mirror_cust",
        "family": "master",
        "source_query": """
            SELECT
                cust_num        AS cust_key,
                cust_code,
                cust_name,
                phone,
                email,
                balance,
                credit_limit,
                credit_account,
                cust_type,
                branch_code,
                update_date     AS source_updated_at
            FROM dbo.cust
            WHERE update_date > '{last_updated}'
        """,
        "pk": ["system_id", "cust_key"],
        "watermark_col": "source_updated_at",
        "use_prowid": False,
        "inject_columns": {
            "system_id": "00CO",
            "synced_at": _now_utc,
            "is_deleted": False,
        },
    },
    # ------------------------------------------------------------------
    # po_header: dbo.po_header → erp_mirror_po_header
    # ------------------------------------------------------------------
    {
        "name": "po_header",
        "cloud_table": "erp_mirror_po_header",
        "family": "operational",
        "source_query": """
            SELECT
                loc_id              AS system_id,
                po_num              AS po_id,
                purchase_type,
                vend_num            AS supplier_key,
                shipfrom_seq,
                order_date,
                expect_date,
                due_date,
                buyer,
                reference,
                ship_via,
                current_receive_no,
                po_status,
                canceled,
                wms_status,
                received_manually,
                mwt_recv_complete,
                mwt_recv_complete_datetime,
                ent_date            AS created_date,
                update_date         AS source_updated_at
            FROM dbo.po_header
            WHERE update_date > '{last_updated}'
        """,
        "pk": ["system_id", "po_id"],
        "watermark_col": "source_updated_at",
        "use_prowid": False,
        "inject_columns": {
            "synced_at": _now_utc,
            "is_deleted": False,
        },
    },
    # ------------------------------------------------------------------
    # receiving_header: dbo.receiving_checkin → erp_mirror_receiving_header
    # ------------------------------------------------------------------
    {
        "name": "receiving_header",
        "cloud_table": "erp_mirror_receiving_header",
        "family": "operational",
        "source_query": """
            SELECT
                loc_id          AS system_id,
                po_num          AS po_id,
                recv_seq        AS receive_num,
                recv_date       AS receive_date,
                recv_status,
                packing_slip,
                wms_user,
                wms_dispatch_id,
                recv_comment,
                ent_date        AS created_date,
                update_date     AS source_updated_at
            FROM dbo.receiving_checkin
            WHERE update_date > '{last_updated}'
        """,
        "pk": ["system_id", "po_id", "receive_num"],
        "watermark_col": "source_updated_at",
        "use_prowid": False,
        "inject_columns": {
            "synced_at": _now_utc,
            "is_deleted": False,
        },
    },
    # ------------------------------------------------------------------
    # print_transaction: dbo.tag_print_queue → erp_mirror_print_transaction
    # ------------------------------------------------------------------
    {
        "name": "print_transaction",
        "cloud_table": "erp_mirror_print_transaction",
        "family": "operational",
        "source_query": """
            SELECT
                loc_id          AS system_id,
                tran_id,
                tran_type,
                created_at,
                update_date     AS source_updated_at
            FROM dbo.tag_print_queue
            WHERE update_date > '{last_updated}'
        """,
        "pk": ["tran_id", "tran_type"],
        "watermark_col": "source_updated_at",
        "use_prowid": False,
        "inject_columns": {
            "synced_at": _now_utc,
            "is_deleted": False,
        },
    },
    # customer_ar / erp_mirror_aropen: disabled — the erp_mirror_aropen table
    # uses a composite unique key (system_id, ref_num, ref_num_seq).
    # Source: dbo.aropen, watermark: update_date.
]


WORKER_NAME = "agility-pi-sync"


def _now_utc() -> datetime:
    """Return current UTC time without tzinfo (for Postgres TIMESTAMP WITHOUT TIME ZONE)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Sync state / batch tracking (mirrors the agility-pi-sync infrastructure)
# ---------------------------------------------------------------------------

def start_batch(cld_cur, batch_id: str) -> None:
    cld_cur.execute(
        """
        INSERT INTO erp_sync_batches (batch_id, worker_name, started_at, status, family)
        VALUES (%s, %s, %s, 'running', 'mixed')
        """,
        [batch_id, WORKER_NAME, _now_utc()],
    )


def finish_batch(
    cld_cur,
    batch_id: str,
    status: str,
    rows_extracted: int,
    rows_upserted: int,
    duration_ms: int,
    error: Optional[str] = None,
) -> None:
    cld_cur.execute(
        """
        UPDATE erp_sync_batches
        SET finished_at = %s, status = %s, rows_extracted = %s,
            rows_upserted = %s, duration_ms = %s, error_message = %s
        WHERE batch_id = %s
        """,
        [_now_utc(), status, rows_extracted, rows_upserted, duration_ms, error, batch_id],
    )


def update_table_state(
    cld_cur,
    table_name: str,
    family: str,
    batch_id: str,
    status: str,
    row_count: int,
    watermark,
    duration_ms: int,
    error: Optional[str] = None,
) -> None:
    now = _now_utc()
    success_at = now if status == "success" else None
    error_at = now if status == "error" else None
    watermark_dt: Optional[datetime] = None
    if watermark and watermark != "1970-01-01T00:00:00":
        try:
            watermark_dt = datetime.fromisoformat(str(watermark).replace("T", " ").split(".")[0])
        except (ValueError, TypeError):
            pass

    cld_cur.execute(
        """
        UPDATE erp_sync_table_state
        SET last_batch_id = %s, last_status = %s,
            last_success_at = COALESCE(%s, last_success_at),
            last_error_at   = COALESCE(%s, last_error_at),
            last_error = %s, last_source_updated_at = COALESCE(%s, last_source_updated_at),
            last_row_count = %s, last_duration_ms = %s
        WHERE table_name = %s
        """,
        [batch_id, status, success_at, error_at, error, watermark_dt,
         row_count, duration_ms, table_name],
    )
    if cld_cur.rowcount == 0:
        cld_cur.execute(
            """
            INSERT INTO erp_sync_table_state
                (table_name, family, strategy, last_batch_id, last_status,
                 last_success_at, last_error_at, last_error,
                 last_source_updated_at, last_row_count, last_duration_ms)
            VALUES (%s, %s, 'incremental', %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [table_name, family, batch_id, status, success_at, error_at,
             error, watermark_dt, row_count, duration_ms],
        )


def update_sync_state(cld_cur, status: str, total_rows: int, errors: List[str]) -> None:
    now = _now_utc()
    counts = json.dumps({"rows_upserted": total_rows})
    error_str = f"Errors in: {', '.join(errors)}" if errors else None
    success_at = now if not errors else None
    error_at = now if errors else None
    cld_cur.execute(
        """
        UPDATE erp_sync_state
        SET last_heartbeat_at = %s, last_status = %s, last_counts_json = %s,
            last_error = %s,
            last_success_at = COALESCE(%s, last_success_at),
            last_error_at   = COALESCE(%s, last_error_at)
        WHERE worker_name = %s
        """,
        [now, status, counts, error_str, success_at, error_at, WORKER_NAME],
    )


def get_source_connection():
    conn = SQL_SERVER_CONN or get_required_env("SQL_SERVER_CONN")
    return pyodbc.connect(conn, timeout=30)


def get_cloud_connection():
    for required in ("host", "dbname", "user", "password"):
        if not CLOUD_DB_CONN.get(required):
            raise RuntimeError(f"Missing required cloud DB setting: {required}")
    return psycopg2.connect(**CLOUD_DB_CONN)


def normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    compacted = re.sub(r"\s+", " ", str(value).strip().lower())
    return re.sub(r"[^a-z0-9 ]", "", compacted)


def normalize_zip(value: Optional[str]) -> str:
    if not value:
        return ""
    digits = re.sub(r"[^0-9]", "", str(value))
    return digits[:5]


def split_house_and_street(address_1: Optional[str]) -> Tuple[str, str]:
    text = normalize_text(address_1)
    if not text:
        return "", ""
    parts = text.split(" ")
    house = parts[0] if parts and parts[0].isdigit() else ""
    core = " ".join(parts[1:] if house else parts)
    core = re.sub(
        r"\b(street|st|avenue|ave|road|rd|drive|dr|lane|ln|court|ct|boulevard|blvd|highway|hwy)\b",
        "",
        core,
    )
    core = re.sub(r"\s+", " ", core).strip()
    return house, core


def build_address_key(row: dict) -> str:
    house, street_core = split_house_and_street(row.get("address_1"))
    return "|".join(
        [
            house,
            street_core,
            normalize_text(row.get("city")),
            normalize_text(row.get("state")),
            normalize_zip(row.get("zip")),
        ]
    )


class ShipToGeocoder:
    def __init__(self, settings: dict):
        self.settings = settings
        self.enabled = settings["enabled"]
        self.fallback_nominatim = settings["fallback_nominatim"]
        self.nominatim_user_agent = settings["nominatim_user_agent"]
        self.nominatim_min_interval_seconds = settings["nominatim_min_interval_seconds"]
        self._last_nominatim_request_ts = 0.0

        self.by_exact: Dict[str, dict] = {}
        self.by_zip: Dict[str, List[dict]] = {}
        self.by_city_state: Dict[str, List[dict]] = {}

        if self.enabled:
            self._load_geojson()

    def _load_geojson(self) -> None:
        path = self.settings.get("geojson_path")
        if not path:
            log.info("Ship-to geocoding enabled, but SHIPTO_GEOJSON_PATH is empty.")
            return

        geo_path = Path(path)
        if not geo_path.exists():
            log.warning("GeoJSON file not found at %s", geo_path)
            return

        open_func = gzip.open if geo_path.suffix == ".gz" else open
        loaded = 0

        try:
            with open_func(geo_path, "rt", encoding="utf-8") as handle:
                for feature in self._iter_geojson_features(handle):
                    coords = feature.get("geometry", {}).get("coordinates", [])
                    if not isinstance(coords, Sequence) or len(coords) < 2:
                        continue
                    lon, lat = coords[0], coords[1]
                    if lat is None or lon is None:
                        continue

                    props = {str(k).lower(): v for k, v in (feature.get("properties") or {}).items()}

                    # Build full street address from number + street if needed
                    addr = props.get("address_1") or props.get("address")
                    if not addr:
                        street = props.get("street", "")
                        number = props.get("number", "")
                        addr = f"{number} {street}".strip() if number else street

                    normalized = {
                        "address_1": addr,
                        "city": props.get("city"),
                        "state": props.get("state") or props.get("region"),
                        "zip": props.get("zip") or props.get("postal_code") or props.get("postcode"),
                        "lat": float(lat),
                        "lon": float(lon),
                    }

                    address_key = build_address_key(normalized)
                    if address_key and address_key not in self.by_exact:
                        self.by_exact[address_key] = normalized

                    zip_key = normalize_zip(normalized.get("zip"))
                    city_state_key = "|".join(
                        [normalize_text(normalized.get("city")), normalize_text(normalized.get("state"))]
                    )

                    if zip_key:
                        self.by_zip.setdefault(zip_key, []).append(normalized)
                    if city_state_key != "|":
                        self.by_city_state.setdefault(city_state_key, []).append(normalized)

                    loaded += 1

            log.info("Loaded %s geojson address candidates from %s", loaded, geo_path)
        except Exception as exc:
            log.warning("Failed to parse geojson file %s: %s", geo_path, exc)

    def _iter_geojson_features(self, handle) -> Iterable[dict]:
        try:
            parsed = json.load(handle)
        except Exception:
            # Fall back to NDJSON (newline-delimited JSON) streaming.
            # Handles features that span multiple lines (embedded newlines
            # in property values) by accumulating lines until a complete
            # JSON object is parsed.
            try:
                handle.seek(0)
            except Exception:
                return
            buffer = ""
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                buffer += line
                try:
                    item = json.loads(buffer)
                except json.JSONDecodeError:
                    continue
                buffer = ""
                if isinstance(item, dict):
                    yield item
            return

        if isinstance(parsed, dict):
            features = parsed.get("features")
            if isinstance(features, list):
                for feature in features:
                    if isinstance(feature, dict):
                        yield feature
                return
            if parsed.get("type") == "Feature":
                yield parsed
                return
        if isinstance(parsed, list):
            for feature in parsed:
                if isinstance(feature, dict):
                    yield feature
            return

    def _fuzzy_match(self, target: dict, candidates: Iterable[dict]) -> Optional[dict]:
        target_house, target_street = split_house_and_street(target.get("address_1"))
        if not target_street:
            return None

        best = None
        best_score = 0.0
        for candidate in candidates:
            cand_house, cand_street = split_house_and_street(candidate.get("address_1"))
            if not cand_street:
                continue

            similarity = SequenceMatcher(None, target_street, cand_street).ratio()
            if target_house and cand_house == target_house:
                similarity += 0.1

            if similarity > best_score:
                best_score = similarity
                best = candidate

        return best if best is not None and best_score >= 0.80 else None

    def _query_nominatim(self, row: dict) -> Tuple[Optional[float], Optional[float], str]:
        if not self.fallback_nominatim:
            return None, None, "failed"

        query = ", ".join(
            p
            for p in [
                row.get("address_1", ""),
                row.get("city", ""),
                row.get("state", ""),
                row.get("zip", ""),
            ]
            if p
        )
        if not query:
            return None, None, "failed"

        params = urlencode({"q": query, "format": "json", "limit": 1, "addressdetails": 0})
        url = f"https://nominatim.openstreetmap.org/search?{params}"

        try:
            if self.nominatim_min_interval_seconds > 0:
                elapsed = time.monotonic() - self._last_nominatim_request_ts
                wait_for = self.nominatim_min_interval_seconds - elapsed
                if wait_for > 0:
                    time.sleep(wait_for)
            request = Request(url, headers={"User-Agent": self.nominatim_user_agent})
            response = urlopen(request, timeout=10)
            payload = response.read().decode("utf-8")
            data = json.loads(payload)
            if not data:
                return None, None, "nominatim_no_result"
            item = data[0]
            return float(item["lat"]), float(item["lon"]), "nominatim"
        except Exception as exc:
            log.warning("Nominatim geocode failed for '%s': %s", query, exc)
            return None, None, "failed"
        finally:
            self._last_nominatim_request_ts = time.monotonic()

    def geocode(self, row: dict) -> Tuple[Optional[float], Optional[float], str]:
        if not self.enabled:
            return None, None, "failed"

        exact = self.by_exact.get(build_address_key(row))
        if exact:
            return exact["lat"], exact["lon"], "local_geojson_exact"

        zip_candidates = self.by_zip.get(normalize_zip(row.get("zip")), [])
        zip_match = self._fuzzy_match(row, zip_candidates)
        if zip_match:
            return zip_match["lat"], zip_match["lon"], "local_geojson_fuzzy_zip"

        city_state_key = "|".join(
            [normalize_text(row.get("city")), normalize_text(row.get("state"))]
        )
        city_candidates = self.by_city_state.get(city_state_key, [])
        city_match = self._fuzzy_match(row, city_candidates)
        if city_match:
            return city_match["lat"], city_match["lon"], "local_geojson_fuzzy_city"

        return self._query_nominatim(row)


def sync_table(src_cur, cld_cur, config: dict, state: dict, batch_id: Optional[str] = None) -> int:
    name = config["name"]
    cloud_table = config["cloud_table"]
    pk_config = config["pk"]
    pk_columns = [pk_config] if isinstance(pk_config, str) else pk_config
    use_prowid = config["use_prowid"]
    inject = dict(config.get("inject_columns", {}))
    if batch_id:
        inject.setdefault("sync_batch_id", batch_id)
    table_start = _now_utc()

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
    # Build the full column list: source columns plus any injected columns not already present.
    all_columns = list(columns) + [c for c in inject if c not in columns]
    row_count = 0
    new_watermark = last_val

    insert_cols = [sql.Identifier(c) for c in all_columns]
    updates = [
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in all_columns
        if c not in pk_columns
    ]
    insert_stmt = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(cloud_table),
        sql.SQL(", ").join(insert_cols),
        sql.SQL(", ").join(sql.Placeholder() for _ in all_columns),
        sql.SQL(", ").join(sql.Identifier(c) for c in pk_columns),
        sql.SQL(", ").join(updates),
    )

    for row in rows:
        row_dict = dict(zip(columns, row))

        # Merge injected column values. Source values take precedence if already present.
        for col, val in inject.items():
            if col not in row_dict:
                row_dict[col] = val() if callable(val) else val

        try:
            cld_cur.execute(insert_stmt, [row_dict.get(c) for c in all_columns])
            row_count += 1

            if use_prowid and row_dict.get("prowid", 0) > new_watermark:
                new_watermark = row_dict["prowid"]
            elif not use_prowid:
                wm_col = config.get("watermark_col", "update_date")
                wm_val = row_dict.get(wm_col) or row_dict.get("update_date")
                if wm_val is not None:
                    wm_str = str(wm_val)
                    if wm_str > str(new_watermark):
                        new_watermark = wm_str

        except Exception as e:
            row_key = ":".join(str(row_dict.get(pk)) for pk in pk_columns)
            log.warning("[%s] Upsert failed for row %s: %s", name, row_key, e)

    state.setdefault(name, {})
    if use_prowid:
        state[name]["last_prowid"] = new_watermark
    else:
        state[name]["last_updated"] = new_watermark

    duration_ms = int((_now_utc() - table_start).total_seconds() * 1000)
    if batch_id:
        try:
            update_table_state(
                cld_cur, cloud_table, config.get("family", "operational"),
                batch_id, "success", row_count, new_watermark, duration_ms,
            )
        except Exception as exc:
            log.warning("[%s] Failed to update table state: %s", name, exc)

    log.info("[%s] Synced %s rows. New watermark: %s", name, row_count, new_watermark)
    return row_count


def ensure_shipto_schema(cld_cur) -> None:
    """Ensure geocoding columns exist on the erp_mirror_cust_shipto table.

    The table itself is created and maintained by the main ERP sync worker.
    This function only adds the geocoding-specific columns if missing.
    """
    cld_cur.execute("ALTER TABLE erp_mirror_cust_shipto ADD COLUMN IF NOT EXISTS lat NUMERIC(9,6)")
    cld_cur.execute("ALTER TABLE erp_mirror_cust_shipto ADD COLUMN IF NOT EXISTS lon NUMERIC(9,6)")
    cld_cur.execute(
        "ALTER TABLE erp_mirror_cust_shipto ADD COLUMN IF NOT EXISTS geocoded_at TIMESTAMP"
    )
    cld_cur.execute(
        "ALTER TABLE erp_mirror_cust_shipto ADD COLUMN IF NOT EXISTS geocode_source VARCHAR(64)"
    )


def _as_lower_dict(row: dict) -> dict:
    return {str(k).lower(): v for k, v in row.items()}


def _source_value(row: dict, *keys: str):
    lowered = _as_lower_dict(row)
    for key in keys:
        if key.lower() in lowered:
            return lowered[key.lower()]
    return None


def transform_shipto_row(src_row: dict) -> dict:
    row = _as_lower_dict(src_row)
    seq_raw = _source_value(row, "seq_num", "seq", "shipto_seq")
    seq_num = str(seq_raw or "").strip()

    return {
        "system_id": _source_value(row, "system_id", "loc_id"),
        "cust_key": str(_source_value(row, "cust_key", "customer_key", "cust") or "").strip(),
        "seq_num": seq_num,
        "shipto_name": _source_value(row, "shipto_name", "name"),
        "address_1": _source_value(row, "address_1", "address1", "addr1"),
        "address_2": _source_value(row, "address_2", "address2", "addr2"),
        "city": _source_value(row, "city"),
        "state": _source_value(row, "state"),
        "zip": _source_value(row, "zip", "postal", "postal_code"),
        "attention": _source_value(row, "attention", "attn"),
        "phone": _source_value(row, "phone"),
        "branch_code": _source_value(row, "branch_code", "branch"),
        "source_prowid": _source_value(row, "source_prowid", "prowid"),
        "source_updated_at": _source_value(row, "source_updated_at", "update_date", "updated_at"),
    }


def addresses_equal(current: dict, existing: dict) -> bool:
    fields = ["address_1", "address_2", "city", "state", "zip"]
    for field in fields:
        if normalize_text(current.get(field)) != normalize_text(existing.get(field)):
            return False
    return True


_FETCH_BATCH_SIZE = 500  # max keys per IN-clause to avoid stack depth limits


def fetch_existing_shipto_rows(cld_cur, keys: List[Tuple[str, str, str]]) -> dict:
    """Fetch existing shipto rows by (system_id, cust_key, seq_num) for geocoding decisions."""
    if not keys:
        return {}

    mapped = {}
    for batch_start in range(0, len(keys), _FETCH_BATCH_SIZE):
        batch = keys[batch_start : batch_start + _FETCH_BATCH_SIZE]
        placeholders = ",".join(["(%s,%s,%s::integer)"] * len(batch))
        params: List[object] = []
        for system_id, cust_key, seq_num in batch:
            params.extend([system_id, cust_key, seq_num])

        query = f"""
            SELECT
                system_id,
                cust_key,
                seq_num::text AS seq_num,
                address_1,
                address_2,
                city,
                state,
                zip,
                lat,
                lon,
                geocode_source,
                geocoded_at
            FROM erp_mirror_cust_shipto
            WHERE (system_id, cust_key, seq_num) IN ({placeholders})
        """

        cld_cur.execute(query, params)
        rows = cld_cur.fetchall()
        columns = [col[0] for col in cld_cur.description]
        for row in rows:
            row_dict = dict(zip(columns, row))
            mapped[(str(row_dict["system_id"]), str(row_dict["cust_key"]), str(row_dict["seq_num"]))] = row_dict

    return mapped


def should_geocode_shipto(row: dict, existing: Optional[dict], settings: dict) -> bool:
    if not settings["enabled"]:
        return False

    if existing is None:
        return True

    address_changed = not addresses_equal(row, existing)
    has_coords = existing.get("lat") is not None and existing.get("lon") is not None

    if address_changed:
        return True

    if settings["require_missing_only"] and has_coords:
        return False

    source = (existing.get("geocode_source") or "").strip().lower()
    if not settings["retry_failed"] and source in {"failed", "nominatim_no_result"}:
        return False

    return not has_coords


def sync_customer_shipto(src_cur, cld_cur, config: dict, state: dict, geocoder: ShipToGeocoder, batch_id: Optional[str] = None) -> int:
    name = config["name"]
    use_prowid = config.get("use_prowid", True)

    if use_prowid:
        last_val = state.get(name, {}).get("last_prowid", 0)
        query = config["source_query"].format(last_prowid=last_val, last_updated="")
    else:
        last_val = state.get(name, {}).get("last_updated", "1970-01-01T00:00:00")
        query = config["source_query"].format(last_prowid=0, last_updated=last_val)

    log.info("[%s] Syncing from watermark: %s", name, last_val)

    try:
        src_cur.execute(query)
    except Exception as exc:
        log.error("[%s] Source query failed: %s", name, exc)
        return 0

    rows = src_cur.fetchall()
    if not rows:
        log.info("[%s] No new rows.", name)
        return 0

    source_columns = [col[0] for col in src_cur.description]
    source_columns_lower = {str(c).lower() for c in source_columns}
    if not source_columns_lower.intersection({"cust_key", "customer_key", "cust"}):
        log.error(
            "[%s] Source query is missing customer key column (expected one of cust_key/customer_key/cust).",
            name,
        )
        return 0
    if not source_columns_lower.intersection({"seq_num", "seq", "shipto_seq"}):
        log.error(
            "[%s] Source query is missing ship-to sequence column (expected one of seq_num/seq/shipto_seq).",
            name,
        )
        return 0

    watermark_col = config.get("watermark_col", "prowid")
    inject = config.get("inject_columns", {})
    transformed: List[dict] = []
    new_watermark = last_val
    for row in rows:
        source = dict(zip(source_columns, row))
        transformed_row = transform_shipto_row(source)

        # Apply inject_columns defaults for fields not in source (e.g. system_id='00CO')
        for col, val in inject.items():
            if col not in transformed_row or transformed_row[col] is None:
                transformed_row[col] = val() if callable(val) else val

        transformed.append(transformed_row)

        if use_prowid:
            prowid = transformed_row.get("source_prowid")
            if prowid is not None and prowid > new_watermark:
                new_watermark = prowid
        else:
            source_lower = {str(k).lower(): v for k, v in source.items()}
            updated = source_lower.get(watermark_col) or source_lower.get("updated_at")
            if updated is not None:
                updated_str = str(updated)
                if updated_str > str(new_watermark):
                    new_watermark = updated_str

    keys = [
        (row["system_id"], row["cust_key"], row["seq_num"])
        for row in transformed
        if row["cust_key"] and row["system_id"]
    ]
    existing_map = fetch_existing_shipto_rows(cld_cur, keys)

    geocode_attempted = 0
    geocode_success = 0
    geocode_failed = 0

    # Full upsert: insert all shipto columns + geocoding columns in one pass.
    # ON CONFLICT targets the (system_id, cust_key, seq_num) unique key.
    _SHIPTO_UPSERT_COLS = [
        "system_id", "cust_key", "seq_num",
        "shipto_name", "address_1", "address_2",
        "city", "state", "zip",
        "attention", "phone", "branch_code",
        "source_prowid", "source_updated_at",
        "lat", "lon", "geocoded_at", "geocode_source",
        "synced_at", "is_deleted",
    ]
    _SHIPTO_PK = {"system_id", "cust_key", "seq_num"}

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    batch_size = SHIPTO_GEOCODE_SETTINGS["batch_size"]
    row_count = 0

    for idx, row in enumerate(transformed, start=1):
        if not row["cust_key"]:
            log.warning("[%s] Skipping row with missing cust_key (source_prowid=%s)", name, row["source_prowid"])
            continue
        if not row["seq_num"]:
            log.warning("[%s] Skipping row with missing seq_num (cust_key=%s)", name, row["cust_key"])
            continue
        if not row["system_id"]:
            log.warning("[%s] Skipping row with missing system_id (cust_key=%s seq_num=%s)", name, row["cust_key"], row["seq_num"])
            continue

        key = (row["system_id"], row["cust_key"], row["seq_num"])
        existing = existing_map.get(key)

        if should_geocode_shipto(row, existing, SHIPTO_GEOCODE_SETTINGS):
            geocode_attempted += 1
            lat, lon, geocode_src = geocoder.geocode(row)
            row["lat"] = lat
            row["lon"] = lon
            row["geocoded_at"] = now_utc
            row["geocode_source"] = geocode_src
            if lat is not None and lon is not None:
                geocode_success += 1
            else:
                geocode_failed += 1
        else:
            row["lat"] = existing.get("lat") if existing else None
            row["lon"] = existing.get("lon") if existing else None
            row["geocoded_at"] = existing.get("geocoded_at") if existing else None
            row["geocode_source"] = existing.get("geocode_source") if existing else None

        row["synced_at"] = now_utc
        row["is_deleted"] = False

        upsert_cols = list(_SHIPTO_UPSERT_COLS)
        if batch_id and "sync_batch_id" not in upsert_cols:
            upsert_cols.append("sync_batch_id")
            row["sync_batch_id"] = batch_id

        upsert_stmt = sql.SQL(
            "INSERT INTO erp_mirror_cust_shipto ({}) VALUES ({}) "
            "ON CONFLICT (system_id, cust_key, seq_num) DO UPDATE SET {}"
        ).format(
            sql.SQL(", ").join(sql.Identifier(c) for c in upsert_cols),
            sql.SQL(", ").join(sql.Placeholder() for _ in upsert_cols),
            sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in upsert_cols if c not in _SHIPTO_PK
            ),
        )

        try:
            cld_cur.execute(upsert_stmt, [row.get(c) for c in upsert_cols])
            row_count += 1
        except Exception as exc:
            log.warning("[%s] Upsert failed for key=%s:%s:%s: %s", name, row["system_id"], row["cust_key"], row["seq_num"], exc)
            cld_cur.connection.rollback()

        if idx % batch_size == 0:
            cld_cur.connection.commit()
            log.info(
                "[%s] Batch committed at %s rows (attempted=%s, success=%s, failed=%s)",
                name,
                idx,
                geocode_attempted,
                geocode_success,
                geocode_failed,
            )

    state.setdefault(name, {})
    if use_prowid:
        state[name]["last_prowid"] = new_watermark
    else:
        state[name]["last_updated"] = new_watermark

    if batch_id:
        try:
            update_table_state(
                cld_cur, config["cloud_table"], config.get("family", "master"),
                batch_id, "success", row_count, new_watermark, 0,
            )
        except Exception as exc:
            log.warning("[%s] Failed to update table state: %s", name, exc)

    log.info(
        "[%s] Synced %s rows. New watermark=%s. Geocode attempted=%s success=%s failed=%s",
        name,
        row_count,
        new_watermark,
        geocode_attempted,
        geocode_success,
        geocode_failed,
    )
    return row_count


def main() -> None:
    batch_id = uuid.uuid4().hex
    batch_start = _now_utc()
    log.info("=== Beisser Sync Starting | batch=%s ===", batch_id)
    state = load_state()
    total_rows = 0
    errors: List[str] = []

    try:
        src_conn = get_source_connection()
        cld_conn = get_cloud_connection()
    except Exception as e:
        log.critical("Connection failed: %s", e)
        return

    src_cur = src_conn.cursor()
    cld_cur = cld_conn.cursor()
    geocoder = ShipToGeocoder(SHIPTO_GEOCODE_SETTINGS)

    try:
        start_batch(cld_cur, batch_id)
        cld_conn.commit()
    except Exception as exc:
        log.warning("Failed to record batch start: %s", exc)
        cld_conn.rollback()

    try:
        ensure_shipto_schema(cld_cur)
        cld_conn.commit()
    except Exception as exc:
        log.error("Schema/bootstrap failed for erp_mirror_cust_shipto: %s", exc)
        cld_conn.rollback()

    for config in TABLE_CONFIGS:
        try:
            if config.get("custom_sync") == "customer_shipto":
                count = sync_customer_shipto(src_cur, cld_cur, config, state, geocoder, batch_id=batch_id)
            else:
                count = sync_table(src_cur, cld_cur, config, state, batch_id=batch_id)
            total_rows += count
            cld_conn.commit()
        except Exception as e:
            log.error("[%s] Unexpected error: %s", config["name"], e)
            errors.append(config["name"])
            cld_conn.rollback()

    save_state(state)

    duration_ms = int((_now_utc() - batch_start).total_seconds() * 1000)
    final_status = "error" if errors else "success"
    try:
        finish_batch(cld_cur, batch_id, final_status, total_rows, total_rows, duration_ms,
                     error=f"Errors in: {', '.join(errors)}" if errors else None)
        update_sync_state(cld_cur, final_status, total_rows, errors)
        cld_conn.commit()
    except Exception as exc:
        log.warning("Failed to record batch finish: %s", exc)
        cld_conn.rollback()

    src_cur.close()
    cld_cur.close()
    src_conn.close()
    cld_conn.close()

    log.info("=== Sync Complete | batch=%s | %s rows | %s errors ===", batch_id, total_rows, len(errors))
    if errors:
        log.warning("Tables with errors: %s", ", ".join(errors))


if __name__ == "__main__":
    main()
