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


TABLE_CONFIGS = [
    {
        "name": "dispatch_orders",
        "cloud_table": "dispatch_orders",
        "source_query": """
            SELECT * FROM dbo.dispatch_orders
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "dispatch_routes",
        "cloud_table": "dispatch_routes",
        "source_query": """
            SELECT * FROM dbo.routes
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
            SELECT * FROM dbo.tag_print_queue
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
            SELECT * FROM dbo.so_header
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "sales_order_lines",
        "cloud_table": "sales_order_lines",
        "source_query": """
            SELECT * FROM dbo.so_detail
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
            SELECT * FROM dbo.cust
            WHERE prowid > {last_prowid}
        """,
        "pk": "prowid",
        "watermark_col": "prowid",
        "use_prowid": True,
    },
    {
        "name": "customer_shipto",
        "cloud_table": "erp_mirror_cust_shipto",
        "source_query": """
            SELECT *
            FROM dbo.cust_shipto
            WHERE update_date > '{last_updated}'
        """,
        "pk": ["cust_key", "seq_num"],
        "watermark_col": "update_date",
        "use_prowid": False,
        "custom_sync": "customer_shipto",
    },
    {
        "name": "customer_ar",
        "cloud_table": "customer_ar",
        "source_query": """
            SELECT * FROM dbo.customer_ar
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
            SELECT * FROM dbo.item
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
            SELECT * FROM dbo.inventory_alerts
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
            SELECT * FROM dbo.receiving_checkin
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
            SELECT * FROM dbo.po_header
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
                    normalized = {
                        "address_1": props.get("address_1") or props.get("address") or props.get("street"),
                        "city": props.get("city"),
                        "state": props.get("state"),
                        "zip": props.get("zip") or props.get("postal_code"),
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


def sync_table(src_cur, cld_cur, config: dict, state: dict) -> int:
    name = config["name"]
    cloud_table = config["cloud_table"]
    pk_config = config["pk"]
    pk_columns = [pk_config] if isinstance(pk_config, str) else pk_config
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
        if c not in pk_columns
    ]
    insert_stmt = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(cloud_table),
        sql.SQL(", ").join(insert_cols),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        sql.SQL(", ").join(sql.Identifier(c) for c in pk_columns),
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
            row_key = ":".join(str(row_dict.get(pk)) for pk in pk_columns)
            log.warning("[%s] Upsert failed for row %s: %s", name, row_key, e)

    state.setdefault(name, {})
    if use_prowid:
        state[name]["last_prowid"] = new_watermark
    else:
        state[name]["last_updated"] = new_watermark

    log.info("[%s] Synced %s rows. New watermark: %s", name, row_count, new_watermark)
    return row_count


def ensure_shipto_schema(cld_cur) -> None:
    cld_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_mirror_cust_shipto (
            cust_key VARCHAR(64) NOT NULL,
            seq_num VARCHAR(64) NOT NULL,
            shipto_name TEXT,
            address_1 TEXT,
            address_2 TEXT,
            city TEXT,
            state VARCHAR(16),
            zip VARCHAR(16),
            attention TEXT,
            phone TEXT,
            branch_code VARCHAR(32),
            lat NUMERIC(9,6),
            lon NUMERIC(9,6),
            geocoded_at TIMESTAMP,
            geocode_source VARCHAR(64),
            source_prowid BIGINT,
            source_updated_at TIMESTAMP,
            last_synced_at TIMESTAMP NOT NULL DEFAULT NOW(),
            PRIMARY KEY (cust_key, seq_num)
        )
        """
    )
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
        "source_prowid": _source_value(row, "prowid"),
        "source_updated_at": _source_value(row, "updated_at"),
    }


def addresses_equal(current: dict, existing: dict) -> bool:
    fields = ["address_1", "address_2", "city", "state", "zip"]
    for field in fields:
        if normalize_text(current.get(field)) != normalize_text(existing.get(field)):
            return False
    return True


def fetch_existing_shipto_rows(cld_cur, keys: List[Tuple[str, str]]) -> dict:
    if not keys:
        return {}

    placeholders = ",".join(["(%s,%s)"] * len(keys))
    params: List[object] = []
    for cust_key, seq_num in keys:
        params.extend([cust_key, seq_num])

    query = f"""
        SELECT
            cust_key,
            seq_num,
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
        WHERE (cust_key, seq_num) IN ({placeholders})
    """

    cld_cur.execute(query, params)
    rows = cld_cur.fetchall()
    columns = [col[0] for col in cld_cur.description]
    mapped = {}
    for row in rows:
        row_dict = dict(zip(columns, row))
        mapped[(str(row_dict["cust_key"]), str(row_dict["seq_num"]))] = row_dict
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


def sync_customer_shipto(src_cur, cld_cur, config: dict, state: dict, geocoder: ShipToGeocoder) -> int:
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
    transformed: List[dict] = []
    new_watermark = last_val
    for row in rows:
        source = dict(zip(source_columns, row))
        transformed_row = transform_shipto_row(source)
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

    keys = [(row["cust_key"], row["seq_num"]) for row in transformed if row["cust_key"]]
    existing_map = fetch_existing_shipto_rows(cld_cur, keys)

    geocode_attempted = 0
    geocode_success = 0
    geocode_failed = 0

    upsert_columns = [
        "cust_key",
        "seq_num",
        "shipto_name",
        "address_1",
        "address_2",
        "city",
        "state",
        "zip",
        "attention",
        "phone",
        "branch_code",
        "lat",
        "lon",
        "geocoded_at",
        "geocode_source",
        "source_prowid",
        "source_updated_at",
        "last_synced_at",
    ]

    update_columns = [c for c in upsert_columns if c not in {"cust_key", "seq_num"}]

    insert_stmt = sql.SQL(
        """
        INSERT INTO erp_mirror_cust_shipto ({columns})
        VALUES ({values})
        ON CONFLICT (cust_key, seq_num)
        DO UPDATE SET {updates}
        """
    ).format(
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in upsert_columns),
        values=sql.SQL(", ").join(sql.Placeholder() for _ in upsert_columns),
        updates=sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in update_columns
        ),
    )

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

        key = (row["cust_key"], row["seq_num"])
        existing = existing_map.get(key)

        if should_geocode_shipto(row, existing, SHIPTO_GEOCODE_SETTINGS):
            geocode_attempted += 1
            lat, lon, source = geocoder.geocode(row)
            row["lat"] = lat
            row["lon"] = lon
            row["geocoded_at"] = now_utc
            row["geocode_source"] = source
            if lat is not None and lon is not None:
                geocode_success += 1
            else:
                geocode_failed += 1
        else:
            row["lat"] = existing.get("lat") if existing else None
            row["lon"] = existing.get("lon") if existing else None
            row["geocoded_at"] = existing.get("geocoded_at") if existing else None
            row["geocode_source"] = existing.get("geocode_source") if existing else None

        row["last_synced_at"] = now_utc

        try:
            cld_cur.execute(insert_stmt, [row.get(c) for c in upsert_columns])
            row_count += 1
        except Exception as exc:
            log.warning("[%s] Upsert failed for key=%s:%s: %s", name, row["cust_key"], row["seq_num"], exc)

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
    geocoder = ShipToGeocoder(SHIPTO_GEOCODE_SETTINGS)

    try:
        ensure_shipto_schema(cld_cur)
        cld_conn.commit()
    except Exception as exc:
        log.error("Schema/bootstrap failed for erp_mirror_cust_shipto: %s", exc)
        cld_conn.rollback()

    for config in TABLE_CONFIGS:
        try:
            if config.get("custom_sync") == "customer_shipto":
                count = sync_customer_shipto(src_cur, cld_cur, config, state, geocoder)
            else:
                count = sync_table(src_cur, cld_cur, config, state)
            total_rows += count
            cld_conn.commit()
        except Exception as e:
            log.error("[%s] Unexpected error: %s", config["name"], e)
            errors.append(config["name"])
            cld_conn.rollback()

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
