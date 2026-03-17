from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import inspect, text

from .config import get_settings
from .database import SessionLocal, engine
from .models import ERPSyncBatch, ERPSyncState, ERPSyncTableState
from .sqlserver import connect_sqlserver
from .sync_definitions import ExtractorDefinition, FIRST_SYNC_DEFINITIONS


def utcnow() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


class SqlServerMirrorExtractor:
    def fetch_rows(self, definition: ExtractorDefinition, watermark: datetime | None) -> list[dict[str, Any]]:
        alias_parts = [
            f"{source_column} AS [{target_column}]"
            for source_column, target_column in definition.column_map.items()
        ]
        for watermark_column in definition.watermark_columns:
            if watermark_column not in definition.column_map:
                alias_parts.append(f"{watermark_column} AS [__wm_{watermark_column}]")

        aliases = ", ".join(alias_parts)
        sql = f"SELECT {aliases} FROM {definition.source_table}"
        params: list[Any] = []
        if watermark:
            predicates = [f"{column} >= ?" for column in definition.watermark_columns]
            sql += " WHERE " + " OR ".join(predicates)
            params = [watermark] * len(definition.watermark_columns)

        with connect_sqlserver() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


class PostgresMirrorWriter:
    def bootstrap(self) -> None:
        if engine is None:
            raise RuntimeError(
                "Postgres connection is not configured. Set DATABASE_URL, POSTGRES_DSN, "
                "or PGHOST/PGDATABASE/PGUSER/PGPASSWORD in C:\\Users\\amcgrean\\python\\api\\.env."
            )
        from .database import Base

        Base.metadata.create_all(engine)

    def upsert_rows(self, definition: ExtractorDefinition, rows: list[dict[str, Any]], batch_id: str) -> int:
        if not rows:
            return 0

        prepared_rows = [self._prepare_row(definition, row, batch_id) for row in rows]
        columns = list(prepared_rows[0].keys())
        temp_table = f"tmp_{definition.target_table}_{uuid4().hex[:8]}"
        update_columns = [column for column in columns if column not in definition.natural_keys]

        with engine.begin() as conn:
            conn.execute(text(f"CREATE TEMP TABLE {temp_table} (LIKE {definition.target_table} INCLUDING DEFAULTS) ON COMMIT DROP"))
            insert_sql = text(
                f"""
                INSERT INTO {temp_table} ({", ".join(columns)})
                VALUES ({", ".join(f":{column}" for column in columns)})
                """
            )
            conn.execute(insert_sql, prepared_rows)

            update_clause = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
            upsert_sql = text(
                f"""
                INSERT INTO {definition.target_table} ({", ".join(columns)})
                SELECT {", ".join(columns)} FROM {temp_table}
                ON CONFLICT ({", ".join(definition.natural_keys)})
                DO UPDATE SET {update_clause}
                """
            )
            conn.execute(upsert_sql)
        return len(prepared_rows)

    def _prepare_row(self, definition: ExtractorDefinition, row: dict[str, Any], batch_id: str) -> dict[str, Any]:
        payload = {key: value for key, value in row.items() if not key.startswith("__wm_")}
        payload["source_updated_at"] = self._coalesce_source_updated_at(definition, row)
        payload["synced_at"] = utcnow()
        payload["sync_batch_id"] = batch_id
        payload["row_fingerprint"] = self._fingerprint(row)
        payload["is_deleted"] = False
        return payload

    def _coalesce_source_updated_at(self, definition: ExtractorDefinition, row: dict[str, Any]) -> datetime | None:
        for column in definition.watermark_columns:
            hidden_value = row.get(f"__wm_{column}")
            if hidden_value:
                return hidden_value
            mapped_column = definition.column_map.get(column, column)
            value = row.get(mapped_column)
            if value:
                return value
        return None

    def _fingerprint(self, row: dict[str, Any]) -> str:
        serialized = json.dumps(row, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class SyncRuntime:
    def __init__(self):
        self.settings = get_settings()
        self.extractor = SqlServerMirrorExtractor()
        self.writer = PostgresMirrorWriter()

    def bootstrap(self) -> None:
        self.writer.bootstrap()

    def run_once(self, definitions: list[ExtractorDefinition] | None = None) -> str:
        definitions = definitions or FIRST_SYNC_DEFINITIONS
        batch_id = uuid4().hex
        started = utcnow()
        batch = ERPSyncBatch(
            batch_id=batch_id,
            worker_name=self.settings.worker_name,
            family="mixed",
            table_count=len(definitions),
            status="running",
            started_at=started,
        )

        with self.session() as session:
            session.add(batch)
            session.commit()

            total_rows = 0
            status = "success"
            last_error = None

            for definition in definitions:
                table_state = self._get_or_create_table_state(session, definition)
                watermark = table_state.last_source_updated_at
                started_table = utcnow()
                try:
                    rows = self.extractor.fetch_rows(definition, watermark)
                    merged = self.writer.upsert_rows(definition, rows, batch_id)
                    total_rows += merged
                    table_state.last_batch_id = batch_id
                    table_state.last_status = "success"
                    table_state.last_row_count = merged
                    table_state.last_duration_ms = int((utcnow() - started_table).total_seconds() * 1000)
                    table_state.last_success_at = utcnow()
                    table_state.last_error = None
                    table_state.last_source_updated_at = self._max_source_updated_at(rows)
                except Exception as exc:
                    status = "error"
                    last_error = str(exc)
                    table_state.last_status = "error"
                    table_state.last_error = last_error
                    table_state.last_error_at = utcnow()
                session.add(table_state)

            batch.finished_at = utcnow()
            batch.duration_ms = int((batch.finished_at - started).total_seconds() * 1000)
            batch.rows_extracted = total_rows
            batch.rows_staged = total_rows
            batch.rows_upserted = total_rows
            batch.status = status
            batch.error_message = last_error

            self._record_heartbeat(session, batch_id, status, total_rows, last_error)
            session.add(batch)
            session.commit()

        return batch_id

    def _get_or_create_table_state(self, session, definition: ExtractorDefinition) -> ERPSyncTableState:
        row = session.query(ERPSyncTableState).filter_by(table_name=definition.target_table).first()
        if row:
            return row
        return ERPSyncTableState(
            table_name=definition.target_table,
            family=definition.family.value,
            strategy="incremental",
        )

    def _record_heartbeat(self, session, batch_id: str, status: str, row_count: int, error: str | None) -> None:
        heartbeat = session.query(ERPSyncState).filter_by(worker_name=self.settings.worker_name).first()
        if heartbeat is None:
            heartbeat = ERPSyncState(worker_name=self.settings.worker_name)
        heartbeat.worker_mode = self.settings.worker_mode
        heartbeat.interval_seconds = self.settings.heartbeat_interval_seconds
        heartbeat.last_heartbeat_at = utcnow()
        heartbeat.last_status = status
        heartbeat.last_error = error
        heartbeat.last_change_token = batch_id[:12]
        heartbeat.last_payload_hash = batch_id
        heartbeat.last_push_reason = "sync_cycle"
        heartbeat.last_counts_json = json.dumps({"rows_upserted": row_count})
        if status == "success":
            heartbeat.last_success_at = utcnow()
        else:
            heartbeat.last_error_at = utcnow()
        session.add(heartbeat)

    def _max_source_updated_at(self, rows: list[dict[str, Any]]) -> datetime | None:
        candidates = []
        for row in rows:
            for value in row.values():
                if isinstance(value, datetime):
                    candidates.append(value)
        return max(candidates) if candidates else None

    @contextmanager
    def session(self):
        if SessionLocal is None:
            raise RuntimeError(
                "Postgres connection is not configured. Set DATABASE_URL, POSTGRES_DSN, "
                "or PGHOST/PGDATABASE/PGUSER/PGPASSWORD in C:\\Users\\amcgrean\\python\\api\\.env."
            )
        session = SessionLocal()
        try:
            yield session
        finally:
            session.close()
