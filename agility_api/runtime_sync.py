from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from psycopg2.extras import execute_values
from sqlalchemy import text

from .config import configure_logging, get_settings
from .database import SessionLocal, engine
from .models import ERPSyncBatch, ERPSyncState, ERPSyncTableState
from .sqlserver import connect_sqlserver
from .sync_definitions import ExtractorDefinition, FIRST_SYNC_DEFINITIONS


def utcnow() -> datetime:
    return datetime.utcnow().replace(microsecond=0)


class SqlServerMirrorExtractor:
    def __init__(self, logger, fetch_batch_size: int, operational_history_years: int):
        self.logger = logger
        self.fetch_batch_size = fetch_batch_size
        self.operational_history_years = operational_history_years

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
        effective_watermark = self._effective_watermark(definition, watermark)
        if effective_watermark:
            predicates = [f"{column} >= ?" for column in definition.watermark_columns]
            sql += " WHERE " + " OR ".join(predicates)
            params = [effective_watermark] * len(definition.watermark_columns)
        if definition.default_order_by:
            sql += " ORDER BY " + ", ".join(definition.default_order_by)

        with connect_sqlserver() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            columns = [col[0] for col in cursor.description]
            rows: list[dict[str, Any]] = []
            chunk_index = 0
            while True:
                batch = cursor.fetchmany(self.fetch_batch_size)
                if not batch:
                    break
                chunk_index += 1
                rows.extend(dict(zip(columns, row)) for row in batch)
                self.logger.info(
                    "[%s] fetched source chunk %s (%s rows, %s total)",
                    definition.name,
                    chunk_index,
                    len(batch),
                    len(rows),
                )
            return rows

    def _effective_watermark(self, definition: ExtractorDefinition, watermark: datetime | None) -> datetime | None:
        lower_bound = None
        now_cutoff = utcnow() + timedelta(days=1)
        sane_watermark = watermark
        if sane_watermark and sane_watermark > now_cutoff:
            self.logger.warning(
                "[%s] ignoring future watermark %s and falling back to rolling window",
                definition.name,
                sane_watermark,
            )
            sane_watermark = None
        if definition.family.value == "operational" and self.operational_history_years > 0:
            lower_bound = utcnow() - timedelta(days=365 * self.operational_history_years)
        if sane_watermark and lower_bound:
            return max(sane_watermark, lower_bound)
        return sane_watermark or lower_bound


class PostgresMirrorWriter:
    def __init__(self, logger, write_batch_size: int):
        self.logger = logger
        self.write_batch_size = write_batch_size
        settings = get_settings()
        self.merge_batch_size = settings.merge_batch_size
        self.heavy_merge_batch_size = settings.heavy_merge_batch_size
        self.heavy_merge_row_threshold = settings.heavy_merge_row_threshold
        self.heavy_merge_tables = set(settings.heavy_merge_tables)

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

        total_written = 0
        merge_batch_size = self._merge_batch_size_for(definition, len(rows))
        temp_table = f"tmp_{definition.target_table}_{uuid4().hex[:8]}"
        first_prepared = self._prepare_row(definition, rows[0], batch_id)
        columns = list(first_prepared.keys())
        update_columns = [column for column in columns if column not in definition.natural_keys]

        raw_connection = engine.raw_connection()
        try:
            with raw_connection.cursor() as cursor:
                cursor.execute("SET statement_timeout = 0")
                cursor.execute(
                    f"CREATE TEMP TABLE {temp_table} "
                    f"(LIKE {definition.target_table} INCLUDING DEFAULTS) ON COMMIT DROP"
                )

                insert_sql = (
                    f"INSERT INTO {temp_table} ({', '.join(columns)}) VALUES %s"
                )
                for index in range(0, len(rows), self.write_batch_size):
                    raw_chunk = rows[index:index + self.write_batch_size]
                    prepared_rows = [self._prepare_row(definition, row, batch_id) for row in raw_chunk]
                    values = [tuple(prepared_row[column] for column in columns) for prepared_row in prepared_rows]
                    execute_values(cursor, insert_sql, values, page_size=len(values))
                    total_written += len(prepared_rows)
                    self.logger.info(
                        "[%s] staged mirror chunk %s-%s (%s rows, %s total)",
                        definition.name,
                        index + 1,
                        index + len(prepared_rows),
                        len(prepared_rows),
                        total_written,
                    )

                update_clause = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
                merged_total = 0
                while True:
                    merge_sql = (
                        f"WITH moved AS ("
                        f" DELETE FROM {temp_table}"
                        f" WHERE ctid IN (SELECT ctid FROM {temp_table} LIMIT {merge_batch_size})"
                        f" RETURNING {', '.join(columns)}"
                        f" ) "
                        f"INSERT INTO {definition.target_table} ({', '.join(columns)}) "
                        f"SELECT {', '.join(columns)} FROM moved "
                        f"ON CONFLICT ({', '.join(definition.natural_keys)}) "
                        f"DO UPDATE SET {update_clause}"
                    )
                    cursor.execute(merge_sql)
                    merged_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                    if merged_rows == 0:
                        break
                    merged_total += merged_rows
                    self.logger.info(
                        "[%s] merged target batch (%s rows, %s total)",
                        definition.name,
                        merged_rows,
                        merged_total,
                    )
            raw_connection.commit()
        except Exception:
            if getattr(raw_connection, "closed", 1) == 0:
                raw_connection.rollback()
            raise
        finally:
            if getattr(raw_connection, "closed", 1) == 0:
                raw_connection.close()

        self.logger.info("[%s] merged staged rows into %s", definition.name, definition.target_table)
        return total_written

    def count_target_rows(self, definition: ExtractorDefinition) -> int:
        with engine.connect() as conn:
            return conn.execute(text(f"SELECT COUNT(*) FROM {definition.target_table}")).scalar_one()

    def _merge_batch_size_for(self, definition: ExtractorDefinition, row_count: int) -> int:
        if (
            definition.name in self.heavy_merge_tables
            or row_count >= self.heavy_merge_row_threshold
        ):
            if self.heavy_merge_batch_size < self.merge_batch_size:
                self.logger.info(
                    "[%s] using reduced merge batch size %s (default=%s, source_rows=%s)",
                    definition.name,
                    self.heavy_merge_batch_size,
                    self.merge_batch_size,
                    row_count,
                )
                return self.heavy_merge_batch_size
        return self.merge_batch_size

    def _prepare_row(self, definition: ExtractorDefinition, row: dict[str, Any], batch_id: str) -> dict[str, Any]:
        payload = {key: value for key, value in row.items() if not key.startswith("__wm_")}
        if definition.name == "so_detail" and isinstance(payload.get("bo"), bool):
            payload["bo"] = 1 if payload["bo"] else 0
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
        self.logger = configure_logging()
        self.extractor = SqlServerMirrorExtractor(
            self.logger,
            self.settings.batch_size,
            self.settings.operational_history_years,
        )
        self.writer = PostgresMirrorWriter(self.logger, self.settings.batch_size)

    def bootstrap(self) -> None:
        self.logger.info("Bootstrapping Postgres schema")
        self.writer.bootstrap()
        self.logger.info("Schema bootstrap complete")

    def run_once(self, definitions: list[ExtractorDefinition] | None = None) -> str:
        definitions = definitions or FIRST_SYNC_DEFINITIONS
        batch_id = uuid4().hex
        started = utcnow()
        self.logger.info("Starting sync batch %s for %s tables", batch_id, len(definitions))
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
            with self.session() as session:
                table_state = self._get_or_create_table_state(session, definition)
                watermark = table_state.last_source_updated_at

            started_table = utcnow()
            self.logger.info("[%s] starting table sync (watermark=%s)", definition.name, watermark)
            try:
                rows = self.extractor.fetch_rows(definition, watermark)
                self.logger.info("[%s] fetched %s source rows", definition.name, len(rows))
                merged = self.writer.upsert_rows(definition, rows, batch_id)
                total_rows += merged
                with self.session() as session:
                    table_state = self._get_or_create_table_state(session, definition)
                    target_row_count = self.writer.count_target_rows(definition)
                    table_state.last_batch_id = batch_id
                    table_state.last_status = "success"
                    table_state.last_row_count = target_row_count
                    table_state.last_duration_ms = int((utcnow() - started_table).total_seconds() * 1000)
                    table_state.last_success_at = utcnow()
                    table_state.last_error = None
                    max_source_updated_at = self._max_source_updated_at(definition, rows)
                    if max_source_updated_at is not None:
                        table_state.last_source_updated_at = max_source_updated_at
                    session.add(table_state)
                    session.commit()
                    self.logger.info(
                        "[%s] sync complete: merged=%s row_count=%s duration_ms=%s new_watermark=%s",
                        definition.name,
                        merged,
                        target_row_count,
                        table_state.last_duration_ms,
                        table_state.last_source_updated_at,
                    )
            except Exception as exc:
                status = "error"
                last_error = f"{definition.name}: {exc}"
                self.logger.exception("[%s] sync failed", definition.name)
                with self.session() as session:
                    table_state = self._get_or_create_table_state(session, definition)
                    table_state.last_status = "error"
                    table_state.last_error = last_error
                    table_state.last_error_at = utcnow()
                    session.add(table_state)
                    session.commit()

        finished_at = utcnow()
        duration_ms = int((finished_at - started).total_seconds() * 1000)
        with self.session() as session:
            batch = session.query(ERPSyncBatch).filter_by(batch_id=batch_id).one()
            batch.finished_at = finished_at
            batch.duration_ms = duration_ms
            batch.rows_extracted = total_rows
            batch.rows_staged = total_rows
            batch.rows_upserted = total_rows
            batch.status = status
            batch.error_message = last_error

            self._record_heartbeat(session, batch_id, status, total_rows, last_error)
            session.add(batch)
            session.commit()
        self.logger.info(
            "Completed sync batch %s status=%s rows=%s duration_ms=%s",
            batch_id,
            status,
            total_rows,
            duration_ms,
        )

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

    def _max_source_updated_at(self, definition: ExtractorDefinition, rows: list[dict[str, Any]]) -> datetime | None:
        candidates = []
        now_cutoff = utcnow() + timedelta(days=1)
        for row in rows:
            for column in definition.watermark_columns:
                hidden_value = row.get(f"__wm_{column}")
                mapped_column = definition.column_map.get(column, column)
                value = hidden_value or row.get(mapped_column)
                if isinstance(value, datetime) and value <= now_cutoff:
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
