from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

from .database import engine
from .sqlserver import connect_sqlserver
from .sync_definitions import FIRST_SYNC_DEFINITIONS, ExtractorDefinition


@dataclass(slots=True)
class CountComparison:
    name: str
    source_table: str
    target_table: str
    source_count: int
    mirror_count: int
    delta: int


def _require_engine():
    if engine is None:
        raise RuntimeError("Postgres engine is not configured.")
    return engine


def source_count(definition: ExtractorDefinition) -> int:
    sql = f"SELECT COUNT(*) AS row_count FROM {definition.source_table}"
    with connect_sqlserver() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def mirror_count(definition: ExtractorDefinition) -> int:
    pg_engine = _require_engine()
    with pg_engine.begin() as conn:
        row = conn.execute(text(f"SELECT COUNT(*) FROM {definition.target_table}")).fetchone()
        return int(row[0]) if row else 0


def compare_counts(definitions: list[ExtractorDefinition] | None = None) -> list[CountComparison]:
    definitions = definitions or FIRST_SYNC_DEFINITIONS
    results: list[CountComparison] = []
    for definition in definitions:
        src = source_count(definition)
        dst = mirror_count(definition)
        results.append(
            CountComparison(
                name=definition.name,
                source_table=definition.source_table,
                target_table=definition.target_table,
                source_count=src,
                mirror_count=dst,
                delta=dst - src,
            )
        )
    return results


def sample_source_rows(definition: ExtractorDefinition, limit: int = 5) -> list[dict[str, Any]]:
    aliases = ", ".join(
        f"{source_column} AS [{target_column}]"
        for source_column, target_column in definition.column_map.items()
    )
    order_by = ", ".join(definition.natural_keys)
    sql = f"SELECT TOP {limit} {aliases} FROM {definition.source_table} ORDER BY {order_by}"
    with connect_sqlserver() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def sample_mirror_rows(definition: ExtractorDefinition, limit: int = 5) -> list[dict[str, Any]]:
    pg_engine = _require_engine()
    columns = list(definition.column_map.values())
    order_by = ", ".join(definition.natural_keys)
    sql = text(
        f"""
        SELECT {", ".join(columns)}
        FROM {definition.target_table}
        ORDER BY {order_by}
        LIMIT :limit
        """
    )
    with pg_engine.begin() as conn:
        rows = conn.execute(sql, {"limit": limit}).mappings().all()
        return [dict(row) for row in rows]


def definition_by_name(name: str) -> ExtractorDefinition:
    for definition in FIRST_SYNC_DEFINITIONS:
        if definition.name == name:
            return definition
    raise KeyError(f"Unknown definition: {name}")
