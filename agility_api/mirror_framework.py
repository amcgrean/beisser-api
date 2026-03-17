from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Protocol, Sequence


class SyncFamily(str, Enum):
    MASTER = "master"
    OPERATIONAL = "operational"
    AR = "ar"
    DOCUMENT = "document"


class SyncStrategy(str, Enum):
    INCREMENTAL = "incremental"
    REPLACE = "replace"
    WINDOWED = "windowed"
    FULL_REFRESH = "full_refresh"


@dataclass(slots=True)
class TableSyncConfig:
    table_name: str
    staging_table_name: str
    family: SyncFamily
    strategy: SyncStrategy
    natural_key_columns: tuple[str, ...]
    source_query: str
    source_updated_column: str | None = None
    cadence_seconds: int = 60
    batch_size: int = 1000


class SqlServerExtractor(Protocol):
    def fetch_rows(self, config: TableSyncConfig, *, watermark: datetime | None) -> Sequence[dict[str, Any]]:
        ...


@dataclass(slots=True)
class SyncTableResult:
    table_name: str
    extracted_rows: int = 0
    staged_rows: int = 0
    merged_rows: int = 0
    deleted_rows: int = 0
    duration_ms: int = 0
    status: str = "pending"
    error: str | None = None


@dataclass(slots=True)
class SyncBatchResult:
    batch_id: str
    started_at: datetime
    finished_at: datetime | None = None
    status: str = "running"
    table_results: list[SyncTableResult] = field(default_factory=list)
