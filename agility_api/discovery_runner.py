from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from .config import ROOT
from .discovery_queries import GENERAL_DISCOVERY_QUERIES, MIRROR_TARGET_TABLES, TARGET_DISCOVERY_QUERIES
from .sqlserver import connect_sqlserver, load_sqlserver_config


def output_dir() -> Path:
    path = ROOT / "discovery_output"
    path.mkdir(parents=True, exist_ok=True)
    return path


def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def rows_to_dicts(cursor) -> list[dict]:
    cols = [col[0] for col in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def run_query(cursor, sql: str) -> list[dict]:
    cursor.execute(sql)
    return rows_to_dicts(cursor)


def save_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def save_json(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def build_target_gap_report(table_rows: list[dict], column_rows: list[dict]) -> list[dict]:
    found_tables = {row["table_name"].lower() for row in table_rows}
    columns_by_table: dict[str, list[str]] = {}
    for row in column_rows:
        columns_by_table.setdefault(row["table_name"].lower(), []).append(row["column_name"])

    report = []
    for table_name in MIRROR_TARGET_TABLES:
        report.append(
            {
                "target_table": table_name,
                "found": table_name in found_tables,
                "column_count": len(columns_by_table.get(table_name, [])),
                "sample_columns": ", ".join(columns_by_table.get(table_name, [])[:12]),
            }
        )
    return report


def run_discovery_suite() -> dict:
    stamp = timestamp()
    out_dir = output_dir()
    cfg = load_sqlserver_config()

    results: dict[str, list[dict]] = {}
    with connect_sqlserver() as conn:
        cursor = conn.cursor()
        for name, sql in GENERAL_DISCOVERY_QUERIES.items():
            rows = run_query(cursor, sql)
            results[name] = rows
            save_csv(out_dir / f"{stamp}_{name}.csv", rows)

        for name, sql in TARGET_DISCOVERY_QUERIES.items():
            rows = run_query(cursor, sql)
            results[name] = rows
            save_csv(out_dir / f"{stamp}_{name}.csv", rows)

    gap_report = build_target_gap_report(
        results.get("mirror_target_tables", []),
        results.get("mirror_target_columns", []),
    )
    results["mirror_target_gap_report"] = gap_report
    save_csv(out_dir / f"{stamp}_mirror_target_gap_report.csv", gap_report)

    summary = {
        "sql_server": {
            "server": cfg.server,
            "database": cfg.database,
        },
        "generated_at": stamp,
        "files_written": sorted(path.name for path in out_dir.glob(f"{stamp}_*")),
        "target_tables": MIRROR_TARGET_TABLES,
    }
    save_json(out_dir / f"{stamp}_discovery_summary.json", summary)
    save_json(out_dir / f"{stamp}_all_discovery.json", results)
    return summary
