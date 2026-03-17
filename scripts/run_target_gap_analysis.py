from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agility_api.discovery_queries import MIRROR_TARGET_TABLES
from agility_api.discovery_runner import build_target_gap_report, run_query
from agility_api.sqlserver import connect_sqlserver


def main() -> None:
    from agility_api.discovery_queries import TARGET_DISCOVERY_QUERIES

    with connect_sqlserver() as conn:
        cursor = conn.cursor()
        table_rows = run_query(cursor, TARGET_DISCOVERY_QUERIES["mirror_target_tables"])
        column_rows = run_query(cursor, TARGET_DISCOVERY_QUERIES["mirror_target_columns"])

    report = build_target_gap_report(table_rows, column_rows)
    print("Mirror target table coverage")
    for row in report:
        status = "FOUND" if row["found"] else "MISSING"
        print(f"- {row['target_table']}: {status} | columns={row['column_count']}")
        if row["sample_columns"]:
            print(f"  sample: {row['sample_columns']}")

    missing = [row["target_table"] for row in report if not row["found"]]
    if missing:
        print("\nMissing targets:")
        for name in missing:
            print(f"- {name}")
    else:
        print(f"\nAll {len(MIRROR_TARGET_TABLES)} target tables were found.")


if __name__ == "__main__":
    main()
