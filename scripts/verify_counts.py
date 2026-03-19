from pathlib import Path
import argparse
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agility_api.verification import compare_counts, definitions_by_selector


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare source and mirror row counts")
    parser.add_argument("table_names", nargs="*", help="optional definition names, e.g. po_header po_detail")
    parser.add_argument("--family", help="optional family name, e.g. operational or master")
    args = parser.parse_args()

    definitions = definitions_by_selector(names=args.table_names or None, family=args.family)
    results = compare_counts(definitions)
    print("Source vs mirror row counts")
    for row in results:
        print(
            f"- {row.name}: source={row.source_count} | mirror={row.mirror_count} | delta={row.delta}"
        )
