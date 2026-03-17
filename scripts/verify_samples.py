from pathlib import Path
import argparse
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agility_api.verification import definition_by_name, sample_mirror_rows, sample_source_rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Show source vs mirror samples for one sync definition")
    parser.add_argument("table_name", help="definition name, e.g. cust or so_header")
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()

    definition = definition_by_name(args.table_name)
    source_rows = sample_source_rows(definition, limit=args.limit)
    mirror_rows = sample_mirror_rows(definition, limit=args.limit)

    print("SOURCE")
    print(json.dumps(source_rows, indent=2, default=str))
    print("\nMIRROR")
    print(json.dumps(mirror_rows, indent=2, default=str))
