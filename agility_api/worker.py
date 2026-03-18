from __future__ import annotations

import argparse
import time

from .config import get_settings
from .runtime_sync import SyncRuntime
from .sync_definitions import definitions_for_family, definitions_for_names


def main() -> None:
    parser = argparse.ArgumentParser(description="agility-api sync worker")
    parser.add_argument("--once", action="store_true", help="run a single sync cycle")
    parser.add_argument("--bootstrap", action="store_true", help="create/update mirror tables before syncing")
    parser.add_argument(
        "--family",
        choices=["master", "operational", "ar", "document"],
        help="run only one sync family",
    )
    parser.add_argument(
        "--tables",
        help="comma-separated table definition names to run, for example cust,cust_shipto",
    )
    args = parser.parse_args()

    settings = get_settings()
    runtime = SyncRuntime()
    definitions = None

    if args.family:
        definitions = definitions_for_family(args.family)

    if args.tables:
        table_definitions = definitions_for_names([name.strip() for name in args.tables.split(",") if name.strip()])
        definitions = table_definitions

    if definitions is not None and not definitions:
        raise SystemExit("No sync definitions matched the provided --family/--tables filter.")

    if args.bootstrap:
        runtime.bootstrap()

    if args.once:
        batch_id = runtime.run_once(definitions=definitions)
        print(f"Completed sync batch {batch_id}")
        return

    while True:
        batch_id = runtime.run_once(definitions=definitions)
        print(f"Completed sync batch {batch_id}")
        time.sleep(settings.heartbeat_interval_seconds)


if __name__ == "__main__":
    main()
