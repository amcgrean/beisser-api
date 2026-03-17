from __future__ import annotations

import argparse
import time

from .config import get_settings
from .runtime_sync import SyncRuntime


def main() -> None:
    parser = argparse.ArgumentParser(description="agility-api sync worker")
    parser.add_argument("--once", action="store_true", help="run a single sync cycle")
    parser.add_argument("--bootstrap", action="store_true", help="create/update mirror tables before syncing")
    args = parser.parse_args()

    settings = get_settings()
    runtime = SyncRuntime()

    if args.bootstrap:
        runtime.bootstrap()

    if args.once:
        batch_id = runtime.run_once()
        print(f"Completed sync batch {batch_id}")
        return

    while True:
        batch_id = runtime.run_once()
        print(f"Completed sync batch {batch_id}")
        time.sleep(settings.heartbeat_interval_seconds)


if __name__ == "__main__":
    main()
