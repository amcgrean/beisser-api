from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agility_api.discovery_runner import run_discovery_suite


if __name__ == "__main__":
    summary = run_discovery_suite()
    print("Discovery complete")
    print(f"Server: {summary['sql_server']['server']}")
    print(f"Database: {summary['sql_server']['database']}")
    print("Files:")
    for name in summary["files_written"]:
        print(f"  - {name}")
