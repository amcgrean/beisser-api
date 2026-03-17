from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agility_api.verification import compare_counts


if __name__ == "__main__":
    results = compare_counts()
    print("Source vs mirror row counts")
    for row in results:
        print(
            f"- {row.name}: source={row.source_count} | mirror={row.mirror_count} | delta={row.delta}"
        )
