from __future__ import annotations

from ingester.neo4j_writer import ensure_schema


def main() -> None:
    ensure_schema()


if __name__ == "__main__":
    main()
