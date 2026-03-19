from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from api import db


def _candidate_topics(topic_name: str | None = None) -> list[dict]:
    query = """
    MATCH (t:Topic)-[general_rel:BELONGS_TO_CATEGORY]->(general:TopicCategory {name: 'General'})
    MATCH (t)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
    WHERE cat.name <> 'General'
    OPTIONAL MATCH (cat)-[:IN_DOMAIN]->(dom:TopicDomain)
    WHERE $topic_name IS NULL OR t.name = $topic_name
    RETURN t.name AS topic,
           collect(DISTINCT cat.name) AS retained_categories,
           collect(DISTINCT dom.name) AS retained_domains,
           count(DISTINCT general_rel) AS general_links
    ORDER BY topic
    """
    return db.run_query(query, {"topic_name": topic_name})


def _remove_general_links(topic_name: str | None = None) -> int:
    query = """
    MATCH (t:Topic)-[r:BELONGS_TO_CATEGORY]->(:TopicCategory {name: 'General'})
    MATCH (t)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
    WHERE cat.name <> 'General'
      AND ($topic_name IS NULL OR t.name = $topic_name)
    DELETE r
    RETURN count(DISTINCT t) AS topics_updated
    """
    row = db.run_single(query, {"topic_name": topic_name}) or {}
    return int(row.get("topics_updated") or 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Remove redundant Topic -> General category links when a topic already has a non-General category"
    )
    parser.add_argument("--topic", help="Optional single topic name to target")
    parser.add_argument("--apply", action="store_true", help="Apply deletions (default is dry-run)")
    args = parser.parse_args()

    candidates = _candidate_topics(args.topic)
    logger.info(f"Redundant General-link candidates: {len(candidates)}")
    for row in candidates[:50]:
        logger.info(
            "  TOPIC: {} -> keep categories={} domains={} remove_general_links={}",
            row.get("topic"),
            row.get("retained_categories") or [],
            row.get("retained_domains") or [],
            int(row.get("general_links") or 0),
        )

    if not args.apply:
        logger.info("Dry run complete (no graph updates applied). Use --apply to execute.")
        return 0

    updated = _remove_general_links(args.topic)
    logger.success(f"Removed redundant General links from {updated} topic(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
