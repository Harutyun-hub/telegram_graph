from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from api import db
from buffer.supabase_writer import SupabaseWriter
from utils.topic_normalizer import classify_topic


def _topic_rows(limit: int) -> list[dict]:
    return db.run_query(
        """
        MATCH (t:Topic)
        RETURN t.name AS name, coalesce(t.proposed,false) AS proposed
        ORDER BY t.name
        LIMIT $limit
        """,
        {"limit": int(limit)},
    )


def _merge_topic(source_name: str, target_name: str, *, proposed: bool) -> None:
    params = {
        "source_name": source_name,
        "target_name": target_name,
        "proposed": bool(proposed),
    }

    db.run_query(
        """
        MATCH (src:Topic {name: $source_name})
        MERGE (dst:Topic {name: $target_name})
        ON CREATE SET dst.proposed = $proposed
        WITH src, dst
        WHERE elementId(src) <> elementId(dst)
        MATCH (n)-[r:TAGGED]->(src)
        MERGE (n)-[:TAGGED]->(dst)
        DELETE r
        """,
        params,
    )

    db.run_query(
        """
        MATCH (src:Topic {name: $source_name})
        MERGE (dst:Topic {name: $target_name})
        WITH src, dst
        WHERE elementId(src) <> elementId(dst)
        MATCH (u:User)-[r:INTERESTED_IN]->(src)
        MERGE (u)-[:INTERESTED_IN]->(dst)
        DELETE r
        """,
        params,
    )

    db.run_query(
        """
        MATCH (src:Topic {name: $source_name})
        MERGE (dst:Topic {name: $target_name})
        WITH src, dst
        WHERE elementId(src) <> elementId(dst)
        MATCH (ch:Channel)-[r:DISCUSSES]->(src)
        MERGE (ch)-[:DISCUSSES]->(dst)
        DELETE r
        """,
        params,
    )

    db.run_query(
        """
        MATCH (src:Topic {name: $source_name})
        MERGE (dst:Topic {name: $target_name})
        WITH src, dst
        WHERE elementId(src) <> elementId(dst)
        MATCH (src)-[r:BELONGS_TO_CATEGORY]->(c:TopicCategory)
        MERGE (dst)-[:BELONGS_TO_CATEGORY]->(c)
        DELETE r
        """,
        params,
    )

    db.run_query(
        """
        MATCH (src:Topic {name: $source_name})
        MERGE (dst:Topic {name: $target_name})
        WITH src, dst
        WHERE elementId(src) <> elementId(dst)
        MATCH (other:Topic)-[r:CO_OCCURS_WITH]->(src)
        WHERE elementId(other) <> elementId(dst)
        MERGE (other)-[:CO_OCCURS_WITH]->(dst)
        DELETE r
        """,
        params,
    )

    db.run_query(
        """
        MATCH (src:Topic {name: $source_name})
        MERGE (dst:Topic {name: $target_name})
        WITH src, dst
        WHERE elementId(src) <> elementId(dst)
        MATCH (src)-[r:CO_OCCURS_WITH]->(other:Topic)
        WHERE elementId(other) <> elementId(dst)
        MERGE (dst)-[:CO_OCCURS_WITH]->(other)
        DELETE r
        """,
        params,
    )

    db.run_query(
        """
        MATCH (src:Topic {name: $source_name})
        WHERE src.name <> $target_name
        OPTIONAL MATCH (src)-[r]-()
        WITH src, count(r) AS rel_count
        WHERE rel_count = 0
        DELETE src
        """,
        params,
    )


def _summary() -> dict:
    row = db.run_single(
        """
        MATCH (t:Topic)
        OPTIONAL MATCH (t)-[:BELONGS_TO_CATEGORY]->(c:TopicCategory)-[:IN_DOMAIN]->(d:TopicDomain)
        WITH count(DISTINCT t) AS total,
             count(DISTINCT CASE WHEN coalesce(t.proposed,false) THEN t END) AS proposed,
             count(DISTINCT CASE WHEN c.name='General' OR d.name='General' THEN t END) AS general_topics
        RETURN total, proposed, general_topics,
               CASE WHEN total=0 THEN 0.0 ELSE toFloat(proposed)/toFloat(total) END AS proposed_ratio,
               CASE WHEN total=0 THEN 0.0 ELSE toFloat(general_topics)/toFloat(total) END AS general_ratio
        """
    ) or {}
    return {
        "topics": int(row.get("total") or 0),
        "proposed_topics": int(row.get("proposed") or 0),
        "general_topics": int(row.get("general_topics") or 0),
        "proposed_ratio": round(float(row.get("proposed_ratio") or 0.0), 4),
        "general_ratio": round(float(row.get("general_ratio") or 0.0), 4),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Reclassify and merge Topic nodes by canonical alias mapping")
    parser.add_argument("--limit", type=int, default=2000, help="Max topic nodes to scan")
    parser.add_argument("--apply", action="store_true", help="Apply merges (default dry-run)")
    args = parser.parse_args()

    SupabaseWriter().refresh_runtime_topic_aliases()

    rows = _topic_rows(max(1, int(args.limit)))
    merge_plan: list[tuple[str, str, bool]] = []
    for row in rows:
        topic_name = str(row.get("name") or "").strip()
        if not topic_name:
            continue
        cls = classify_topic(topic_name)
        canonical = str(cls.get("taxonomy_topic") or "").strip()
        if canonical and canonical != topic_name:
            merge_plan.append((topic_name, canonical, bool(cls.get("proposed", False))))

    unique_plan = sorted(set(merge_plan), key=lambda item: item[0].lower())
    logger.info(f"Topic reclassification candidates: {len(unique_plan)}")
    for source, target, _ in unique_plan[:40]:
        logger.info(f"  TOPIC: {source} -> {target}")

    if not args.apply:
        logger.info("Dry run complete (no topic merges applied). Use --apply to execute.")
        logger.info(f"Current summary: {_summary()}")
        return 0

    for source, target, proposed in unique_plan:
        _merge_topic(source, target, proposed=proposed)

    logger.success("Topic reclassification merges completed")
    logger.info(f"Updated summary: {_summary()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
