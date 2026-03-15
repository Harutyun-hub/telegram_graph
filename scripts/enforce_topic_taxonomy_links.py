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
from utils.topic_normalizer import classify_topic, normalize_topic_category, normalize_topic_domain


def _topic_names(limit: int) -> list[str]:
    rows = db.run_query(
        """
        MATCH (t:Topic)
        RETURN t.name AS name
        ORDER BY t.name
        LIMIT $limit
        """,
        {"limit": int(limit)},
    )
    return [str(row.get("name") or "").strip() for row in rows if row.get("name")]


def _summary() -> dict:
    row = db.run_single(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(c:TopicCategory)-[:IN_DOMAIN]->(d:TopicDomain)
        WITH count(DISTINCT t) AS total,
             count(DISTINCT CASE WHEN c.name='General' OR d.name='General' THEN t END) AS general_topics
        RETURN total, general_topics,
               CASE WHEN total = 0 THEN 0.0 ELSE toFloat(general_topics) / toFloat(total) END AS ratio
        """
    ) or {}
    return {
        "topics": int(row.get("total") or 0),
        "general_topics": int(row.get("general_topics") or 0),
        "general_ratio": round(float(row.get("ratio") or 0.0), 4),
    }


def _apply_topic_link(topic_name: str, category: str, domain: str) -> None:
    db.run_query(
        """
        MATCH (t:Topic {name: $topic_name})
        MERGE (c:TopicCategory {name: $category})
        MERGE (d:TopicDomain {name: $domain})
        MERGE (c)-[:IN_DOMAIN]->(d)
        MERGE (t)-[:BELONGS_TO_CATEGORY]->(c)
        """,
        {
            "topic_name": topic_name,
            "category": category,
            "domain": domain,
        },
    )

    # Enforce one canonical category edge for non-proposed canonical topics.
    db.run_query(
        """
        MATCH (t:Topic {name: $topic_name})-[r:BELONGS_TO_CATEGORY]->(other:TopicCategory)
        WHERE other.name <> $category
        DELETE r
        """,
        {
            "topic_name": topic_name,
            "category": category,
        },
    )

    # Enforce one canonical domain edge per category to prevent
    # category-domain drift (e.g. canonical category still linked to General).
    db.run_query(
        """
        MATCH (c:TopicCategory {name: $category})-[r:IN_DOMAIN]->(other:TopicDomain)
        WHERE other.name <> $domain
        DELETE r
        """,
        {
            "category": category,
            "domain": domain,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Enforce canonical Topic -> Category -> Domain links")
    parser.add_argument("--limit", type=int, default=3000, help="Maximum topics to scan")
    parser.add_argument("--apply", action="store_true", help="Apply updates (default dry-run)")
    parser.add_argument(
        "--include-proposed",
        action="store_true",
        help="Also enforce inferred non-general links for proposed topics",
    )
    args = parser.parse_args()

    SupabaseWriter().refresh_runtime_topic_aliases()

    topic_names = _topic_names(max(1, int(args.limit)))
    plan: list[tuple[str, str, str]] = []
    for topic_name in topic_names:
        cls = classify_topic(topic_name)
        if not cls:
            continue
        is_proposed = bool(cls.get("proposed", False))
        if is_proposed and not args.include_proposed:
            continue

        category = normalize_topic_category(str(cls.get("closest_category") or "General"))
        domain = normalize_topic_domain(str(cls.get("domain") or "General"))
        if is_proposed and (category == "General" or domain == "General"):
            continue
        if not category:
            category = "General"
        if not domain:
            domain = "General"

        plan.append((topic_name, category, domain))

    logger.info(f"Canonical link candidates: {len(plan)}")
    for topic_name, category, domain in plan[:40]:
        logger.info(f"  LINK: {topic_name} -> {category} -> {domain}")

    if not args.apply:
        logger.info("Dry run complete (no taxonomy links modified). Use --apply to execute.")
        logger.info(f"Current summary: {_summary()}")
        return 0

    for topic_name, category, domain in plan:
        _apply_topic_link(topic_name, category, domain)

    logger.success("Canonical taxonomy link enforcement completed")
    logger.info(f"Updated summary: {_summary()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
