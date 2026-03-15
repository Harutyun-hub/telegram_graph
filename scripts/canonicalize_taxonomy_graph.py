from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from api import db
from utils.taxonomy import canonical_category_name, canonical_domain_name


def _fetch_domain_names() -> list[str]:
    rows = db.run_query("MATCH (d:TopicDomain) RETURN d.name AS name ORDER BY name")
    return [str(row.get("name") or "").strip() for row in rows if row.get("name")]


def _fetch_category_names() -> list[str]:
    rows = db.run_query("MATCH (c:TopicCategory) RETURN c.name AS name ORDER BY name")
    return [str(row.get("name") or "").strip() for row in rows if row.get("name")]


def _merge_domain(source_name: str, target_name: str) -> None:
    db.run_query(
        """
        MATCH (src:TopicDomain {name: $source_name})
        MERGE (dst:TopicDomain {name: $target_name})
        WITH src, dst
        WHERE elementId(src) <> elementId(dst)
        MATCH (c:TopicCategory)-[r:IN_DOMAIN]->(src)
        MERGE (c)-[:IN_DOMAIN]->(dst)
        DELETE r
        """,
        {"source_name": source_name, "target_name": target_name},
    )

    db.run_query(
        """
        MATCH (src:TopicDomain {name: $source_name})
        WHERE src.name <> $target_name
        OPTIONAL MATCH (src)-[r]-()
        WITH src, count(r) AS rel_count
        WHERE rel_count = 0
        DELETE src
        """,
        {"source_name": source_name, "target_name": target_name},
    )


def _merge_category(source_name: str, target_name: str) -> None:
    db.run_query(
        """
        MATCH (src:TopicCategory {name: $source_name})
        MERGE (dst:TopicCategory {name: $target_name})
        WITH src, dst
        WHERE elementId(src) <> elementId(dst)
        MATCH (t:Topic)-[r:BELONGS_TO_CATEGORY]->(src)
        MERGE (t)-[:BELONGS_TO_CATEGORY]->(dst)
        DELETE r
        """,
        {"source_name": source_name, "target_name": target_name},
    )

    db.run_query(
        """
        MATCH (src:TopicCategory {name: $source_name})
        MERGE (dst:TopicCategory {name: $target_name})
        WITH src, dst
        WHERE elementId(src) <> elementId(dst)
        MATCH (src)-[r:IN_DOMAIN]->(d:TopicDomain)
        MERGE (dst)-[:IN_DOMAIN]->(d)
        DELETE r
        """,
        {"source_name": source_name, "target_name": target_name},
    )

    db.run_query(
        """
        MATCH (src:TopicCategory {name: $source_name})
        WHERE src.name <> $target_name
        OPTIONAL MATCH (src)-[r]-()
        WITH src, count(r) AS rel_count
        WHERE rel_count = 0
        DELETE src
        """,
        {"source_name": source_name, "target_name": target_name},
    )


def _domain_rename_plan() -> list[tuple[str, str]]:
    plan: list[tuple[str, str]] = []
    for name in _fetch_domain_names():
        canonical = canonical_domain_name(name)
        if canonical and canonical != name:
            plan.append((name, canonical))
    return sorted(set(plan))


def _category_rename_plan() -> list[tuple[str, str]]:
    def _normalized(value: str) -> str:
        text = " ".join(str(value).strip().lower().split())
        return text.replace(" and ", " & ")

    plan: list[tuple[str, str]] = []
    for name in _fetch_category_names():
        canonical = canonical_category_name(name)
        # Category merges are intentionally conservative:
        # apply only when labels are lexical variants (e.g. "And" vs "&").
        if canonical and canonical != name and _normalized(canonical) == _normalized(name):
            plan.append((name, canonical))
    return sorted(set(plan))


def _summary() -> dict:
    domains = db.run_single("MATCH (d:TopicDomain) RETURN count(d) AS n") or {}
    categories = db.run_single("MATCH (c:TopicCategory) RETURN count(c) AS n") or {}
    topics = db.run_single("MATCH (t:Topic) RETURN count(t) AS n") or {}
    general = db.run_single(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(c:TopicCategory)-[:IN_DOMAIN]->(d:TopicDomain)
        WITH count(DISTINCT t) AS total,
             count(DISTINCT CASE WHEN c.name='General' OR d.name='General' THEN t END) AS general_topics
        RETURN total, general_topics,
               CASE WHEN total = 0 THEN 0.0 ELSE toFloat(general_topics) / toFloat(total) END AS ratio
        """
    ) or {}
    return {
        "domains": int(domains.get("n") or 0),
        "categories": int(categories.get("n") or 0),
        "topics": int(topics.get("n") or 0),
        "general_topics": int(general.get("general_topics") or 0),
        "general_ratio": round(float(general.get("ratio") or 0.0), 4),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Canonicalize TopicDomain/TopicCategory labels in Neo4j")
    parser.add_argument("--apply", action="store_true", help="Apply merge operations (default is dry-run)")
    args = parser.parse_args()

    domain_plan = _domain_rename_plan()
    category_plan = _category_rename_plan()

    logger.info(f"Domain canonicalization candidates: {len(domain_plan)}")
    for source, target in domain_plan:
        logger.info(f"  DOMAIN: {source} -> {target}")

    logger.info(f"Category canonicalization candidates: {len(category_plan)}")
    for source, target in category_plan:
        logger.info(f"  CATEGORY: {source} -> {target}")

    if not args.apply:
        logger.info("Dry run complete (no graph updates applied). Use --apply to execute.")
        logger.info(f"Current summary: {_summary()}")
        return 0

    for source, target in domain_plan:
        _merge_domain(source, target)
    for source, target in category_plan:
        _merge_category(source, target)

    logger.success("Canonicalization merge operations completed")
    logger.info(f"Updated summary: {_summary()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
