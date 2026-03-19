from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import config
from api.db import run_query


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate topic mention counts inside the recent analytics retention window.",
    )
    parser.add_argument("--days", type=int, default=config.GRAPH_ANALYTICS_RETENTION_DAYS)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    days = max(1, int(args.days))
    rows = run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']
        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime() - duration({days: $days})
            RETURN count(DISTINCT p) AS postCount
        }
        CALL (t) {
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime() - duration({days: $days})
            RETURN count(DISTINCT c) AS commentCount
        }
        WITH t, cat, postCount, commentCount, (postCount + commentCount) AS mentionCount
        WHERE mentionCount > 0
        RETURN
            t.name AS topic,
            cat.name AS category,
            postCount,
            commentCount,
            mentionCount,
            CASE
                WHEN (postCount + commentCount) = 0 THEN 0.0
                ELSE round(toFloat(mentionCount) / toFloat(postCount + commentCount), 2)
            END AS messageRatio
        ORDER BY mentionCount DESC, topic ASC
        LIMIT $limit
        """,
        {"days": days, "limit": max(1, int(args.limit))},
    )

    suspicious = [
        row for row in rows
        if int(row.get("mentionCount") or 0) > (int(row.get("postCount") or 0) + int(row.get("commentCount") or 0))
    ]

    logger.info(
        "Validated topic mentions | "
        f"days={days} topics_checked={len(rows)} suspicious_topics={len(suspicious)}"
    )

    for row in suspicious[:20]:
        logger.warning(
            "Suspicious topic ratio | "
            f"topic={row.get('topic')} category={row.get('category')} "
            f"posts={row.get('postCount')} comments={row.get('commentCount')} "
            f"mentions={row.get('mentionCount')} ratio={row.get('messageRatio')}"
        )

    return 0 if not suspicious else 1


if __name__ == "__main__":
    raise SystemExit(main())
