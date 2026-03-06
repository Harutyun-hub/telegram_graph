"""
pulse.py — Tier 1: Community Pulse (30-second executive overview)

Provides: communityHealth, trendingTopics, communityBrief
"""
from __future__ import annotations
from api.db import run_query, run_single


def get_community_health() -> dict:
    """Compute community health score from sentiment distribution + activity."""
    row = run_single("""
        MATCH (u:User)-[hs:HAS_SENTIMENT]->(s:Sentiment)
        WITH s.label AS sentiment, sum(hs.count) AS total
        WITH collect({sentiment: sentiment, count: total}) AS dist,
             sum(total) AS grand_total
        UNWIND dist AS d
        RETURN d.sentiment AS sentiment, d.count AS count,
               grand_total,
               round(100.0 * d.count / grand_total, 1) AS pct
        ORDER BY d.count DESC
        LIMIT 1
    """)
    pos = run_single("""
        MATCH (u:User)-[hs:HAS_SENTIMENT]->(s:Sentiment {label: 'Positive'})
        RETURN sum(hs.count) AS positive
    """)
    neg = run_single("""
        MATCH (u:User)-[hs:HAS_SENTIMENT]->(s:Sentiment {label: 'Negative'})
        RETURN sum(hs.count) AS negative
    """)
    total_users = run_single("MATCH (u:User) RETURN count(u) AS n")
    total_posts = run_single("MATCH (p:Post) RETURN count(p) AS n")
    active_7d = run_single("""
        MATCH (u:User) WHERE u.last_seen > datetime() - duration('P7D')
        RETURN count(u) AS n
    """)

    p = (pos or {}).get("positive", 0) or 0
    n = (neg or {}).get("negative", 0) or 0
    ratio = p / max(p + n, 1)
    score = round(ratio * 100)

    return {
        "score": score,
        "trend": "up" if score > 50 else "down",
        "activeUsers": (active_7d or {}).get("n", 0),
        "totalPosts": (total_posts or {}).get("n", 0),
        "totalUsers": (total_users or {}).get("n", 0),
        "dominantSentiment": (row or {}).get("sentiment", "Neutral"),
    }


def get_trending_topics(limit: int = 10) -> list[dict]:
    """Topics with measurable 14d trend vs previous 14d."""
    rows = run_query("""
        MATCH (p:Post)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE p.posted_at > datetime() - duration('P28D')
        WITH t, cat,
             count(CASE WHEN p.posted_at > datetime() - duration('P14D') THEN 1 END) AS mentions14d,
             count(CASE WHEN p.posted_at > datetime() - duration('P28D')
                         AND p.posted_at <= datetime() - duration('P14D') THEN 1 END) AS mentionsPrev14d,
             avg(CASE WHEN p.posted_at > datetime() - duration('P14D') THEN p.views END) AS avgViews14d
        WHERE mentions14d > 0
        RETURN t.name AS topic,
               cat.name AS category,
               mentions14d AS mentions,
               round(coalesce(avgViews14d, 0)) AS avgViews,
               mentions14d AS currentMentions,
               mentionsPrev14d AS previousMentions,
               CASE
                   WHEN mentionsPrev14d > 0
                   THEN round(100.0 * (mentions14d - mentionsPrev14d) / mentionsPrev14d, 1)
                   WHEN mentions14d > 0 THEN 100.0
                   ELSE 0.0
               END AS trendPct
        ORDER BY mentions14d DESC
        LIMIT $limit
    """, {"limit": limit})
    return [
        {
            "name": r["topic"], "category": r["category"],
            "mentions": r["mentions"], "avgViews": r.get("avgViews", 0),
            "trendPct": r.get("trendPct", 0),
            "currentMentions": r.get("currentMentions", 0),
            "previousMentions": r.get("previousMentions", 0),
        }
        for r in rows
    ]


def get_community_brief() -> dict:
    """Summary stats for the AI-generated brief."""
    stats = run_single("""
        MATCH (p:Post) WHERE p.posted_at > datetime() - duration('P1D')
        WITH count(p) AS posts24h
        OPTIONAL MATCH (c:Comment) WHERE c.posted_at > datetime() - duration('P1D')
        WITH posts24h, count(c) AS comments24h
        OPTIONAL MATCH (u:User) WHERE u.last_seen > datetime() - duration('P1D')
        RETURN posts24h, comments24h, count(u) AS activeUsers24h
    """)

    new_members = run_single("""
        MATCH (u:User)-[:WROTE]->(c:Comment)
        WITH u, min(c.posted_at) AS firstActivity
        RETURN
          count(CASE WHEN firstActivity > datetime() - duration('P7D') THEN 1 END) AS newUsers7d,
          count(CASE WHEN firstActivity > datetime() - duration('P14D')
                     AND firstActivity <= datetime() - duration('P7D') THEN 1 END) AS newUsersPrev7d
    """)

    new_users_7d = (new_members or {}).get("newUsers7d", 0) or 0
    new_users_prev_7d = (new_members or {}).get("newUsersPrev7d", 0) or 0
    if new_users_prev_7d > 0:
        new_users_growth_pct = round(100.0 * (new_users_7d - new_users_prev_7d) / new_users_prev_7d, 1)
    elif new_users_7d > 0:
        new_users_growth_pct = 100.0
    else:
        new_users_growth_pct = 0.0

    top_topics = get_trending_topics(5)
    return {
        "postsLast24h": (stats or {}).get("posts24h", 0),
        "commentsLast24h": (stats or {}).get("comments24h", 0),
        "activeUsersLast24h": (stats or {}).get("activeUsers24h", 0),
        "newActiveUsers7d": new_users_7d,
        "newActiveUsersPrev7d": new_users_prev_7d,
        "newActiveUsersGrowthPct": new_users_growth_pct,
        "topTopics": [t["name"] for t in top_topics],
    }
