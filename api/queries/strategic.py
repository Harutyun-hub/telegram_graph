"""
strategic.py — Tier 2: What People Talk About (deep topic intelligence)

Provides: topicBubbles, trendLines, heatmap, questionCategories, lifecycleStages
"""
from __future__ import annotations
from api.db import run_query


def get_topic_bubbles() -> list[dict]:
    """Topic bubble chart: name, category, mention count, sentiment."""
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
        WITH t, cat,
             count(DISTINCT p) AS postMentions,
             count(CASE WHEN p.posted_at > datetime() - duration('P7D') THEN 1 END) AS posts7d,
             count(CASE WHEN p.posted_at > datetime() - duration('P14D')
                         AND p.posted_at <= datetime() - duration('P7D') THEN 1 END) AS postsPrev7d
        OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
        WITH t, cat, postMentions, posts7d, postsPrev7d,
             count(DISTINCT c) AS commentMentions,
             count(CASE WHEN c.posted_at > datetime() - duration('P7D') THEN 1 END) AS comments7d,
             count(CASE WHEN c.posted_at > datetime() - duration('P14D')
                         AND c.posted_at <= datetime() - duration('P7D') THEN 1 END) AS commentsPrev7d
        OPTIONAL MATCH (u:User)-[i:INTERESTED_IN]->(t)
        WITH t.name AS name, cat.name AS category,
             postMentions, commentMentions,
             count(DISTINCT u) AS userInterest,
             coalesce(sum(i.count), 0) AS totalInteractions,
             (posts7d + comments7d) AS mentions7d,
             (postsPrev7d + commentsPrev7d) AS mentionsPrev7d
        RETURN name, category, postMentions, commentMentions,
               (postMentions + commentMentions) AS mentionCount,
               userInterest, totalInteractions,
               mentions7d, mentionsPrev7d,
               CASE
                   WHEN mentionsPrev7d > 0
                   THEN round(100.0 * (mentions7d - mentionsPrev7d) / mentionsPrev7d, 1)
                   WHEN mentions7d > 0 THEN 100.0
                   ELSE 0.0
               END AS growth7dPct
        ORDER BY postMentions DESC
    """)


def get_trend_lines() -> list[dict]:
    """Weekly post counts per topic for the last 8 weeks."""
    return run_query("""
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        WHERE p.posted_at > datetime() - duration('P56D')
        WITH t.name AS topic,
             date(p.posted_at).week AS week,
             date(p.posted_at).year AS year,
             count(p) AS posts
        RETURN topic, year, week, posts
        ORDER BY topic, year, week
    """)


def get_heatmap() -> list[dict]:
    """Content type × topic matrix (media_type distribution per topic)."""
    return run_query("""
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        WITH t.name AS topic, coalesce(p.media_type, 'text') AS mediaType,
             count(p) AS count
        RETURN topic, mediaType, count
        ORDER BY topic, count DESC
    """)


def get_question_categories() -> list[dict]:
    """Information-seeking by topic + response coverage proxy (14d)."""
    return run_query("""
        MATCH (u:User)-[:EXHIBITS]->(i:Intent {name: 'Information Seeking'})
        MATCH (u)-[:INTERESTED_IN]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        OPTIONAL MATCH (:User)-[r:REPLIED_TO_USER]->(u)
        WHERE r.last_seen > datetime() - duration('P14D')
        WITH cat.name AS category, t.name AS topic,
             count(DISTINCT u) AS seekers,
             count(DISTINCT CASE WHEN r IS NOT NULL THEN u END) AS respondedSeekers
        RETURN category, topic, seekers, respondedSeekers,
               CASE
                   WHEN seekers > 0 THEN round(100.0 * respondedSeekers / seekers, 1)
                   ELSE 0.0
               END AS coveragePct
        ORDER BY seekers DESC
    """)


def get_lifecycle_stages() -> list[dict]:
    """Topic lifecycle: first seen, last seen, post momentum."""
    return run_query("""
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        WITH t.name AS topic,
             min(p.posted_at) AS firstSeen,
             max(p.posted_at) AS lastSeen,
             count(p) AS totalPosts
        OPTIONAL MATCH (p2:Post)-[:TAGGED]->(t2:Topic {name: topic})
        WHERE p2.posted_at > datetime() - duration('P7D')
        WITH topic, firstSeen, lastSeen, totalPosts,
             count(p2) AS recentPosts
        RETURN topic,
               toString(firstSeen) AS firstSeen,
               toString(lastSeen) AS lastSeen,
               totalPosts, recentPosts,
               CASE
                 WHEN recentPosts = 0 THEN 'dormant'
                 WHEN duration.between(firstSeen, datetime()).days < 14 THEN 'emerging'
                 WHEN recentPosts > totalPosts * 0.3 THEN 'peaking'
                 ELSE 'established'
               END AS stage
        ORDER BY recentPosts DESC
    """)
