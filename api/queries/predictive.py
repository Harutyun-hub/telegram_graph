"""
predictive.py — Tier 6: Predictive Intelligence

Provides: emergingInterests, retentionFactors, churnSignals, growthFunnel,
          decisionStages
"""
from __future__ import annotations
from api.db import run_query


def get_emerging_interests() -> list[dict]:
    """Topics that appeared recently with growing momentum."""
    return run_query("""
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        WITH t.name AS topic,
             min(p.posted_at) AS firstSeen,
             count(p) AS totalPosts
        WHERE duration.between(firstSeen, datetime()).days < 30
        OPTIONAL MATCH (p2:Post)-[:TAGGED]->(t2:Topic {name: topic})
        WHERE p2.posted_at > datetime() - duration('P7D')
        WITH topic, firstSeen, totalPosts, count(p2) AS recentPosts
        RETURN topic, toString(firstSeen) AS firstSeen,
               totalPosts, recentPosts,
               round(100.0 * recentPosts / totalPosts, 1) AS momentum
        ORDER BY momentum DESC
        LIMIT 15
    """)


def get_retention_factors() -> list[dict]:
    """Topics/intents that correlate with user retention (multi-comment users)."""
    return run_query("""
        MATCH (u:User)-[:WROTE]->(c:Comment)
        WITH u, count(c) AS totalComments
        WHERE totalComments >= 3
        MATCH (u)-[:INTERESTED_IN]->(t:Topic)
        WITH t.name AS topic, count(u) AS retainedUsers, avg(totalComments) AS avgComments
        RETURN topic, retainedUsers, round(avgComments, 1) AS avgComments
        ORDER BY retainedUsers DESC
        LIMIT 15
    """)


def get_churn_signals() -> list[dict]:
    """Users who were active but stopped (no activity in 14+ days)."""
    return run_query("""
        MATCH (u:User)-[:WROTE]->(c:Comment)
        WITH u, max(c.posted_at) AS lastActivity, count(c) AS totalComments
        WHERE lastActivity < datetime() - duration('P14D')
          AND totalComments >= 2
        MATCH (u)-[:INTERESTED_IN]->(t:Topic)
        WITH u.telegram_user_id AS userId,
             toString(lastActivity) AS lastActivity,
             totalComments,
             u.community_role AS role,
             collect(t.name)[..3] AS topics
        RETURN userId, lastActivity, totalComments, role, topics
        ORDER BY totalComments DESC
        LIMIT 20
    """)


def get_growth_funnel() -> list[dict]:
    """Users bucketed by engagement level."""
    return run_query("""
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)
        WITH u, count(c) AS comments
        RETURN
          CASE
            WHEN comments = 0 THEN 'Lurker'
            WHEN comments <= 2 THEN 'Newcomer'
            WHEN comments <= 5 THEN 'Participant'
            WHEN comments <= 15 THEN 'Regular'
            ELSE 'Power User'
          END AS stage,
          count(u) AS users
        ORDER BY users DESC
    """)


def get_decision_stages() -> list[dict]:
    """Migration intent distribution — decision pipeline."""
    return run_query("""
        MATCH (u:User)
        WHERE u.migration_intent IS NOT NULL
        WITH u.migration_intent AS intent, count(u) AS users,
             collect(u.inferred_age_bracket)[..5] AS ageDistribution
        RETURN intent, users, ageDistribution
        ORDER BY users DESC
    """)
