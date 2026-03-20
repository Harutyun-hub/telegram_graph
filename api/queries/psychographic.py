"""
psychographic.py — Tier 5: Who Are the People (audience intelligence)

Provides: personas, interests, origins, integrationData, newcomerJourney
"""
from __future__ import annotations
from api.dashboard_dates import DashboardDateContext
from api.db import run_query


def get_personas(ctx: DashboardDateContext) -> list[dict]:
    """Active-user persona clusters in the selected window."""
    return run_query("""
        MATCH (u:User)-[i:INTERESTED_IN]->(:Topic)
        WHERE i.last_seen >= datetime($start)
          AND i.last_seen < datetime($end)
        WITH DISTINCT u
        WITH coalesce(u.community_role, 'Member') AS role,
             u.communication_style AS style,
             u.inferred_age_bracket AS age,
             u.inferred_gender AS gender,
             count(*) AS count
        RETURN role, style, age, gender, count
        ORDER BY count DESC
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
    })


def get_interests(ctx: DashboardDateContext) -> list[dict]:
    """Selected-window interest penetration across active users."""
    return run_query("""
        CALL () {
            MATCH (u:User)-[i:INTERESTED_IN]->(:Topic)
            WHERE i.last_seen >= datetime($start)
              AND i.last_seen < datetime($end)
            RETURN count(DISTINCT u) AS activeUsers
        }
        MATCH (u:User)-[i:INTERESTED_IN]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE i.last_seen >= datetime($start)
          AND i.last_seen < datetime($end)
          AND coalesce(cat.name, 'General') <> 'General'
          AND t.name IS NOT NULL
          AND trim(t.name) <> ''
          AND NOT toLower(trim(t.name)) IN ['null', 'none', 'unknown']
        WITH activeUsers, cat.name AS category,
             count(DISTINCT u) AS users,
             sum(coalesce(i.count, 1)) AS interactions
        WHERE activeUsers > 0 AND users > 0
        RETURN category,
               users,
               activeUsers,
               interactions,
               round((toFloat(users) / toFloat(activeUsers)) * 1000) / 10.0 AS penetrationPct,
               round((toFloat(interactions) / toFloat(users)) * 10) / 10.0 AS avgInteractionsPerUser
        ORDER BY penetrationPct DESC, users DESC, category ASC
        LIMIT 8
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
    })


def get_origins() -> list[dict]:
    """Inferred demographics distribution."""
    return run_query("""
        MATCH (u:User)
        WITH u.language AS language,
             u.inferred_age_bracket AS age,
             u.inferred_gender AS gender,
             count(u) AS count
        RETURN language, age, gender, count
        ORDER BY count DESC
    """)


def get_integration_data() -> list[dict]:
    """Language and code-switching patterns — integration indicator."""
    return run_query("""
        MATCH (u:User)
        WITH u.language AS language,
             u.code_switching AS codeSwitching,
             u.certainty_level AS certainty,
             count(u) AS count
        RETURN language, codeSwitching, certainty, count
        ORDER BY count DESC
    """)


def get_newcomer_journey() -> list[dict]:
    """Users ordered by first activity, showing topic evolution."""
    return run_query("""
        MATCH (u:User)-[w:WROTE]->(c:Comment)
        WITH u, min(c.posted_at) AS firstSeen, count(c) AS commentCount
        MATCH (u)-[:INTERESTED_IN]->(t:Topic)
        WITH u.telegram_user_id AS userId, firstSeen, commentCount,
             collect(t.name)[..5] AS topics,
             u.migration_intent AS migrationIntent,
             u.community_role AS role
        RETURN userId, toString(firstSeen) AS firstSeen,
               commentCount, topics, migrationIntent, role
        ORDER BY firstSeen DESC
        LIMIT 30
    """)
