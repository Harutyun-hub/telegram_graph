"""
psychographic.py — Tier 5: Who Are the People (audience intelligence)

Provides: personas, interests, origins, integrationData, newcomerJourney
"""
from __future__ import annotations
from api.db import run_query


def get_personas() -> list[dict]:
    """User clusters by community_role + communication_style."""
    return run_query("""
        MATCH (u:User)
        WITH u.community_role AS role,
             u.communication_style AS style,
             u.inferred_age_bracket AS age,
             u.inferred_gender AS gender,
             count(u) AS count
        WHERE role IS NOT NULL
        RETURN role, style, age, gender, count
        ORDER BY count DESC
    """)


def get_interests() -> list[dict]:
    """Topic interest distribution across users."""
    return run_query("""
        MATCH (u:User)-[i:INTERESTED_IN]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WITH cat.name AS category, t.name AS topic,
             count(DISTINCT u) AS users, sum(i.count) AS interactions
        RETURN category, topic, users, interactions
        ORDER BY users DESC
    """)


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
