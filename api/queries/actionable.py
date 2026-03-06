"""
actionable.py — Tier 7: Business Intelligence & Opportunities

Provides: businessOpportunities, jobSeeking, jobTrends, housingData
"""
from __future__ import annotations
from api.db import run_query


def get_business_opportunities() -> list[dict]:
    """Business opportunity signals from user behavior."""
    return run_query("""
        MATCH (u:User)-[:SIGNALS_OPPORTUNITY]->(b:BusinessOpportunity)
        WITH b.type AS type, b.description AS description,
             count(u) AS signals
        OPTIONAL MATCH (u2:User)-[:SIGNALS_OPPORTUNITY]->(b2:BusinessOpportunity {type: type})
        OPTIONAL MATCH (u2)-[:INTERESTED_IN]->(t:Topic)
        WITH type, description, signals,
             collect(DISTINCT t.name)[..5] AS relatedTopics
        RETURN type, description, signals, relatedTopics
        ORDER BY signals DESC
    """)


def get_job_seeking() -> list[dict]:
    """Users signaling job-related opportunities."""
    return run_query("""
        MATCH (u:User)-[:SIGNALS_OPPORTUNITY]->(b:BusinessOpportunity)
        WHERE b.type IN ['Job_Seeking', 'Hiring']
        WITH u.telegram_user_id AS userId,
             b.type AS signalType,
             u.inferred_age_bracket AS age,
             u.community_role AS role,
             u.financial_distress_level AS distress
        RETURN userId, signalType, age, role, distress
        ORDER BY signalType
    """)


def get_job_trends() -> list[dict]:
    """Job-related topic trends over time."""
    return run_query("""
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        WHERE t.name IN ['Employment', 'Business Opportunity', 'Investment Opportunity']
        WITH date(p.posted_at).week AS week,
             date(p.posted_at).year AS year,
             t.name AS topic,
             count(p) AS posts
        RETURN year, week, topic, posts
        ORDER BY year, week
    """)


def get_housing_data() -> list[dict]:
    """Housing-related topics and user interest."""
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE cat.name = 'Economy' AND t.name IN ['Housing Market', 'Investment Opportunity']
        OPTIONAL MATCH (u:User)-[i:INTERESTED_IN]->(t)
        OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
        WITH t.name AS topic, count(DISTINCT u) AS interestedUsers,
             count(DISTINCT p) AS posts,
             sum(i.count) AS interactions
        RETURN topic, interestedUsers, posts, interactions
    """)
