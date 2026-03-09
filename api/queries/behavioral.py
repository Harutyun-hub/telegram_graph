"""
behavioral.py — Tier 3: Problems & Satisfaction (pain point monitoring)

Provides: problems, serviceGaps, satisfactionAreas, moodData, urgencySignals
"""
from __future__ import annotations
from api.db import run_query


def get_problems() -> list[dict]:
    """Users with negative/urgent sentiment grouped by topic + 7d trend."""
    return run_query("""
        MATCH (u:User)-[i:INTERESTED_IN]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        MATCH (u)-[hs:HAS_SENTIMENT]->(s:Sentiment)
        WHERE s.label IN ['Negative', 'Urgent']
        WITH t, cat,
             count(DISTINCT u) AS affectedUsers,
             count(DISTINCT CASE WHEN s.label = 'Urgent' THEN u END) AS urgentUsers,
             count(DISTINCT CASE WHEN hs.last_seen > datetime() - duration('P7D')
                                   AND i.last_seen > datetime() - duration('P7D')
                                 THEN u END) AS affectedThisWeek,
              count(DISTINCT CASE WHEN hs.last_seen > datetime() - duration('P14D')
                                    AND hs.last_seen <= datetime() - duration('P7D')
                                    AND i.last_seen > datetime() - duration('P14D')
                                    AND i.last_seen <= datetime() - duration('P7D')
                                  THEN u END) AS affectedPrevWeek
        OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
        WHERE p.posted_at > datetime() - duration('P30D')
          AND p.text IS NOT NULL
          AND trim(p.text) <> ''
        WITH t, cat, affectedUsers, affectedThisWeek, affectedPrevWeek, urgentUsers,
             collect(DISTINCT left(trim(p.text), 180))[0] AS samplePostText
        OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
        WHERE c.posted_at > datetime() - duration('P30D')
          AND c.text IS NOT NULL
          AND trim(c.text) <> ''
        WITH t, cat, affectedUsers, affectedThisWeek, affectedPrevWeek, urgentUsers, samplePostText,
             collect(DISTINCT left(trim(c.text), 180))[0] AS sampleCommentText
        RETURN t.name AS topic,
               cat.name AS category,
               affectedUsers,
               affectedThisWeek,
               affectedPrevWeek,
               (affectedThisWeek + affectedPrevWeek) AS trendSupport,
               coalesce(samplePostText, sampleCommentText, '') AS sampleText,
               CASE WHEN urgentUsers > 0 THEN 'Urgent' ELSE 'Negative' END AS severity,
               CASE
                    WHEN (affectedThisWeek + affectedPrevWeek) < 8 THEN null
                    ELSE round(100.0 * (affectedThisWeek - affectedPrevWeek) / (affectedPrevWeek + 3), 1)
                END AS trendPct
        ORDER BY affectedUsers DESC
        LIMIT 20
    """)


def get_service_gaps() -> list[dict]:
    """Topics with high interest but mostly negative sentiment — supply gaps."""
    return run_query("""
        MATCH (u:User)-[i:INTERESTED_IN]->(t:Topic)
        WITH t,
             count(DISTINCT u) AS demand,
             count(DISTINCT CASE WHEN i.last_seen > datetime() - duration('P7D') THEN u END) AS demandThisWeek,
             count(DISTINCT CASE WHEN i.last_seen > datetime() - duration('P14D')
                                   AND i.last_seen <= datetime() - duration('P7D') THEN u END) AS demandPrevWeek
        OPTIONAL MATCH (u2:User)-[:INTERESTED_IN]->(t)
        OPTIONAL MATCH (u2)-[hs:HAS_SENTIMENT]->(s:Sentiment)
        WITH t.name AS topic, demand, demandThisWeek, demandPrevWeek,
             sum(CASE WHEN s.label = 'Negative' THEN hs.count ELSE 0 END) AS negCount,
             sum(CASE WHEN s.label = 'Positive' THEN hs.count ELSE 0 END) AS posCount
        WHERE demand > 3
        RETURN topic, demand,
               negCount, posCount,
               demandThisWeek, demandPrevWeek,
               (demandThisWeek + demandPrevWeek) AS demandGrowthSupport,
               round(100.0 * negCount / (negCount + posCount + 1), 1) AS dissatisfactionPct,
               CASE
                    WHEN (demandThisWeek + demandPrevWeek) < 8 THEN null
                    ELSE round(100.0 * (demandThisWeek - demandPrevWeek) / (demandPrevWeek + 3), 1)
                END AS demandGrowthPct
        ORDER BY dissatisfactionPct DESC
        LIMIT 15
    """)


def get_satisfaction_areas() -> list[dict]:
    """Satisfaction scores per TopicCategory."""
    return run_query("""
        MATCH (u:User)-[:INTERESTED_IN]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        OPTIONAL MATCH (u)-[hs:HAS_SENTIMENT]->(s:Sentiment)
        WITH cat.name AS category,
             sum(CASE WHEN s.label = 'Positive' THEN hs.count ELSE 0 END) AS pos,
             sum(CASE WHEN s.label = 'Negative' THEN hs.count ELSE 0 END) AS neg,
             sum(CASE WHEN s.label = 'Neutral' THEN hs.count ELSE 0 END) AS neu
        RETURN category, pos, neg, neu,
               round(100.0 * pos / (pos + neg + neu + 1), 1) AS satisfactionPct
        ORDER BY satisfactionPct DESC
    """)


def get_mood_data() -> list[dict]:
    """Weekly sentiment distribution for mood-over-time chart."""
    return run_query("""
        MATCH (c:Comment)-[:REPLIES_TO]->(p:Post)
        MATCH (u:User)-[:WROTE]->(c)
        MATCH (u)-[hs:HAS_SENTIMENT]->(s:Sentiment)
        WITH date(c.posted_at).week AS week,
             date(c.posted_at).year AS year,
             s.label AS sentiment,
             count(*) AS count
        RETURN year, week, sentiment, count
        ORDER BY year, week
    """)


def get_urgency_signals() -> list[dict]:
    """Topics flagged as urgent by users with 'Urgent' sentiment."""
    return run_query("""
        MATCH (u:User)-[:HAS_SENTIMENT]->(s:Sentiment {label: 'Urgent'})
        MATCH (u)-[:INTERESTED_IN]->(t:Topic)
        WITH t.name AS topic, count(DISTINCT u) AS urgentUsers
        RETURN topic, urgentUsers
        ORDER BY urgentUsers DESC
        LIMIT 10
    """)
