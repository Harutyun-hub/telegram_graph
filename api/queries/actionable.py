"""
actionable.py — Tier 7: Business Intelligence & Opportunities

Provides: businessOpportunities, jobSeeking, jobTrends, housingData
"""
from __future__ import annotations
from api.dashboard_dates import DashboardDateContext
from api.db import run_query


def get_business_opportunities(ctx: DashboardDateContext) -> list[dict]:
    """Business opportunity signals among users active in the selected window."""
    return run_query("""
        MATCH (u:User)-[:SIGNALS_OPPORTUNITY]->(b:BusinessOpportunity)
        WHERE EXISTS {
            MATCH (u)-[i:INTERESTED_IN]->(:Topic)
            WHERE i.last_seen >= datetime($start)
              AND i.last_seen < datetime($end)
        }
        WITH b.type AS type, b.description AS description,
             count(DISTINCT u) AS signals
        OPTIONAL MATCH (u2:User)-[:SIGNALS_OPPORTUNITY]->(b2:BusinessOpportunity {type: type})
        WHERE EXISTS {
            MATCH (u2)-[i2:INTERESTED_IN]->(:Topic)
            WHERE i2.last_seen >= datetime($previous_start)
              AND i2.last_seen < datetime($previous_end)
        }
        OPTIONAL MATCH (u)-[:INTERESTED_IN]->(t:Topic)
        WITH type, description, signals,
             count(DISTINCT u2) AS previousSignals,
             collect(DISTINCT t.name)[..5] AS relatedTopics
        RETURN type, description, signals, previousSignals, relatedTopics
        ORDER BY signals DESC
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
    })


def get_job_seeking(ctx: DashboardDateContext) -> list[dict]:
    """Active users signaling job-related opportunities."""
    return run_query("""
        MATCH (u:User)-[:SIGNALS_OPPORTUNITY]->(b:BusinessOpportunity)
        WHERE b.type IN ['Job_Seeking', 'Hiring']
          AND EXISTS {
              MATCH (u)-[i:INTERESTED_IN]->(:Topic)
              WHERE i.last_seen >= datetime($start)
                AND i.last_seen < datetime($end)
          }
        WITH u.telegram_user_id AS userId,
             b.type AS signalType,
             u.inferred_age_bracket AS age,
             u.community_role AS role,
             u.financial_distress_level AS distress
        RETURN userId, signalType, age, role, distress
        ORDER BY signalType
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
    })


def get_job_trends(ctx: DashboardDateContext) -> list[dict]:
    """Selected-window job-related opportunity trends."""
    return run_query("""
        MATCH (u:User)-[:SIGNALS_OPPORTUNITY]->(b:BusinessOpportunity)
        WHERE b.type IN ['Job_Seeking', 'Hiring', 'Partnership_Request']
          AND EXISTS {
              MATCH (u)-[cur:INTERESTED_IN]->(:Topic)
              WHERE cur.last_seen >= datetime($start)
                AND cur.last_seen < datetime($end)
          }
        WITH b.type AS topic, count(DISTINCT u) AS currentUsers
        CALL (topic) {
            MATCH (u:User)-[:SIGNALS_OPPORTUNITY]->(b:BusinessOpportunity {type: topic})
            WHERE EXISTS {
                MATCH (u)-[prev:INTERESTED_IN]->(:Topic)
                WHERE prev.last_seen >= datetime($previous_start)
                  AND prev.last_seen < datetime($previous_end)
            }
            RETURN count(DISTINCT u) AS previousUsers
        }
        RETURN topic, currentUsers, previousUsers
        ORDER BY currentUsers DESC, topic ASC
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
    })


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
