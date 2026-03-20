"""
predictive.py — Tier 6: Predictive Intelligence

Provides: emergingInterests, retentionFactors, churnSignals, growthFunnel,
          decisionStages
"""
from __future__ import annotations
from api.dashboard_dates import DashboardDateContext
from api.db import run_query


def get_emerging_interests(ctx: DashboardDateContext) -> list[dict]:
    """Topics gaining traction inside the selected window."""
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']
        CALL (t) {
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            RETURN count(DISTINCT p) AS currentPosts,
                   min(p.posted_at) AS firstSeen,
                   head([name IN collect(coalesce(ch.title, ch.username, '')) WHERE name <> '']) AS originChannel
        }
        CALL (t) {
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($previous_start)
              AND p.posted_at < datetime($previous_end)
            RETURN count(DISTINCT p) AS previousPosts
        }
        WITH t.name AS topic,
             currentPosts,
             previousPosts,
             firstSeen,
             coalesce(originChannel, 'Community channel') AS originChannel,
             (currentPosts + previousPosts) AS growthSupport
        WHERE currentPosts > 0
        RETURN topic,
               toString(firstSeen) AS firstSeen,
               currentPosts AS recentPosts,
               currentPosts AS totalPosts,
               currentPosts AS currentPosts,
               previousPosts,
               growthSupport,
               originChannel,
               CASE
                   WHEN growthSupport < 8 THEN null
                   ELSE round(100.0 * (currentPosts - previousPosts) / (previousPosts + 3), 1)
               END AS momentum
        ORDER BY currentPosts DESC, topic ASC
        LIMIT 15
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
    })


def get_retention_factors(ctx: DashboardDateContext) -> list[dict]:
    """Selected-window topics associated with users retained across matching windows."""
    return run_query("""
        MATCH (u:User)-[:WROTE]->(c:Comment)
        WHERE c.posted_at >= datetime($start)
          AND c.posted_at < datetime($end)
        WITH u, count(c) AS totalComments
        WHERE totalComments >= 3
          AND EXISTS {
              MATCH (u)-[:WROTE]->(prev:Comment)
              WHERE prev.posted_at >= datetime($previous_start)
                AND prev.posted_at < datetime($previous_end)
          }
        MATCH (u)-[:INTERESTED_IN]->(t:Topic)
        WITH t.name AS topic, count(DISTINCT u) AS retainedUsers, avg(totalComments) AS avgComments
        RETURN topic, retainedUsers, round(avgComments, 1) AS avgComments
        ORDER BY retainedUsers DESC
        LIMIT 15
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
    })


def get_churn_signals(ctx: DashboardDateContext) -> list[dict]:
    """Topics losing active contributors versus the previous matching window."""
    return run_query("""
        MATCH (u:User)-[:INTERESTED_IN]->(t:Topic)
        WHERE EXISTS {
            MATCH (u)-[prev:INTERESTED_IN]->(t)
            WHERE prev.last_seen >= datetime($previous_start)
              AND prev.last_seen < datetime($previous_end)
        }
        WITH u, t,
             EXISTS {
                 MATCH (u)-[cur:INTERESTED_IN]->(t)
                 WHERE cur.last_seen >= datetime($start)
                   AND cur.last_seen < datetime($end)
             } AS activeNow
        WHERE activeNow = false
        WITH t.name AS topic, count(DISTINCT u) AS lostUsers
        CALL (topic) {
            MATCH (u:User)-[prev:INTERESTED_IN]->(t:Topic {name: topic})
            WHERE prev.last_seen >= datetime($previous_start)
              AND prev.last_seen < datetime($previous_end)
            RETURN count(DISTINCT u) AS previousUsers
        }
        WITH topic, lostUsers, previousUsers, (lostUsers + previousUsers) AS growthSupport
        RETURN topic,
               lostUsers,
               previousUsers,
               growthSupport,
               CASE
                   WHEN growthSupport < 8 THEN null
                   ELSE round(100.0 * lostUsers / (previousUsers + 3), 1)
               END AS trendPct
        ORDER BY lostUsers DESC, topic ASC
        LIMIT 12
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
    })


def get_growth_funnel(ctx: DashboardDateContext) -> list[dict]:
    """Users bucketed by engagement level inside the selected window."""
    return run_query("""
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)
        WHERE c.posted_at >= datetime($start)
          AND c.posted_at < datetime($end)
        WITH u, count(c) AS comments
        WHERE comments > 0
        RETURN
          CASE
            WHEN comments <= 1 THEN 'Lurker'
            WHEN comments <= 2 THEN 'Newcomer'
            WHEN comments <= 5 THEN 'Participant'
            WHEN comments <= 15 THEN 'Regular'
            ELSE 'Power User'
          END AS stage,
          count(u) AS users
        ORDER BY users DESC
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
    })


def get_decision_stages(ctx: DashboardDateContext) -> list[dict]:
    """Migration intent distribution among active users in the selected window."""
    return run_query("""
        MATCH (u:User)-[i:INTERESTED_IN]->(:Topic)
        WHERE i.last_seen >= datetime($start)
          AND i.last_seen < datetime($end)
          AND u.migration_intent IS NOT NULL
        WITH DISTINCT u
        WITH u.migration_intent AS intent,
             count(*) AS users,
             collect(u.inferred_age_bracket)[..5] AS ageDistribution,
             count(CASE WHEN EXISTS {
                 MATCH (u)-[prev:INTERESTED_IN]->(:Topic)
                 WHERE prev.last_seen >= datetime($previous_start)
                   AND prev.last_seen < datetime($previous_end)
             } THEN 1 END) AS previousUsers
        RETURN intent, users, previousUsers, ageDistribution
        ORDER BY users DESC
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
    })
