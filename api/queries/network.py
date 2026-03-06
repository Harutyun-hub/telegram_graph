"""
network.py — Tier 4: Channels, Voices & Activity

Provides: communityChannels, keyVoices, hourlyActivity, weeklyActivity,
          recommendations, viralTopics
"""
from __future__ import annotations
from api.db import run_query


def get_community_channels() -> list[dict]:
    """All channels with post counts, top topics, activity stats."""
    return run_query("""
        MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)
        WITH ch, count(p) AS postCount,
             avg(p.views) AS avgViews,
             max(p.posted_at) AS lastPost
        OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p2:Post)-[:TAGGED]->(t:Topic)
        WITH ch, postCount, round(avgViews) AS avgViews, lastPost,
             t.name AS topic, count(p2) AS topicPosts
        ORDER BY topicPosts DESC
        WITH ch, postCount, avgViews, lastPost,
             collect(topic)[..5] AS topTopics
        RETURN ch.username AS username, ch.title AS title,
               ch.member_count AS memberCount,
               ch.description AS description,
               postCount, avgViews,
               toString(lastPost) AS lastPost,
               topTopics
        ORDER BY postCount DESC
    """)


def get_key_voices() -> list[dict]:
    """Most active/influential users by comment count + reply network."""
    return run_query("""
        MATCH (u:User)-[w:WROTE]->(c:Comment)
        WITH u, count(c) AS commentCount
        OPTIONAL MATCH (u)-[r:REPLIED_TO_USER]->()
        WITH u, commentCount,
             count(r) AS replyCount,
             u.community_role AS role,
             u.communication_style AS style,
             u.inferred_gender AS gender,
             u.inferred_age_bracket AS age
        RETURN u.telegram_user_id AS userId,
               commentCount, replyCount,
               role, style, gender, age,
               commentCount + replyCount * 2 AS influenceScore
        ORDER BY influenceScore DESC
        LIMIT 20
    """)


def get_hourly_activity() -> list[dict]:
    """Comment distribution by hour of day."""
    return run_query("""
        MATCH (c:Comment)
        WHERE c.posting_hour IS NOT NULL
        WITH c.posting_hour AS hour, count(c) AS count
        RETURN hour, count
        ORDER BY hour
    """)


def get_weekly_activity() -> list[dict]:
    """Post count by day of week."""
    return run_query("""
        MATCH (p:Post)
        WITH date(p.posted_at).dayOfWeek AS dow, count(p) AS count
        RETURN dow, count
        ORDER BY dow
    """)


def get_recommendations() -> list[dict]:
    """Users with Support/Help intent — recommenders."""
    return run_query("""
        MATCH (u:User)-[e:EXHIBITS]->(i:Intent {name: 'Support / Help'})
        MATCH (u)-[:INTERESTED_IN]->(t:Topic)
        WITH u.telegram_user_id AS userId, e.count AS helpCount,
             collect(t.name)[..3] AS topics
        RETURN userId, helpCount, topics
        ORDER BY helpCount DESC
        LIMIT 15
    """)


def get_viral_topics() -> list[dict]:
    """Topics with most co-occurrences — information spread indicators."""
    return run_query("""
        MATCH (t1:Topic)-[r:CO_OCCURS_WITH]-(t2:Topic)
        WITH t1.name AS topic, sum(r.count) AS coOccurrences,
             count(DISTINCT t2) AS connectedTopics
        RETURN topic, coOccurrences, connectedTopics
        ORDER BY coOccurrences DESC
        LIMIT 15
    """)
