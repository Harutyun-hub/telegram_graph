"""
network.py — Tier 4: Channels, Voices & Activity

Provides: communityChannels, keyVoices, hourlyActivity, weeklyActivity,
          recommendations, viralTopics
"""
from __future__ import annotations
from api.db import run_query
try:
    from utils.channel_classifier import classify_channel, calculate_engagement_score, calculate_growth_rate
except ImportError:
    # Fallback if classifier not available
    def classify_channel(*args, **kwargs):
        return 'General'
    def calculate_engagement_score(*args, **kwargs):
        return 50.0
    def calculate_growth_rate(*args, **kwargs):
        return 0.0


def get_community_channels() -> list[dict]:
    """All channels with engagement metrics, growth rates, and activity stats."""
    results = run_query("""
        MATCH (all_p:Post)
        WITH max(all_p.posted_at) AS globalLatest
        MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)
        WITH ch, globalLatest, count(p) AS postCount,
             avg(p.views) AS avgViews,
             avg(p.forwards) AS avgForwards,
             avg(p.comments) AS avgComments,
             max(p.posted_at) AS lastPost,
             sum(CASE WHEN p.posted_at >= globalLatest - duration('P30D') THEN 1 ELSE 0 END) AS posts30d,
             sum(CASE WHEN p.posted_at >= globalLatest - duration('P7D') THEN 1 ELSE 0 END) AS posts7d,
             sum(CASE WHEN p.posted_at >= globalLatest - duration('P14D') AND p.posted_at < globalLatest - duration('P7D') THEN 1 ELSE 0 END) AS posts14to7d
        WHERE posts30d > 0  // Filter out inactive channels
        OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p2:Post)-[:TAGGED]->(t:Topic)
        WITH ch, postCount, avgViews, avgForwards, avgComments, lastPost, posts30d, posts7d, posts14to7d,
             t.name AS topic, count(p2) AS topicPosts
        ORDER BY topicPosts DESC
        WITH ch, postCount, avgViews, avgForwards, avgComments, lastPost, posts30d, posts7d, posts14to7d,
             collect(topic)[..5] AS topTopics
        WITH ch.username AS username,
             ch.title AS name,
             ch.member_count AS members,
             ch.description AS description,
             postCount,
             round(avgViews) AS avgViews,
             round(avgForwards) AS avgForwards,
             round(avgComments) AS avgComments,
             posts7d, posts14to7d, posts30d,
             toString(lastPost) AS lastPost,
             topTopics,
             // Calculate daily messages average
             CASE WHEN posts7d > 0 THEN round(posts7d / 7.0, 1) ELSE 0 END AS dailyMessages,
             // Calculate engagement rate (normalized to 0-100)
             CASE
                 WHEN ch.member_count > 0 THEN
                     round(((avgViews + avgForwards * 2 + avgComments * 3) / ch.member_count) * 100, 1)
                 ELSE 0
             END AS engagement,
             // Calculate growth percentage
             CASE
                 WHEN posts14to7d > 0 THEN
                     round(((posts7d - posts14to7d) * 100.0 / posts14to7d), 1)
                 ELSE
                     CASE WHEN posts7d > 0 THEN 100 ELSE 0 END
             END AS growth,
             // Classify channel type based on description and topics
             CASE
                 WHEN ch.description =~ '(?i).*(work|job|career|employment|recruit).*' OR
                      ANY(t IN topTopics WHERE t =~ '(?i).*(work|job|career|employment).*') THEN 'Work'
                 WHEN ch.description =~ '(?i).*(family|children|parent|школ).*' OR
                      ANY(t IN topTopics WHERE t =~ '(?i).*(family|children|parent).*') THEN 'Family'
                 WHEN ch.description =~ '(?i).*(housing|rent|apartment|недвижимость|квартир).*' OR
                      ANY(t IN topTopics WHERE t =~ '(?i).*(housing|rent|apartment|real estate).*') THEN 'Housing'
                 WHEN ch.description =~ '(?i).*(business|entrepreneur|startup|бизнес).*' OR
                      ANY(t IN topTopics WHERE t =~ '(?i).*(business|entrepreneur|startup).*') THEN 'Business'
                 WHEN ch.description =~ '(?i).*(legal|law|visa|document|легал).*' OR
                      ANY(t IN topTopics WHERE t =~ '(?i).*(legal|law|visa|immigration).*') THEN 'Legal'
                 WHEN ch.description =~ '(?i).*(lifestyle|food|restaurant|entertainment|досуг).*' OR
                      ANY(t IN topTopics WHERE t =~ '(?i).*(lifestyle|food|entertainment).*') THEN 'Lifestyle'
                 ELSE 'General'
             END AS type,
             topTopics[0] AS topTopicEN,
             topTopics[0] AS topTopicRU
        RETURN username, name, type, members, dailyMessages, engagement, growth,
               topTopicEN, topTopicRU, description, postCount, avgViews,
               avgForwards, avgComments, posts7d, posts14to7d, lastPost, topTopics
        ORDER BY engagement DESC
        LIMIT 50
    """)

    # Post-process results to ensure data quality and enhance classification
    processed_results = []
    for channel in results:
        # Enhanced type classification using the utility
        if channel.get('type') == 'General' or not channel.get('type'):
            channel['type'] = classify_channel(
                title=channel.get('name'),
                description=channel.get('description'),
                topics=channel.get('topTopics', []),
                threshold=0.2  # Lower threshold for better matching
            )

        # Ensure engagement is within valid range
        if channel.get('engagement'):
            channel['engagement'] = min(max(channel['engagement'], 0), 100)

        # Ensure growth is a reasonable number
        if channel.get('growth') is not None:
            channel['growth'] = min(max(channel['growth'], -100), 1000)

        # Ensure required fields exist with defaults
        channel.setdefault('name', f"Channel_{channel.get('username', 'Unknown')}")
        channel.setdefault('type', 'General')
        channel.setdefault('members', 0)
        channel.setdefault('dailyMessages', 0)
        channel.setdefault('engagement', 0)
        channel.setdefault('growth', 0)

        processed_results.append(channel)

    return processed_results


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
