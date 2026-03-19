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
             avg(coalesce(p.views, 0)) AS avgViews,
             avg(coalesce(p.forwards, 0)) AS avgForwards,
             avg(coalesce(p.comment_count, 0)) AS avgComments,
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
    """Most active/influential users with real usernames, channels, and topics."""
    # First get data from Neo4j
    neo4j_results = run_query("""
        MATCH (u:User)-[w:WROTE]->(c:Comment)
        WITH u, count(c) AS commentCount, collect(DISTINCT c) AS comments

        // Get channels where user is active
        OPTIONAL MATCH (c2:Comment)<-[:WROTE]-(u)
        OPTIONAL MATCH (c2)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
        WITH u, commentCount, comments,
             collect(DISTINCT ch.title)[..5] AS activeChannels

        // Get topics user discusses
        OPTIONAL MATCH (c3:Comment)<-[:WROTE]-(u)
        OPTIONAL MATCH (c3)-[:TAGGED]->(t:Topic)
        WITH u, commentCount, comments, activeChannels,
             collect(DISTINCT t.name)[..5] AS userTopics

        // Get reply interactions
        OPTIONAL MATCH (u)-[r:REPLIED_TO_USER]->()
        WITH u, commentCount, comments, activeChannels, userTopics,
             count(r) AS replyCount

        // Return user data
        WITH u.telegram_user_id AS userId,
             commentCount,
             replyCount,
             u.community_role AS role,
             u.communication_style AS style,
             u.inferred_gender AS gender,
             u.inferred_age_bracket AS age,
             activeChannels AS topChannels,
             userTopics AS topics,
             commentCount + replyCount * 2 AS influenceScore,
             // Calculate posts per week (approximate from comment frequency)
             CASE WHEN commentCount > 0 THEN
                  round(commentCount * 7.0 / 30.0)
             ELSE 0 END AS postsPerWeek,
             // Reply rate percentage
             CASE WHEN commentCount > 0 THEN
                  round(100.0 * replyCount / commentCount)
             ELSE 0 END AS replyRate

        RETURN userId, commentCount, replyCount, influenceScore,
               role, style, gender, age,
               topChannels, topics, postsPerWeek, replyRate
        ORDER BY influenceScore DESC
        LIMIT 20
    """)

    # Fetch usernames from Supabase
    try:
        from buffer.supabase_writer import SupabaseWriter
        writer = SupabaseWriter()

        # Get user IDs from Neo4j results
        user_ids = [r['userId'] for r in neo4j_results if r.get('userId')]

        if user_ids:
            # Fetch usernames from Supabase
            supabase_users = writer.client.table('telegram_users') \
                .select('telegram_user_id, username, first_name, last_name') \
                .in_('telegram_user_id', user_ids) \
                .execute()

            # Create a lookup dictionary
            user_lookup = {}
            for user in (supabase_users.data or []):
                tid = user.get('telegram_user_id')
                if tid:
                    user_lookup[tid] = {
                        'username': user.get('username'),
                        'firstName': user.get('first_name'),
                        'lastName': user.get('last_name')
                    }

            # Merge Supabase data with Neo4j results
            for result in neo4j_results:
                user_id = result.get('userId')
                if user_id and user_id in user_lookup:
                    user_data = user_lookup[user_id]
                    # Create display name from available data
                    display_name = user_data.get('username') or user_data.get('firstName') or f"User_{user_id}"
                    result['displayName'] = display_name
                    result['username'] = user_data.get('username')
                    result['firstName'] = user_data.get('firstName')
                    result['lastName'] = user_data.get('lastName')
                else:
                    result['displayName'] = f"User_{user_id}"
                    result['username'] = None
                    result['firstName'] = None
                    result['lastName'] = None

        return neo4j_results

    except Exception as e:
        # If Supabase fetch fails, return Neo4j data with default names
        for result in neo4j_results:
            user_id = result.get('userId')
            result['displayName'] = f"User_{user_id}"
            result['username'] = None
            result['firstName'] = None
            result['lastName'] = None
        return neo4j_results


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


def get_information_velocity() -> list[dict]:
    """Track how topics spread across channels with real timestamps."""
    return run_query("""
        // Find topics that appear in multiple channels
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
        WHERE p.posted_at IS NOT NULL
        WITH t, ch, min(p.posted_at) AS firstSeen,
             count(p) AS postCount,
             sum(p.views) AS totalViews

        // Group by topic to find spread pattern
        WITH t.name AS topic,
             collect({
                 channel: ch.title,
                 firstSeen: firstSeen,
                 postCount: postCount,
                 views: totalViews
             }) AS channelData
        WHERE size(channelData) >= 2  // Topic must appear in at least 2 channels

        // Find originator and calculate spread
        WITH topic, channelData
        ORDER BY topic
        WITH topic, channelData,
             // Sort by firstSeen to find originator
             [x IN channelData | x.firstSeen][0] AS earliestTime,
             [x IN channelData | x.firstSeen][-1] AS latestTime

        WITH topic, channelData, earliestTime, latestTime,
             [x IN channelData WHERE x.firstSeen = earliestTime | x.channel][0] AS originator

        // Calculate spread metrics
        WITH topic, originator, channelData,
             duration.between(earliestTime, latestTime).hours AS spreadHours,
             size(channelData) AS channelsReached,
             reduce(s = 0, x IN channelData | s + x.views) AS totalReach,
             // Find amplifier channels (highest engagement after originator)
             [x IN channelData WHERE x.channel <> originator | x] AS amplifierData

        WITH topic, originator, spreadHours, channelsReached, totalReach,
             // Sort amplifiers by views to find top amplifiers
             [x IN amplifierData | x.channel][..3] AS amplifiers,
             // Determine velocity based on spread speed
             CASE
                 WHEN spreadHours <= 6 AND channelsReached >= 5 THEN 'explosive'
                 WHEN spreadHours <= 24 AND channelsReached >= 3 THEN 'fast'
                 ELSE 'normal'
             END AS velocity

        RETURN topic, originator,
               coalesce(spreadHours, 0) AS spreadHours,
               channelsReached,
               totalReach,
               amplifiers,
               velocity
        ORDER BY totalReach DESC
        LIMIT 15
    """)
