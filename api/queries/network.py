"""
network.py — Tier 4: Channels, Voices & Activity

Provides: communityChannels, keyVoices, hourlyActivity, weeklyActivity,
          recommendations, viralTopics
"""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timezone
import json
from statistics import median
from api.dashboard_dates import DashboardDateContext
from api.db import run_query

LOOKBACK_DAYS = 30
ACTIVITY_LOOKBACK_DAYS = 7
MIN_POSTS_FOR_RANK = 5
MIN_TOTAL_VIEWS_FOR_RANK = 1000
SHRINKAGE_PRIOR_POSTS = 8
DOMINANT_CATEGORY_MIN_SHARE = 0.40
DOMINANT_CATEGORY_MIN_MENTIONS = 3
DOMINANT_CATEGORY_MIN_MARGIN = 0.10

_CATEGORY_TO_WIDGET_TYPE: dict[str, str] = {
    'Employment': 'Work',
    'Business & Enterprise': 'Business',
    'Financial System': 'Business',
    'Tech Economy': 'Business',
    'Housing & Infrastructure': 'Housing',
    'Family & Relationships': 'Family',
    'Education': 'Family',
    'Arts & Entertainment': 'Lifestyle',
    'Community Life': 'Lifestyle',
}

_WIDGET_TYPE_METADATA_KEYWORDS: dict[str, tuple[str, ...]] = {
    'Work': ('job', 'jobs', 'career', 'vacancy', 'hiring', 'work', 'работ', 'ваканс', 'карьер'),
    'Business': ('business', 'startup', 'founder', 'entrepreneur', 'tax', 'finance', 'бизнес', 'стартап', 'налог', 'финанс'),
    'Housing': ('rent', 'rental', 'housing', 'apartment', 'real estate', 'аренда', 'жиль', 'квартир', 'недвиж'),
    'Family': ('family', 'kids', 'children', 'parents', 'school', 'kindergarten', 'сем', 'дет', 'школ', 'родител'),
    'Legal': ('legal', 'visa', 'residency', 'residence', 'documents', 'passport', 'law', 'виза', 'документ', 'паспорт', 'легал'),
    'Lifestyle': ('food', 'restaurant', 'events', 'culture', 'lifestyle', 'travel', 'еда', 'ресторан', 'событ', 'досуг'),
}


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _reaction_total(payload: str | None) -> int:
    if not payload:
        return 0
    try:
        parsed = json.loads(payload)
    except Exception:
        return 0
    if not isinstance(parsed, dict):
        return 0
    total = 0
    for value in parsed.values():
        try:
            total += max(0, int(value))
        except Exception:
            continue
    return total


def _median(values: list[float]) -> float:
    return float(median(values)) if values else 0.0


def _map_topic_category_to_widget_type(category: str | None) -> str:
    name = str(category or '').strip()
    if not name:
        return 'General'
    return _CATEGORY_TO_WIDGET_TYPE.get(name, 'General')


def _normalize_source_key(username: str | None, name: str | None, channel_id: str | None) -> str:
    normalized_username = str(username or '').strip().lower().lstrip('@')
    if normalized_username:
        return f'u:{normalized_username}'

    normalized_name = str(name or '').strip().lower()
    if normalized_name:
        return f'n:{normalized_name}'

    return f'id:{str(channel_id or "").strip()}'


def _metadata_widget_type(name: str | None, description: str | None) -> str:
    title_text = str(name or '').strip().lower()
    description_text = str(description or '').strip().lower()
    if not title_text and not description_text:
        return 'General'

    scores: dict[str, int] = defaultdict(int)
    for widget_type, keywords in _WIDGET_TYPE_METADATA_KEYWORDS.items():
        for keyword in keywords:
            if keyword in title_text:
                scores[widget_type] += 3
            if keyword in description_text:
                scores[widget_type] += 1

    if not scores:
        return 'General'

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    best_type, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0
    if best_score < 3 or (best_score - second_score) < 2:
        return 'General'
    return best_type


def _dominant_widget_type(rows: list[dict]) -> tuple[str, str]:
    totals: dict[str, int] = defaultdict(int)
    raw_totals: dict[str, int] = defaultdict(int)
    total_mentions = 0

    for row in rows:
        mentions = int(row.get('mentions') or 0)
        if mentions <= 0:
            continue
        raw_category = str(row.get('category') or 'General')
        widget_type = _map_topic_category_to_widget_type(raw_category)
        totals[widget_type] += mentions
        raw_totals[raw_category] += mentions
        total_mentions += mentions

    if total_mentions <= 0:
        return 'General', 'General'

    ranked = sorted(totals.items(), key=lambda item: item[1], reverse=True)
    top_type, top_mentions = ranked[0]
    second_mentions = ranked[1][1] if len(ranked) > 1 else 0
    share = top_mentions / total_mentions
    second_share = second_mentions / total_mentions if total_mentions > 0 else 0.0

    if (
        top_type == 'General'
        or top_mentions < DOMINANT_CATEGORY_MIN_MENTIONS
        or share < DOMINANT_CATEGORY_MIN_SHARE
        or (share - second_share) < DOMINANT_CATEGORY_MIN_MARGIN
    ):
        return 'General', 'General'

    top_raw_category = max(
        ((category, mentions) for category, mentions in raw_totals.items() if _map_topic_category_to_widget_type(category) == top_type),
        key=lambda item: item[1],
        default=('General', 0),
    )[0]
    return top_type, top_raw_category


def get_community_channels(ctx: DashboardDateContext) -> list[dict]:
    """Active channels ranked by median recent post engagement with sample controls."""
    post_rows = run_query("""
        MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)
        WHERE p.posted_at >= datetime($start) AND p.posted_at < datetime($end)
          AND coalesce(ch.source_type, 'channel') = 'channel'
          AND coalesce(p.entry_kind, 'broadcast_post') = 'broadcast_post'
        RETURN ch.uuid AS channelId,
               ch.username AS username,
               ch.title AS name,
               ch.member_count AS members,
               ch.description AS description,
               p.uuid AS postId,
               toString(p.posted_at) AS postedAt,
               coalesce(p.views, 0) AS views,
               coalesce(p.forwards, 0) AS forwards,
               coalesce(p.comment_count, 0) AS comments,
               coalesce(p.reactions, '') AS reactions
    """, {"start": ctx.start_at.isoformat(), "end": ctx.end_at.isoformat()})

    category_rows = run_query("""
        MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)-[:TAGGED]->(t:Topic)
        WHERE p.posted_at >= datetime($start) AND p.posted_at < datetime($end)
          AND coalesce(ch.source_type, 'channel') = 'channel'
          AND coalesce(p.entry_kind, 'broadcast_post') = 'broadcast_post'
        OPTIONAL MATCH (t)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WITH ch, coalesce(cat.name, 'General') AS category, count(DISTINCT p) AS mentions
        RETURN ch.uuid AS channelId, category, mentions
    """, {"start": ctx.start_at.isoformat(), "end": ctx.end_at.isoformat()})

    comment_rows = run_query("""
        MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)<-[:REPLIES_TO]-(c:Comment)
        WHERE c.posted_at >= datetime($start) AND c.posted_at < datetime($end)
          AND coalesce(ch.source_type, 'channel') = 'channel'
          AND coalesce(p.entry_kind, 'broadcast_post') = 'broadcast_post'
        RETURN ch.uuid AS channelId, count(c) AS comments7d
    """, {"start": ctx.start_at.isoformat(), "end": ctx.end_at.isoformat()})

    activity_cutoff = ctx.start_at.timestamp()
    channels: dict[str, dict] = {}
    channel_to_source_key: dict[str, str] = {}
    all_post_rates: list[float] = []

    for row in post_rows:
        channel_id = str(row.get('channelId') or '').strip()
        if not channel_id:
            continue
        source_key = _normalize_source_key(row.get('username'), row.get('name'), channel_id)
        channel_to_source_key[channel_id] = source_key

        entry = channels.setdefault(source_key, {
            'username': row.get('username'),
            'name': row.get('name'),
            'members': row.get('members'),
            'description': row.get('description'),
            'lastPost': None,
            'postRates': [],
            'postCount': 0,
            'posts7d': 0,
            'totalViews30d': 0.0,
            'totalForwards30d': 0.0,
            'totalComments30d': 0.0,
            'totalReactions30d': 0.0,
        })

        views = max(0.0, float(row.get('views') or 0))
        forwards = max(0.0, float(row.get('forwards') or 0))
        comments = max(0.0, float(row.get('comments') or 0))
        reactions = float(_reaction_total(row.get('reactions')))
        posted_at = _parse_iso_datetime(row.get('postedAt'))

        entry['postCount'] += 1
        entry['totalViews30d'] += views
        entry['totalForwards30d'] += forwards
        entry['totalComments30d'] += comments
        entry['totalReactions30d'] += reactions

        if posted_at:
            if entry['lastPost'] is None or posted_at > entry['lastPost']:
                entry['lastPost'] = posted_at
                entry['username'] = row.get('username') or entry.get('username')
                entry['name'] = row.get('name') or entry.get('name')
                entry['members'] = row.get('members') or entry.get('members')
                entry['description'] = row.get('description') or entry.get('description')
            if posted_at.timestamp() >= activity_cutoff:
                entry['posts7d'] += 1
        elif not entry.get('name') and row.get('name'):
            entry['name'] = row.get('name')

        if views > 0:
            rate = min((reactions + forwards + comments) / views, 1.0)
            entry['postRates'].append(rate)
            all_post_rates.append(rate)

    global_median_rate = _median(all_post_rates)
    comments7d_by_channel: dict[str, int] = defaultdict(int)
    for row in comment_rows:
        channel_id = str(row.get('channelId') or '').strip()
        source_key = channel_to_source_key.get(channel_id)
        if not source_key:
            continue
        comments7d_by_channel[source_key] += int(row.get('comments7d') or 0)

    categories_by_channel: dict[str, list[dict]] = defaultdict(list)
    for row in category_rows:
        channel_id = str(row.get('channelId') or '').strip()
        if channel_id not in channel_to_source_key:
            continue
        categories_by_channel[channel_to_source_key[channel_id]].append({
            'category': row.get('category'),
            'mentions': int(row.get('mentions') or 0),
        })

    processed_results: list[dict] = []
    for source_key, channel in channels.items():
        post_count = int(channel['postCount'])
        total_views = float(channel['totalViews30d'])
        valid_rate_count = len(channel['postRates'])

        if post_count < MIN_POSTS_FOR_RANK or valid_rate_count < MIN_POSTS_FOR_RANK or total_views < MIN_TOTAL_VIEWS_FOR_RANK:
            continue

        channel_median = _median(channel['postRates'])
        shrunk_rate = (
            (channel_median * valid_rate_count) + (global_median_rate * SHRINKAGE_PRIOR_POSTS)
        ) / (valid_rate_count + SHRINKAGE_PRIOR_POSTS)

        widget_type, dominant_category = _dominant_widget_type(categories_by_channel.get(source_key, []))
        if widget_type == 'General':
            widget_type = _metadata_widget_type(channel.get('name'), channel.get('description'))
        comments7d = comments7d_by_channel.get(source_key, 0)
        recent_messages = int(channel['posts7d']) + comments7d

        avg_views = round(total_views / post_count) if post_count > 0 else 0
        avg_forwards = round(float(channel['totalForwards30d']) / post_count) if post_count > 0 else 0
        avg_comments = round(float(channel['totalComments30d']) / post_count) if post_count > 0 else 0
        last_post = channel['lastPost']

        processed_results.append({
            'username': channel.get('username'),
            'name': str(channel.get('name') or channel.get('username') or f'Channel_{source_key}'),
            'type': widget_type,
            'members': int(channel.get('members') or 0),
            'dailyMessages': round(recent_messages / float(max(1, ctx.days)), 1) if recent_messages > 0 else 0.0,
            'engagement': round(min(max(shrunk_rate * 100, 0.0), 100.0), 1),
            'topTopicEN': dominant_category,
            'topTopicRU': dominant_category,
            'description': channel.get('description'),
            'postCount': post_count,
            'avgViews': avg_views,
            'avgForwards': avg_forwards,
            'avgComments': avg_comments,
            'posts7d': int(channel['posts7d']),
            'posts30d': post_count,
            'comments7d': comments7d,
            'lastPost': last_post.isoformat() if isinstance(last_post, datetime) else None,
        })

    processed_results.sort(
        key=lambda row: (row.get('engagement', 0), row.get('postCount', 0), row.get('dailyMessages', 0)),
        reverse=True,
    )
    return processed_results[:50]


def get_key_voices(ctx: DashboardDateContext) -> list[dict]:
    """Most active recent commenters with real usernames, channels, and topics."""
    # First get data from Neo4j
    neo4j_results = run_query("""
        MATCH (u:User)-[:WROTE]->(c:Comment)
        WHERE c.posted_at >= datetime($start) AND c.posted_at < datetime($end)
        WITH u,
             count(DISTINCT c) AS commentCount,
             count(DISTINCT date(c.posted_at)) AS activeDays
        WHERE commentCount >= 3

        // Get channels where the user has been active in the same recent window
        OPTIONAL MATCH (u)-[:WROTE]->(c2:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
        WHERE c2.posted_at >= datetime($start) AND c2.posted_at < datetime($end)
        WITH u, commentCount, activeDays,
             collect(DISTINCT ch.title)[..5] AS activeChannels

        // Get topics from the user's recent comments
        OPTIONAL MATCH (u)-[:WROTE]->(c3:Comment)-[:TAGGED]->(t:Topic)
        WHERE c3.posted_at >= datetime($start) AND c3.posted_at < datetime($end)
        WITH u, commentCount, activeDays, activeChannels,
             collect(DISTINCT t.name)[..5] AS userTopics

        // Count active reply connections in the same window
        OPTIONAL MATCH (u)-[r:REPLIED_TO_USER]->()
        WHERE coalesce(r.last_seen, r.first_seen) >= datetime($start)
          AND coalesce(r.last_seen, r.first_seen) < datetime($end)
        WITH u, commentCount, activeDays, activeChannels, userTopics,
             count(DISTINCT r) AS replyCount

        // Return recent activity profile
        WITH u.telegram_user_id AS userId,
             commentCount,
             replyCount,
             activeDays,
             u.community_role AS role,
             u.communication_style AS style,
             u.inferred_gender AS gender,
             u.inferred_age_bracket AS age,
             activeChannels AS topChannels,
             userTopics AS topics,
             (commentCount + replyCount * 2 + activeDays) AS activityScore,
             CASE WHEN commentCount > 0 THEN
                  round(commentCount * 7.0 / 30.0)
             ELSE 0 END AS postsPerWeek,
             CASE WHEN commentCount > 0 THEN
                  round(100.0 * replyCount / commentCount)
             ELSE 0 END AS replyRate

        RETURN userId, commentCount, replyCount, activeDays,
               activityScore AS influenceScore,
               role, style, gender, age,
               topChannels, topics, postsPerWeek, replyRate
        ORDER BY activityScore DESC, commentCount DESC, activeDays DESC
        LIMIT 20
    """, {"start": ctx.start_at.isoformat(), "end": ctx.end_at.isoformat()})

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


def get_information_velocity(ctx: DashboardDateContext) -> list[dict]:
    """Track how topics spread across channels with real timestamps."""
    return run_query("""
        // Find topics that appear in multiple channels
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
        WHERE p.posted_at IS NOT NULL
          AND p.posted_at >= datetime($start)
          AND p.posted_at < datetime($end)
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
    """, {"start": ctx.start_at.isoformat(), "end": ctx.end_at.isoformat()})
