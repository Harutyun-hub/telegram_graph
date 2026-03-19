"""
comparative.py — Tier 8: Comparative Analytics & Deep Dive

Provides: weeklyShifts, sentimentByTopic, topPosts, contentTypePerformance,
          vitalityIndicators, allTopics, allChannels, allAudience
"""
from __future__ import annotations
from api.db import run_query, run_single


def get_weekly_shifts() -> list[dict]:
    """Week-over-week changes in key metrics."""
    return run_query("""
        // This week's activity
        MATCH (p:Post) WHERE p.posted_at > datetime() - duration('P7D')
        WITH count(p) AS thisWeekPosts
        OPTIONAL MATCH (c:Comment) WHERE c.posted_at > datetime() - duration('P7D')
        WITH thisWeekPosts, count(c) AS thisWeekComments
        OPTIONAL MATCH (u:User) WHERE u.last_seen > datetime() - duration('P7D')
        WITH thisWeekPosts, thisWeekComments, count(u) AS thisWeekUsers

        // Last week's activity
        OPTIONAL MATCH (p2:Post) WHERE p2.posted_at > datetime() - duration('P14D')
                                  AND p2.posted_at <= datetime() - duration('P7D')
        WITH thisWeekPosts, thisWeekComments, thisWeekUsers,
             count(p2) AS lastWeekPosts
        OPTIONAL MATCH (c2:Comment) WHERE c2.posted_at > datetime() - duration('P14D')
                                     AND c2.posted_at <= datetime() - duration('P7D')
        WITH thisWeekPosts, thisWeekComments, thisWeekUsers, lastWeekPosts,
             count(c2) AS lastWeekComments

        RETURN thisWeekPosts, lastWeekPosts,
               thisWeekComments, lastWeekComments,
               thisWeekUsers,
               CASE WHEN lastWeekPosts > 0
                    THEN round(100.0 * (thisWeekPosts - lastWeekPosts) / lastWeekPosts, 1)
                    ELSE 0 END AS postChange,
               CASE WHEN lastWeekComments > 0
                    THEN round(100.0 * (thisWeekComments - lastWeekComments) / lastWeekComments, 1)
                    ELSE 0 END AS commentChange
    """)


def get_sentiment_by_topic() -> list[dict]:
    """Sentiment breakdown per topic."""
    return run_query("""
        CALL () {
            MATCH (p:Post)-[:TAGGED]->(t:Topic)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']
            RETURN t.name AS topic, s.label AS sentiment, count(*) AS count
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t:Topic)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']
            RETURN t.name AS topic, s.label AS sentiment, count(*) AS count
        }
        RETURN topic, sentiment, sum(count) AS count
        ORDER BY topic, count DESC
    """)


def get_top_posts() -> list[dict]:
    """Highest-engagement posts."""
    return run_query("""
        MATCH (p:Post)-[:IN_CHANNEL]->(ch:Channel)
        OPTIONAL MATCH (p)-[:TAGGED]->(t:Topic)
        WITH p, ch, collect(t.name)[..3] AS topics
        RETURN p.uuid AS uuid,
               left(p.text, 200) AS text,
               p.views AS views, p.forwards AS forwards,
               p.comment_count AS comments,
               ch.username AS channel,
               toString(p.posted_at) AS postedAt,
               topics
        ORDER BY p.views DESC
        LIMIT 20
    """)


def get_content_type_performance() -> list[dict]:
    """Average engagement by content/media type."""
    return run_query("""
        MATCH (p:Post)
        WITH coalesce(p.media_type, 'text') AS mediaType,
             count(p) AS count,
             avg(p.views) AS avgViews,
             avg(p.forwards) AS avgForwards
        RETURN mediaType, count,
               round(avgViews) AS avgViews,
               round(avgForwards) AS avgForwards
        ORDER BY avgViews DESC
    """)


def get_vitality_indicators() -> dict:
    """Composite community health indicators."""
    total_users = (run_single("MATCH (u:User) RETURN count(u) AS n") or {}).get("n", 0)
    active_users = (run_single("""
        MATCH (u:User) WHERE u.last_seen > datetime() - duration('P7D')
        RETURN count(u) AS n
    """) or {}).get("n", 0)
    total_topics = (run_single("MATCH (t:Topic) RETURN count(t) AS n") or {}).get("n", 0)
    total_posts = (run_single("MATCH (p:Post) RETURN count(p) AS n") or {}).get("n", 0)
    total_comments = (run_single("MATCH (c:Comment) RETURN count(c) AS n") or {}).get("n", 0)
    avg_comments = total_comments / max(total_posts, 1)

    return {
        "totalUsers": total_users,
        "activeUsers7d": active_users,
        "activityRate": round(100 * active_users / max(total_users, 1), 1),
        "totalTopics": total_topics,
        "totalPosts": total_posts,
        "totalComments": total_comments,
        "avgCommentsPerPost": round(avg_comments, 1),
    }


# ── Detail Pages ─────────────────────────────────────────────────────────────

def get_all_topics(page: int = 0, size: int = 50) -> list[dict]:
    """All topics with stats for the Topics detail page."""
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        CALL (t) {
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            RETURN count(p) AS postCount
        }
        CALL (t) {
            OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
            RETURN count(c) AS commentCount
        }
        CALL (t) {
            OPTIONAL MATCH (u:User)-[i:INTERESTED_IN]->(t)
            RETURN count(DISTINCT u) AS userCount,
                   coalesce(sum(i.count), 0) AS totalInteractions
        }
        CALL (t) {
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration('P7D')
            RETURN count(p) AS posts7d
        }
        CALL (t) {
            OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at > datetime() - duration('P7D')
            RETURN count(c) AS comments7d
        }
        CALL (t) {
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration('P14D')
              AND p.posted_at <= datetime() - duration('P7D')
            RETURN count(p) AS postsPrev7d
        }
        CALL (t) {
            OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at > datetime() - duration('P14D')
              AND c.posted_at <= datetime() - duration('P7D')
            RETURN count(c) AS commentsPrev7d
        }
        CALL (t) {
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration('P56D')
            WITH date(p.posted_at).year AS year, date(p.posted_at).week AS week, count(p) AS count
            WHERE week IS NOT NULL
            ORDER BY year, week
            RETURN collect({year: year, week: week, count: count}) AS weeklyRows
        }
        CALL (t) {
            CALL (t) {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
                RETURN toLower(coalesce(s.label, '')) AS label, count(*) AS score
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
                RETURN toLower(coalesce(s.label, '')) AS label, count(*) AS score
            }
            WITH label, sum(score) AS score
            WHERE label <> ''
            RETURN
                sum(CASE WHEN label = 'positive' THEN score ELSE 0 END) AS positiveScore,
                sum(CASE WHEN label = 'neutral' THEN score ELSE 0 END) AS neutralScore,
                sum(CASE WHEN label IN ['negative', 'urgent', 'sarcastic'] THEN score ELSE 0 END) AS negativeScore
        }
        WITH t, cat, postCount, commentCount, userCount, totalInteractions,
             posts7d, comments7d, postsPrev7d, commentsPrev7d,
             weeklyRows,
             coalesce(positiveScore, 0) AS positiveScore,
             coalesce(neutralScore, 0) AS neutralScore,
             coalesce(negativeScore, 0) AS negativeScore,
             coalesce(positiveScore, 0) + coalesce(neutralScore, 0) + coalesce(negativeScore, 0) AS sentimentTotal,
             (postCount + commentCount) AS mentionCount,
             (posts7d + comments7d) AS last7Mentions,
             (postsPrev7d + commentsPrev7d) AS prev7Mentions
        ORDER BY postCount DESC
        SKIP $skip LIMIT $size

        // Original posts tagged with this topic
        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            WITH p, ch
            ORDER BY p.posted_at DESC
            RETURN collect({
                id: coalesce(p.uuid, 'post:' + elementId(p)),
                type: 'message',
                author: coalesce(ch.username, ch.title, 'unknown'),
                channel: coalesce(ch.title, ch.username, 'unknown'),
                text: left(coalesce(p.text, ''), 1200),
                timestamp: toString(p.posted_at),
                reactions: coalesce(p.views, 0),
                replies: coalesce(p.comment_count, 0)
            })[..3] AS postEvidence
        }

        // Replies/comments tagged with this topic
        CALL (t) {
            MATCH (c:Comment)-[:TAGGED]->(t)
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            WITH c, u, ch
            ORDER BY c.posted_at DESC
            RETURN collect({
                id: coalesce(c.uuid, 'comment:' + elementId(c)),
                type: 'reply',
                author: coalesce(toString(u.telegram_user_id), 'anonymous'),
                channel: coalesce(ch.title, ch.username, 'unknown'),
                text: left(coalesce(c.text, ''), 1200),
                timestamp: toString(c.posted_at),
                reactions: 0,
                replies: 0
            })[..3] AS commentEvidence
        }

        // Question-like posts for proof mode
        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration('P90D')
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
              AND p.text CONTAINS '?'
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            WITH p, ch
            ORDER BY p.posted_at DESC
            RETURN collect({
                id: coalesce(p.uuid, 'post:' + elementId(p)),
                type: 'message',
                author: coalesce(ch.username, ch.title, 'unknown'),
                channel: coalesce(ch.title, ch.username, 'unknown'),
                text: left(coalesce(p.text, ''), 1200),
                timestamp: toString(p.posted_at),
                reactions: coalesce(p.views, 0),
                replies: coalesce(p.comment_count, 0)
            })[..6] AS questionPostEvidence
        }

        // Question-like comments for proof mode
        CALL (t) {
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at > datetime() - duration('P90D')
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
              AND c.text CONTAINS '?'
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            WITH c, u, ch
            ORDER BY c.posted_at DESC
            RETURN collect({
                id: coalesce(c.uuid, 'comment:' + elementId(c)),
                type: 'reply',
                author: coalesce(toString(u.telegram_user_id), 'anonymous'),
                channel: coalesce(ch.title, ch.username, 'unknown'),
                text: left(coalesce(c.text, ''), 1200),
                timestamp: toString(c.posted_at),
                reactions: 0,
                replies: 0
            })[..6] AS questionCommentEvidence
        }

        RETURN t.name AS name,
               cat.name AS category,
               postCount,
               commentCount,
               mentionCount,
               userCount,
               totalInteractions,
               last7Mentions,
               prev7Mentions,
               weeklyRows,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * positiveScore / sentimentTotal)) ELSE 0 END AS sentimentPositive,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * neutralScore / sentimentTotal)) ELSE 0 END AS sentimentNeutral,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * negativeScore / sentimentTotal)) ELSE 0 END AS sentimentNegative,
                CASE
                    WHEN prev7Mentions > 0
                    THEN round(100.0 * (last7Mentions - prev7Mentions) / prev7Mentions, 1)
                    WHEN last7Mentions > 0 THEN 100.0
                    ELSE 0.0
                END AS growth7dPct,
                postEvidence + commentEvidence AS evidence,
                questionPostEvidence + questionCommentEvidence AS questionEvidence
    """, {"skip": page * size, "size": size})


def get_all_channels() -> list[dict]:
    """All channels with full stats for the Channels detail page."""
    return run_query("""
        MATCH (ch:Channel)
        OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(pAll:Post)
        WITH ch,
             count(pAll) AS postCount,
             avg(coalesce(pAll.views, 0)) AS avgViews,
             max(pAll.posted_at) AS lastPost

        CALL (ch) {
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at > datetime() - duration('P7D')
            RETURN count(p) AS posts7d
        }
        CALL (ch) {
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at > datetime() - duration('P14D')
              AND p.posted_at <= datetime() - duration('P7D')
            RETURN count(p) AS postsPrev7d
        }
        CALL (ch) {
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at > datetime() - duration('P7D')
            WITH p.posted_at.dayOfWeek AS dow, count(p) AS c
            WHERE dow IS NOT NULL
            RETURN collect({dow: dow, count: c}) AS weeklyRows
        }
        CALL (ch) {
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at > datetime() - duration('P7D')
            WITH p.posted_at.hour AS hour, count(p) AS c
            WHERE hour IS NOT NULL
            RETURN collect({hour: hour, count: c}) AS hourlyRows
        }
        CALL (ch) {
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)-[:TAGGED]->(t:Topic)
            WITH t.name AS topic, count(p) AS mentions
            WHERE topic IS NOT NULL
            ORDER BY mentions DESC
            WITH collect({name: topic, mentions: mentions})[..6] AS topTopics, sum(mentions) AS totalMentions
            RETURN [tt IN topTopics | {
                name: tt.name,
                mentions: tt.mentions,
                pct: CASE WHEN totalMentions > 0 THEN toInteger(round(100.0 * tt.mentions / totalMentions)) ELSE 0 END
            }] AS topTopics
        }
        CALL (ch) {
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WITH coalesce(p.media_type, 'text') AS mediaType, count(p) AS count
            WHERE mediaType IS NOT NULL
            ORDER BY count DESC
            RETURN collect({type: mediaType, count: count})[..6] AS messageTypes
        }
        CALL (ch) {
            OPTIONAL MATCH (u:User)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch)
            WITH u, count(c) AS posts
            WHERE u IS NOT NULL
            ORDER BY posts DESC
            RETURN collect({
                name: coalesce(toString(u.telegram_user_id), 'anonymous'),
                posts: posts,
                helpScore: toInteger(CASE WHEN posts * 5 > 100 THEN 100 ELSE posts * 5 END)
            })[..4] AS topVoices
        }
        CALL (ch) {
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WITH ch, p
            ORDER BY p.posted_at DESC
            RETURN collect({
                id: coalesce(p.uuid, ''),
                author: coalesce(ch.username, ch.title, 'unknown'),
                text: left(coalesce(p.text, ''), 220),
                timestamp: toString(p.posted_at),
                reactions: coalesce(p.views, 0),
                replies: coalesce(p.comment_count, 0)
            })[..6] AS recentPosts
        }
        CALL (ch) {
            CALL {
                WITH ch
                OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)-[:HAS_SENTIMENT]->(s:Sentiment)
                RETURN toLower(coalesce(s.label, '')) AS label, count(s) AS score
                UNION ALL
                WITH ch
                OPTIONAL MATCH (u:User)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch)
                OPTIONAL MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
                RETURN toLower(coalesce(s.label, '')) AS label, count(s) AS score
            }
            WITH label, sum(score) AS score
            WHERE label <> ''
            RETURN
                sum(CASE WHEN label = 'positive' THEN score ELSE 0 END) AS positiveScore,
                sum(CASE WHEN label = 'neutral' THEN score ELSE 0 END) AS neutralScore,
                sum(CASE WHEN label IN ['negative', 'urgent', 'sarcastic'] THEN score ELSE 0 END) AS negativeScore
        }

        WITH ch, postCount, avgViews, lastPost,
             posts7d, postsPrev7d,
             weeklyRows, hourlyRows,
             topTopics, messageTypes, topVoices, recentPosts,
             coalesce(positiveScore, 0) AS positiveScore,
             coalesce(neutralScore, 0) AS neutralScore,
             coalesce(negativeScore, 0) AS negativeScore,
             coalesce(positiveScore, 0) + coalesce(neutralScore, 0) + coalesce(negativeScore, 0) AS sentimentTotal

        RETURN ch.username AS username,
               ch.title AS title,
               ch.member_count AS memberCount,
               ch.description AS description,
               postCount,
               round(avgViews) AS avgViews,
               toString(lastPost) AS lastPost,
               toInteger(round(posts7d / 7.0)) AS dailyMessages,
               CASE
                   WHEN postsPrev7d > 0 THEN round(100.0 * (posts7d - postsPrev7d) / postsPrev7d, 1)
                   WHEN posts7d > 0 THEN 100.0
                   ELSE 0.0
               END AS growth7dPct,
               weeklyRows,
               hourlyRows,
               topTopics,
               messageTypes,
               topVoices,
               recentPosts,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * positiveScore / sentimentTotal)) ELSE 0 END AS sentimentPositive,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * neutralScore / sentimentTotal)) ELSE 0 END AS sentimentNeutral,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * negativeScore / sentimentTotal)) ELSE 0 END AS sentimentNegative
        ORDER BY postCount DESC
    """)


def get_all_audience(page: int = 0, size: int = 50) -> list[dict]:
    """All users with profiles for the Audience detail page."""
    return run_query("""
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)
        WITH u, count(c) AS commentCount
        OPTIONAL MATCH (u)-[:INTERESTED_IN]->(t:Topic)
        WITH u, commentCount,
             collect(t.name)[..5] AS topics,
             collect({name: t.name, count: 1})[..5] AS topTopics

        CALL (u) {
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WITH ch, count(c) AS messageCount
            WHERE ch IS NOT NULL
            ORDER BY messageCount DESC
            RETURN collect({
                name: coalesce(ch.title, ch.username, 'unknown'),
                type: 'General',
                role: 'Member',
                messageCount: messageCount
            })[..3] AS channels
        }
        CALL (u) {
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WITH c, ch
            ORDER BY c.posted_at DESC
            RETURN collect({
                text: left(coalesce(c.text, ''), 220),
                channel: coalesce(ch.title, ch.username, 'unknown'),
                timestamp: toString(c.posted_at),
                reactions: 0,
                replies: 0
            })[..4] AS recentMessages
        }
        CALL (u) {
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)
            WHERE c.posted_at > datetime() - duration('P42D')
            WITH date(c.posted_at) AS day, count(c) AS msgs
            WHERE day IS NOT NULL
            ORDER BY day ASC
            RETURN collect({week: toString(day), msgs: msgs})[..6] AS activityData
        }
        CALL (u) {
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:HAS_SENTIMENT]->(s:Sentiment)
            WITH toLower(coalesce(s.label, '')) AS label, count(s) AS score
            WHERE label <> ''
            RETURN
                sum(CASE WHEN label = 'positive' THEN score ELSE 0 END) AS positiveScore,
                sum(CASE WHEN label = 'neutral' THEN score ELSE 0 END) AS neutralScore,
                sum(CASE WHEN label IN ['negative', 'urgent', 'sarcastic'] THEN score ELSE 0 END) AS negativeScore
        }
        WITH u, commentCount, topics, topTopics, channels, recentMessages, activityData,
             coalesce(positiveScore, 0) AS positiveScore,
             coalesce(neutralScore, 0) AS neutralScore,
             coalesce(negativeScore, 0) AS negativeScore,
             coalesce(positiveScore, 0) + coalesce(neutralScore, 0) + coalesce(negativeScore, 0) AS sentimentTotal
        RETURN u.telegram_user_id AS userId,
               u.inferred_gender AS gender,
               u.inferred_age_bracket AS age,
               u.language AS language,
               u.community_role AS role,
               u.communication_style AS style,
               u.migration_intent AS migrationIntent,
               u.financial_distress_level AS financialDistress,
               u.price_sensitivity AS priceSensitivity,
               toString(u.last_seen) AS lastSeen,
               commentCount,
               topics,
               topTopics,
               channels,
               recentMessages,
               activityData,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * positiveScore / sentimentTotal)) ELSE 0 END AS sentimentPositive,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * neutralScore / sentimentTotal)) ELSE 0 END AS sentimentNeutral,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * negativeScore / sentimentTotal)) ELSE 0 END AS sentimentNegative
        ORDER BY commentCount DESC
        SKIP $skip LIMIT $size
    """, {"skip": page * size, "size": size})
