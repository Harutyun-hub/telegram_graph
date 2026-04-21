"""
predictive.py — Tier 6: Predictive Intelligence

Provides: emergingInterests, retentionFactors, churnSignals, growthFunnel,
          decisionStages, newVsReturningVoiceWidget
"""
from __future__ import annotations
import math
from datetime import datetime, timedelta, timezone
import threading
import time
from api.dashboard_dates import DashboardDateContext
from api.db import run_query

NOISY_TOPIC_VALUES = ["", "null", "unknown", "none", "n/a", "na"]
MIN_ACTIVE_COMMENTS = 2
MIN_TOPIC_SUPPORT = 4
SMOOTHING_PRIOR = 5
EMERGING_LOOKBACK_DAYS = 14
EMERGING_COMPARE_DAYS = 7
FUNNEL_DEDUP_FLOOR = 0.45
FUNNEL_READER_VIEW_PERCENTILE = 0.75
FUNNEL_LEADER_SCORE_PERCENTILE = 0.90
_QUERY_CACHE_TTL_SECONDS = 30.0
_QUERY_CACHE: dict[tuple[str, str], tuple[float, list[dict]]] = {}
_QUERY_CACHE_LOCK = threading.Lock()
FUNNEL_ASK_HINTS = [
    "need help",
    "looking for",
    "where can i",
    "how to get",
    "can anyone recommend",
    "please help",
    "need a",
    "need an",
    "recommend a",
    "recommend an",
    "where do i",
    "how do i",
    "нужна помощь",
    "нужен совет",
    "подскажите",
    "где найти",
    "как получить",
    "кто может помочь",
    "помогите",
    "ищу",
    "нужен",
    "нужна",
    "нужно",
]


def _cached_rows(kind: str, ctx: DashboardDateContext, builder) -> list[dict]:
    key = (kind, ctx.cache_key)
    now = time.monotonic()
    with _QUERY_CACHE_LOCK:
        stale_keys = [cache_key for cache_key, (ts, _rows) in _QUERY_CACHE.items() if (now - ts) >= _QUERY_CACHE_TTL_SECONDS]
        for stale_key in stale_keys:
            _QUERY_CACHE.pop(stale_key, None)
        cached = _QUERY_CACHE.get(key)
        if cached is not None:
            return cached[1]
    rows = builder()
    with _QUERY_CACHE_LOCK:
        _QUERY_CACHE[key] = (now, rows)
    return rows


def _predictive_window_params(ctx: DashboardDateContext) -> dict[str, object]:
    current_start = max(ctx.start_at, ctx.end_at - timedelta(days=EMERGING_COMPARE_DAYS))
    previous_end = current_start
    previous_start = max(ctx.start_at, previous_end - timedelta(days=EMERGING_COMPARE_DAYS))
    lookback_start = max(ctx.start_at, ctx.end_at - timedelta(days=EMERGING_LOOKBACK_DAYS))
    return {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "lookback_start": lookback_start.isoformat(),
        "current_start": current_start.isoformat(),
        "previous_start": previous_start.isoformat(),
        "previous_end": previous_end.isoformat(),
    }


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _percentile(values: list[float], q: float) -> float:
    cleaned = sorted(float(v) for v in values if v is not None)
    if not cleaned:
        return 0.0
    if len(cleaned) == 1:
        return cleaned[0]
    pos = (len(cleaned) - 1) * _clamp(q, 0.0, 1.0)
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return cleaned[lower]
    frac = pos - lower
    return cleaned[lower] + (cleaned[upper] - cleaned[lower]) * frac


def get_emerging_interests(ctx: DashboardDateContext) -> list[dict]:
    """Brand-new topics gathering real discussion in the latest 14-day horizon."""
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
        CALL {
            WITH t
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($lookback_start)
                  AND p.posted_at < datetime($end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN p.posted_at AS ts,
                       coalesce(ch.title, ch.username, '') AS channel,
                       'post' AS kind
                UNION ALL
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($lookback_start)
                  AND c.posted_at < datetime($end)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN c.posted_at AS ts,
                       coalesce(ch.title, ch.username, '') AS channel,
                       'comment' AS kind
            }
            RETURN count(*) AS mentions14d,
                   min(ts) AS firstSeenWindow,
                   count(CASE
                       WHEN ts >= datetime($current_start)
                        AND ts < datetime($end)
                       THEN 1
                   END) AS currentMentions,
                   count(CASE
                       WHEN ts >= datetime($current_start)
                        AND ts < datetime($end)
                        AND kind = 'post'
                       THEN 1
                   END) AS currentPosts,
                   count(CASE
                       WHEN ts >= datetime($current_start)
                        AND ts < datetime($end)
                        AND kind = 'comment'
                       THEN 1
                   END) AS currentComments,
                   count(CASE
                       WHEN ts >= datetime($previous_start)
                        AND ts < datetime($previous_end)
                       THEN 1
                   END) AS previousMentions,
                   count(DISTINCT CASE
                       WHEN ts >= datetime($current_start)
                        AND ts < datetime($end)
                       THEN date(ts)
                   END) AS currentActiveDays,
                   count(DISTINCT CASE
                       WHEN ts >= datetime($current_start)
                        AND ts < datetime($end)
                        AND channel <> ''
                       THEN channel
                   END) AS currentChannels
        }
        WITH t, mentions14d, firstSeenWindow, currentMentions, currentPosts, currentComments,
             previousMentions, currentActiveDays, currentChannels
        WHERE mentions14d > 0
          AND currentMentions >= 3
          AND currentMentions > previousMentions
          AND (currentActiveDays >= 2 OR currentComments >= 2 OR currentChannels >= 2)
        CALL {
            WITH t
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at IS NOT NULL
                RETURN p.posted_at AS ts
                UNION ALL
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at IS NOT NULL
                RETURN c.posted_at AS ts
            }
            RETURN min(ts) AS firstSeenEver
        }
        WITH t,
             coalesce(firstSeenEver, t.created_at, firstSeenWindow) AS firstSeen,
             currentMentions,
             currentPosts,
             currentComments,
             previousMentions,
             currentActiveDays,
             currentChannels
        WHERE firstSeen IS NOT NULL
          AND firstSeen >= datetime($lookback_start)
        CALL {
            WITH t
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($lookback_start)
                  AND p.posted_at < datetime($end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN p.posted_at AS ts,
                       coalesce(ch.title, ch.username, '') AS channel
                UNION ALL
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($lookback_start)
                  AND c.posted_at < datetime($end)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN c.posted_at AS ts,
                       coalesce(ch.title, ch.username, '') AS channel
            }
            WITH ts, channel
            WHERE channel <> ''
            ORDER BY ts ASC, channel ASC
            RETURN head(collect(channel)) AS originChannel
        }
        CALL {
            WITH t
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE p.posted_at >= datetime($current_start)
                  AND p.posted_at < datetime($end)
                RETURN toLower(coalesce(s.label, '')) AS label, count(*) AS n
                UNION ALL
                MATCH (c:Comment)-[:TAGGED]->(t)
                MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE c.posted_at >= datetime($current_start)
                  AND c.posted_at < datetime($end)
                RETURN toLower(coalesce(s.label, '')) AS label, count(*) AS n
            }
            WITH label, sum(n) AS n
            WHERE label <> ''
            ORDER BY n DESC, label ASC
            RETURN head(collect(label)) AS dominantSentiment
        }
        WITH t.name AS topic,
             firstSeen,
             currentMentions,
             currentPosts,
             currentComments,
             previousMentions,
             currentActiveDays,
             currentChannels,
             coalesce(originChannel, 'Community channel') AS originChannel,
             coalesce(dominantSentiment, 'neutral') AS dominantSentiment,
             duration.between(firstSeen, datetime($end)).days AS ageDays,
             (currentMentions + previousMentions) AS growthSupport
        WITH topic,
             firstSeen,
             currentMentions,
             currentPosts,
             currentComments,
             previousMentions,
             currentActiveDays,
             currentChannels,
             originChannel,
             dominantSentiment,
             ageDays,
             growthSupport,
             CASE
                 WHEN growthSupport < 4 THEN null
                 ELSE round(100.0 * (currentMentions - previousMentions) / (previousMentions + 2), 1)
             END AS momentum,
             CASE
                 WHEN 100.0 * toFloat(currentMentions) / 15.0 > 100.0 THEN 100.0
                 ELSE 100.0 * toFloat(currentMentions) / 15.0
             END AS volumeScore,
             CASE
                 WHEN 100.0 * toFloat(currentChannels) / 4.0 > 100.0 THEN 100.0
                 ELSE 100.0 * toFloat(currentChannels) / 4.0
             END AS breadthScore,
             CASE
                 WHEN currentMentions <= 0 THEN 0.0
                 WHEN 100.0 * (
                     (toFloat(currentComments) / toFloat(currentMentions)) * 0.6 +
                     (toFloat(currentActiveDays) / 7.0) * 0.4
                 ) > 100.0 THEN 100.0
                 ELSE 100.0 * (
                     (toFloat(currentComments) / toFloat(currentMentions)) * 0.6 +
                     (toFloat(currentActiveDays) / 7.0) * 0.4
                 )
             END AS conversationScore,
             CASE
                 WHEN ageDays <= 0 THEN 100.0
                 WHEN ageDays >= 14 THEN 0.0
                 ELSE 100.0 * toFloat(14 - ageDays) / 14.0
             END AS freshnessScore
        WITH topic,
             firstSeen,
             currentMentions,
             currentPosts,
             currentComments,
             previousMentions,
             currentActiveDays,
             currentChannels,
             originChannel,
             dominantSentiment,
             ageDays,
             growthSupport,
             momentum,
             round(
                 0.35 * CASE WHEN coalesce(momentum, 0.0) < 0.0 THEN 0.0 ELSE coalesce(momentum, 0.0) END +
                 0.25 * volumeScore +
                 0.15 * breadthScore +
                 0.15 * conversationScore +
                 0.10 * freshnessScore,
                 1
             ) AS emergenceScore
        RETURN topic,
               toString(firstSeen) AS firstSeen,
               currentMentions AS recentPosts,
               currentMentions AS totalPosts,
               currentMentions AS currentPosts,
               previousMentions AS previousPosts,
               growthSupport,
               currentComments,
               currentActiveDays,
               currentChannels,
               ageDays,
               originChannel,
               dominantSentiment AS mood,
               momentum,
               emergenceScore,
               CASE
                   WHEN emergenceScore >= 75 THEN 'high'
                   WHEN emergenceScore >= 55 THEN 'medium'
                   ELSE 'low'
               END AS opportunity
        ORDER BY emergenceScore DESC, coalesce(momentum, 0) DESC, currentMentions DESC, topic ASC
        LIMIT 15
    """, {
        **_predictive_window_params(ctx),
        "noise": NOISY_TOPIC_VALUES,
    })


def get_retention_factors(ctx: DashboardDateContext) -> list[dict]:
    """Topic-level continuity factors among previously active members."""
    return _cached_rows("retention_factors", ctx, lambda: run_query("""
        CALL {
            MATCH (u:User)-[:WROTE]->(c:Comment)
            WHERE c.posted_at >= datetime($previous_start)
              AND c.posted_at < datetime($previous_end)
            WITH u, count(DISTINCT c) AS previousComments
            WHERE previousComments >= $min_comments
            WITH u,
                 EXISTS {
                     MATCH (u)-[:WROTE]->(c2:Comment)
                     WHERE c2.posted_at >= datetime($start)
                       AND c2.posted_at < datetime($end)
                     WITH count(DISTINCT c2) AS currentComments
                     WHERE currentComments >= $min_comments
                     RETURN 1
                 } AS retained
            RETURN count(DISTINCT u) AS previousActiveUsers,
                   count(DISTINCT CASE WHEN retained THEN u END) AS retainedActiveUsers
        }
        MATCH (u:User)-[:WROTE]->(c:Comment)
        WHERE c.posted_at >= datetime($previous_start)
          AND c.posted_at < datetime($previous_end)
        WITH u, previousActiveUsers, retainedActiveUsers, count(DISTINCT c) AS previousComments
        WHERE previousComments >= $min_comments
        WITH u,
             previousActiveUsers,
             retainedActiveUsers,
             EXISTS {
                 MATCH (u)-[:WROTE]->(c2:Comment)
                 WHERE c2.posted_at >= datetime($start)
                   AND c2.posted_at < datetime($end)
                 WITH count(DISTINCT c2) AS currentComments
                 WHERE currentComments >= $min_comments
                 RETURN 1
             } AS retained
        MATCH (u)-[:WROTE]->(topicComment:Comment)-[:TAGGED]->(t:Topic)
        WHERE topicComment.posted_at >= datetime($previous_start)
          AND topicComment.posted_at < datetime($previous_end)
          AND coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
        WITH t.name AS topic,
             count(DISTINCT u) AS previousUsers,
             count(DISTINCT CASE WHEN retained THEN u END) AS retainedUsers,
             previousActiveUsers,
             retainedActiveUsers,
             CASE
                 WHEN previousActiveUsers = 0 THEN 0.0
                 ELSE 100.0 * retainedActiveUsers / previousActiveUsers
             END AS baselineContinuityPct
        WHERE previousUsers >= $min_topic_support
        WITH topic,
             previousUsers,
             retainedUsers,
             previousActiveUsers,
             retainedActiveUsers,
             baselineContinuityPct,
             100.0 * (retainedUsers + ($prior_strength * baselineContinuityPct / 100.0)) / (previousUsers + $prior_strength) AS smoothedContinuityPct,
             100.0 * previousUsers / previousActiveUsers AS topicSharePct
        WITH topic,
             previousUsers,
             retainedUsers,
             previousActiveUsers,
             retainedActiveUsers,
             round(baselineContinuityPct, 1) AS baselineContinuityPctRounded,
             round(smoothedContinuityPct, 1) AS continuityPctRounded,
             round(topicSharePct, 1) AS topicSharePctRounded
        WITH topic,
             previousUsers,
             retainedUsers,
             previousActiveUsers,
             retainedActiveUsers,
             baselineContinuityPctRounded AS baselineContinuityPct,
             continuityPctRounded AS continuityPct,
             round(continuityPctRounded - baselineContinuityPctRounded, 1) AS liftPct,
             topicSharePctRounded AS topicSharePct
        WHERE continuityPct > baselineContinuityPct
        RETURN topic,
               previousUsers,
               retainedUsers,
               previousActiveUsers,
               retainedActiveUsers,
               baselineContinuityPct,
               continuityPct,
               liftPct,
               topicSharePct
        ORDER BY liftPct DESC, previousUsers DESC, topic ASC
        LIMIT 8
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
        "noise": NOISY_TOPIC_VALUES,
        "min_comments": MIN_ACTIVE_COMMENTS,
        "min_topic_support": MIN_TOPIC_SUPPORT,
        "prior_strength": SMOOTHING_PRIOR,
    }, op_name="predictive.retention_factors"))


def get_churn_signals(ctx: DashboardDateContext) -> list[dict]:
    """Topic-level drop-off risk among previously active members."""
    return _cached_rows("churn_signals", ctx, lambda: run_query("""
        CALL {
            MATCH (u:User)-[:WROTE]->(c:Comment)
            WHERE c.posted_at >= datetime($previous_start)
              AND c.posted_at < datetime($previous_end)
            WITH u, count(DISTINCT c) AS previousComments
            WHERE previousComments >= $min_comments
            WITH u,
                 EXISTS {
                     MATCH (u)-[:WROTE]->(c2:Comment)
                     WHERE c2.posted_at >= datetime($start)
                       AND c2.posted_at < datetime($end)
                     WITH count(DISTINCT c2) AS currentComments
                     WHERE currentComments >= $min_comments
                     RETURN 1
                 } AS retained
            RETURN count(DISTINCT u) AS previousActiveUsers,
                   count(DISTINCT CASE WHEN NOT retained THEN u END) AS droppedActiveUsers
        }
        MATCH (u:User)-[:WROTE]->(c:Comment)
        WHERE c.posted_at >= datetime($previous_start)
          AND c.posted_at < datetime($previous_end)
        WITH u, previousActiveUsers, droppedActiveUsers, count(DISTINCT c) AS previousComments
        WHERE previousComments >= $min_comments
        WITH u,
             previousActiveUsers,
             droppedActiveUsers,
             NOT EXISTS {
                 MATCH (u)-[:WROTE]->(c2:Comment)
                 WHERE c2.posted_at >= datetime($start)
                   AND c2.posted_at < datetime($end)
                 WITH count(DISTINCT c2) AS currentComments
                 WHERE currentComments >= $min_comments
                 RETURN 1
             } AS dropped
        MATCH (u)-[:WROTE]->(topicComment:Comment)-[:TAGGED]->(t:Topic)
        WHERE topicComment.posted_at >= datetime($previous_start)
          AND topicComment.posted_at < datetime($previous_end)
          AND coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
        WITH t.name AS topic,
             count(DISTINCT u) AS previousUsers,
             count(DISTINCT CASE WHEN dropped THEN u END) AS lostUsers,
             previousActiveUsers,
             droppedActiveUsers,
             CASE
                 WHEN previousActiveUsers = 0 THEN 0.0
                 ELSE 100.0 * droppedActiveUsers / previousActiveUsers
             END AS baselineDropoffPct
        WHERE previousUsers >= $min_topic_support
          AND lostUsers > 0
        WITH topic,
             previousUsers,
             lostUsers,
             previousActiveUsers,
             droppedActiveUsers,
             baselineDropoffPct,
             100.0 * (lostUsers + ($prior_strength * baselineDropoffPct / 100.0)) / (previousUsers + $prior_strength) AS smoothedDropoffPct,
             100.0 * previousUsers / previousActiveUsers AS topicSharePct
        WITH topic,
             previousUsers,
             lostUsers,
             previousActiveUsers,
             droppedActiveUsers,
             round(baselineDropoffPct, 1) AS baselineDropoffPctRounded,
             round(smoothedDropoffPct, 1) AS dropoffPctRounded,
             round(topicSharePct, 1) AS topicSharePctRounded
        WITH topic,
             previousUsers,
             lostUsers,
             previousActiveUsers,
             droppedActiveUsers,
             baselineDropoffPctRounded AS baselineDropoffPct,
             dropoffPctRounded AS dropoffPct,
             round(dropoffPctRounded - baselineDropoffPctRounded, 1) AS excessRiskPct,
             topicSharePctRounded AS topicSharePct
        WHERE excessRiskPct > 0
        RETURN topic,
               lostUsers,
               previousUsers,
               previousActiveUsers,
               droppedActiveUsers,
               baselineDropoffPct,
               dropoffPct,
               excessRiskPct,
               topicSharePct
        ORDER BY excessRiskPct DESC, lostUsers DESC, topic ASC
        LIMIT 10
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
        "noise": NOISY_TOPIC_VALUES,
        "min_comments": MIN_ACTIVE_COMMENTS,
        "min_topic_support": MIN_TOPIC_SUPPORT,
        "prior_strength": SMOOTHING_PRIOR,
    }, op_name="predictive.churn_signals"))


def get_growth_funnel(ctx: DashboardDateContext) -> list[dict]:
    """Cumulative engagement funnel based on audience reach and observed user maturity."""
    params = {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "ask_hints": FUNNEL_ASK_HINTS,
    }

    channel_rows = run_query("""
        MATCH (ch:Channel)<-[:IN_CHANNEL]-(p:Post)
        WHERE p.posted_at >= datetime($start)
          AND p.posted_at < datetime($end)
        RETURN coalesce(ch.uuid, ch.username, ch.title, elementId(ch)) AS channelId,
               max(coalesce(ch.member_count, 0)) AS memberCount,
               collect(coalesce(p.views, 0)) AS views
    """, params)

    overlap_rows = run_query("""
        MATCH (u:User)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
        WHERE c.posted_at >= datetime($start)
          AND c.posted_at < datetime($end)
        RETURN coalesce(toString(u.telegram_user_id), elementId(u)) AS userId,
               count(DISTINCT coalesce(ch.uuid, ch.username, ch.title, elementId(ch))) AS channelCount
    """, params)

    user_rows = run_query("""
        MATCH (u:User)
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WITH c,
                 coalesce(ch.uuid, ch.username, ch.title, '') AS channelKey,
                 toLower(trim(coalesce(c.text, ''))) AS textLower
            RETURN count(DISTINCT c) AS comments,
                   count(DISTINCT CASE WHEN c IS NOT NULL THEN date(c.posted_at) END) AS activeDays,
                   count(DISTINCT CASE WHEN channelKey <> '' THEN channelKey END) AS channels,
                   count(DISTINCT CASE
                       WHEN c IS NOT NULL
                        AND (c.text CONTAINS '?' OR any(h IN $ask_hints WHERE textLower CONTAINS h))
                       THEN coalesce(c.uuid, elementId(c))
                   END) AS askComments
        }
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[ri:EXHIBITS]->(intent:Intent)
            WHERE ri.last_seen >= datetime($start)
              AND ri.last_seen < datetime($end)
            RETURN count(DISTINCT CASE WHEN intent.name = 'Information Seeking' THEN intent END) AS infoSeekingSignals,
                   count(DISTINCT CASE WHEN intent.name = 'Support / Help' THEN intent END) AS supportSignals
        }
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[r:REPLIED_TO_USER]->()
            WHERE coalesce(r.last_seen, r.first_seen) >= datetime($start)
              AND coalesce(r.last_seen, r.first_seen) < datetime($end)
            RETURN count(DISTINCT r) AS replyCount
        }
        WITH u, comments, activeDays, channels, askComments, infoSeekingSignals, supportSignals, replyCount
        WHERE comments > 0
        RETURN coalesce(toString(u.telegram_user_id), elementId(u)) AS userId,
               comments,
               activeDays,
               channels,
               (askComments + infoSeekingSignals) AS askSignals,
               (replyCount + supportSignals) AS helpSignals
    """, params)

    active_channel_count = len(channel_rows)
    total_members = 0
    reader_proxy_sum = 0.0
    for row in channel_rows:
        member_count = max(0, int(row.get("memberCount") or 0))
        view_values = [max(0.0, float(v or 0)) for v in (row.get("views") or [])]
        p75_views = _percentile(view_values, FUNNEL_READER_VIEW_PERCENTILE)
        total_members += member_count
        reader_proxy_sum += min(float(member_count), p75_views) if member_count > 0 else p75_views

    observed_overlap_users = len(overlap_rows)
    observed_channel_incidents = sum(max(0, int(row.get("channelCount") or 0)) for row in overlap_rows)
    if active_channel_count <= 1 or observed_channel_incidents <= 0:
        dedup_factor = 1.0
    else:
        dedup_factor = _clamp(
            float(observed_overlap_users) / float(observed_channel_incidents),
            FUNNEL_DEDUP_FLOOR,
            1.0,
        )

    contributor_comment_min = max(4, math.ceil(ctx.days / 7))
    contributor_day_min = max(2, math.ceil(ctx.days / 14))
    leader_comment_min = max(8, math.ceil(ctx.days / 5))
    leader_day_min = max(3, math.ceil(ctx.days / 14))
    leader_help_min = max(3, math.ceil(ctx.days / 14))

    users: list[dict[str, float | int | str]] = []
    max_comments = 0
    max_days = 0
    max_channels = 0
    max_help = 0
    for row in user_rows:
        comments = max(0, int(row.get("comments") or 0))
        active_days = max(0, int(row.get("activeDays") or 0))
        channels = max(0, int(row.get("channels") or 0))
        ask_signals = max(0, int(row.get("askSignals") or 0))
        help_signals = max(0, int(row.get("helpSignals") or 0))
        users.append({
            "userId": str(row.get("userId") or ""),
            "comments": comments,
            "activeDays": active_days,
            "channels": channels,
            "askSignals": ask_signals,
            "helpSignals": help_signals,
        })
        max_comments = max(max_comments, comments)
        max_days = max(max_days, active_days)
        max_channels = max(max_channels, channels)
        max_help = max(max_help, help_signals)

    ask_users: set[str] = set()
    help_users: set[str] = set()
    contributor_users: set[str] = set()
    leadership_scores: dict[str, float] = {}

    for user in users:
        user_id = str(user["userId"])
        comments = int(user["comments"])
        active_days = int(user["activeDays"])
        channels = int(user["channels"])
        ask_signals = int(user["askSignals"])
        help_signals = int(user["helpSignals"])
        if ask_signals > 0 or help_signals > 0:
            ask_users.add(user_id)
        if help_signals > 0:
            help_users.add(user_id)
        if help_signals > 0 and comments >= contributor_comment_min and active_days >= contributor_day_min:
            contributor_users.add(user_id)
            score = (
                0.50 * (float(help_signals) / float(max_help or 1)) +
                0.25 * (float(active_days) / float(max_days or 1)) +
                0.15 * (float(channels) / float(max_channels or 1)) +
                0.10 * (float(comments) / float(max_comments or 1))
            )
            leadership_scores[user_id] = score

    leader_cutoff = _percentile(list(leadership_scores.values()), FUNNEL_LEADER_SCORE_PERCENTILE)
    leader_users = {
        str(user["userId"])
        for user in users
        if str(user["userId"]) in contributor_users
        and int(user["comments"]) >= leader_comment_min
        and int(user["activeDays"]) >= leader_day_min
        and int(user["helpSignals"]) >= leader_help_min
        and leadership_scores.get(str(user["userId"]), 0.0) >= leader_cutoff
    }

    observed_users = len(users)
    ask_count = len(ask_users)
    help_count = len(help_users)
    contributor_count = len(contributor_users)
    leader_count = len(leader_users)

    reads_estimate = int(round(reader_proxy_sum * dedup_factor))
    if total_members > 0:
        all_members = int(round(total_members * dedup_factor))
        reads = min(reads_estimate, all_members)
    else:
        all_members = 0
        reads = reads_estimate

    reads = max(reads, observed_users, ask_count, help_count, contributor_count, leader_count)
    all_members = max(all_members, reads, observed_users)

    return [
        {"stage": "all", "users": all_members},
        {"stage": "reads", "users": reads},
        {"stage": "asks", "users": ask_count},
        {"stage": "helps", "users": help_count},
        {"stage": "contributes", "users": contributor_count},
        {"stage": "leads", "users": leader_count},
    ]


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


def _week_start_utc(value: datetime) -> datetime:
    dt = value.astimezone(timezone.utc)
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc) - timedelta(days=dt.weekday())


def _week_buckets(ctx: DashboardDateContext, limit: int = 8) -> list[tuple[datetime, datetime]]:
    first_bucket_start = _week_start_utc(ctx.start_at)
    last_observed = ctx.end_at - timedelta(microseconds=1)
    last_bucket_start = _week_start_utc(last_observed)

    buckets: list[tuple[datetime, datetime]] = []
    cursor = first_bucket_start
    while cursor <= last_bucket_start:
        buckets.append((cursor, cursor + timedelta(days=7)))
        cursor += timedelta(days=7)
    return buckets[-limit:]


def get_new_vs_returning_voice_widget(ctx: DashboardDateContext) -> dict:
    """Weekly distinct-comment-author cohorts for the New vs. Returning Voices widget."""
    buckets = _week_buckets(ctx)
    if not buckets:
        return {"buckets": [], "topTopics": [], "meta": {"bucketSizeDays": 7}}

    active_rows = run_query("""
        MATCH (u:User)-[:WROTE]->(window_c:Comment)
        WHERE u.telegram_user_id IS NOT NULL
          AND window_c.posted_at >= datetime($start)
          AND window_c.posted_at < datetime($end)
        WITH DISTINCT u
        MATCH (u)-[:WROTE]->(all_c:Comment)
        WITH u, min(all_c.posted_at) AS firstVoiceAt
        MATCH (u)-[:WROTE]->(window_c:Comment)
        WHERE window_c.posted_at >= datetime($start)
          AND window_c.posted_at < datetime($end)
        WITH DISTINCT
             toString(u.telegram_user_id) AS userId,
             firstVoiceAt,
             date.truncate('week', date(window_c.posted_at)) AS bucketDate
        RETURN userId,
               toString(firstVoiceAt) AS firstVoiceAt,
               toString(bucketDate) AS bucketStart
        ORDER BY bucketStart, userId
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
    })

    first_bucket_by_user: dict[str, str] = {}
    active_users_by_bucket: dict[str, set[str]] = {}
    for row in active_rows:
        user_id = str(row.get("userId") or "").strip()
        bucket_start = str(row.get("bucketStart") or "").strip()
        first_voice_at = str(row.get("firstVoiceAt") or "").strip()
        if not user_id or not bucket_start or not first_voice_at:
            continue
        try:
            first_bucket = _week_start_utc(datetime.fromisoformat(first_voice_at.replace("Z", "+00:00"))).date().isoformat()
        except Exception:
            continue
        first_bucket_by_user[user_id] = first_bucket
        active_users_by_bucket.setdefault(bucket_start, set()).add(user_id)

    bucket_items: list[dict] = []
    for bucket_start, _bucket_end in buckets:
        bucket_key = bucket_start.date().isoformat()
        active_users = active_users_by_bucket.get(bucket_key, set())
        new_voices = sum(1 for user_id in active_users if first_bucket_by_user.get(user_id) == bucket_key)
        returning = sum(1 for user_id in active_users if first_bucket_by_user.get(user_id, "") < bucket_key)
        bucket_items.append({
            "week": bucket_key,
            "newVoices": new_voices,
            "returning": returning,
        })

    latest_bucket_start, latest_bucket_end = buckets[-1]
    latest_bucket_key = latest_bucket_start.date().isoformat()
    latest_new_voices = next((item["newVoices"] for item in bucket_items if item["week"] == latest_bucket_key), 0)

    top_topic_rows = run_query("""
        MATCH (u:User)-[:WROTE]->(c:Comment)
        WHERE u.telegram_user_id IS NOT NULL
        WITH u, min(c.posted_at) AS firstVoiceAt
        WHERE firstVoiceAt >= datetime($bucket_start)
          AND firstVoiceAt < datetime($bucket_end)
        MATCH (u)-[:WROTE]->(first_c:Comment)-[:TAGGED]->(t:Topic)
        WHERE first_c.posted_at = firstVoiceAt
          AND coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
        RETURN t.name AS topic,
               count(DISTINCT toString(u.telegram_user_id)) AS newVoices
        ORDER BY newVoices DESC, topic ASC
        LIMIT 6
    """, {
        "bucket_start": latest_bucket_start.isoformat(),
        "bucket_end": latest_bucket_end.isoformat(),
        "noise": NOISY_TOPIC_VALUES,
    }) if latest_new_voices > 0 else []

    top_topics = [{
        "topic": str(row.get("topic") or "").strip(),
        "newVoices": int(row.get("newVoices") or 0),
        "pct": round((int(row.get("newVoices") or 0) / latest_new_voices) * 100, 1) if latest_new_voices > 0 else 0,
    } for row in top_topic_rows if str(row.get("topic") or "").strip()]

    return {
        "buckets": bucket_items,
        "topTopics": top_topics,
        "meta": {
            "bucketSizeDays": 7,
            "bucketCount": len(bucket_items),
            "latestBucketStart": latest_bucket_key,
            "definition": "distinct comment authors; new equals first-ever observed comment in bucket",
        },
    }
