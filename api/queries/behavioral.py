"""
behavioral.py — Tier 3: Problems & Satisfaction (pain point monitoring)

Provides: problems, serviceGaps, satisfactionAreas, moodData, urgencySignals
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import threading
import time
from api.dashboard_dates import DashboardDateContext
from api.db import run_query


_NOISY_TOPIC_KEYS = ["", "null", "unknown", "none", "n/a", "na"]
_TOPIC_SCOPE_LIMIT = 20
_TOPIC_SCOPE_TTL_SECONDS = 120.0
_TOPIC_SCOPE_CACHE: dict[tuple[str, int], tuple[float, list[str]]] = {}
_TOPIC_SCOPE_LOCK = threading.Lock()
_NEGATIVE_SENTIMENTS = ["Negative", "Urgent", "Sarcastic"]
_DISTRESS_TAGS = ["Anxious", "Frustrated", "Angry", "Exhausted", "Grief", "Distrustful", "Confused"]
_SERVICE_REQUEST_HINTS = [
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


def _window_params(ctx: DashboardDateContext) -> dict[str, object]:
    return {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
    }


def _brief_window_params(days: int, ctx: DashboardDateContext | None = None) -> dict[str, object]:
    if ctx is None:
        window_end = datetime.now(timezone.utc)
        window_start = window_end - timedelta(days=max(7, days))
        compare_days = min(7, max(1, days - 1))
        current_start = max(window_start, window_end - timedelta(days=compare_days))
        previous_end = current_start
        previous_start = max(window_start, previous_end - timedelta(days=compare_days))
        return {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
            "current_start": current_start.isoformat(),
            "previous_start": previous_start.isoformat(),
            "previous_end": previous_end.isoformat(),
        }

    compare_days = min(7, max(1, ctx.days - 1))
    current_start = max(ctx.start_at, ctx.end_at - timedelta(days=compare_days))
    previous_end = current_start
    previous_start = max(ctx.start_at, previous_end - timedelta(days=compare_days))
    return {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "current_start": current_start.isoformat(),
        "previous_start": previous_start.isoformat(),
        "previous_end": previous_end.isoformat(),
    }


def _window_topic_names(ctx: DashboardDateContext, *, limit: int = _TOPIC_SCOPE_LIMIT) -> list[str]:
    cache_key = (ctx.cache_key, int(limit))
    now = time.time()
    with _TOPIC_SCOPE_LOCK:
        cached = _TOPIC_SCOPE_CACHE.get(cache_key)
        if cached and (now - cached[0]) < _TOPIC_SCOPE_TTL_SECONDS:
            return list(cached[1])
        stale_keys = [key for key, (ts, _value) in _TOPIC_SCOPE_CACHE.items() if (now - ts) >= _TOPIC_SCOPE_TTL_SECONDS]
        for stale_key in stale_keys:
            _TOPIC_SCOPE_CACHE.pop(stale_key, None)

    params = _window_params(ctx)
    params.update({"noise": _NOISY_TOPIC_KEYS, "topic_limit": max(1, int(limit))})
    rows = run_query(
        """
        CALL () {
            MATCH (p:Post)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND coalesce(t.proposed, false) = false
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
            RETURN t.name AS topic, count(DISTINCT p) AS mentions
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND coalesce(t.proposed, false) = false
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
            RETURN t.name AS topic, count(DISTINCT c) AS mentions
        }
        WITH topic, sum(mentions) AS totalMentions
        WHERE totalMentions > 0
        RETURN topic
        ORDER BY totalMentions DESC, topic ASC
        LIMIT $topic_limit
        """,
        params,
    )
    topic_names = [str(row.get("topic") or "").strip() for row in rows if str(row.get("topic") or "").strip()]
    with _TOPIC_SCOPE_LOCK:
        _TOPIC_SCOPE_CACHE[cache_key] = (time.time(), list(topic_names))
    return topic_names


def _bounded_window_params(ctx: DashboardDateContext, *, limit: int = _TOPIC_SCOPE_LIMIT) -> dict[str, object]:
    params = _window_params(ctx)
    params.update({"topic_names": _window_topic_names(ctx, limit=limit)})
    return params


def get_problems(ctx: DashboardDateContext) -> list[dict]:
    """Topic-level problem signals from message-level sentiment evidence."""
    params = {
        "noise": _NOISY_TOPIC_KEYS,
        "negative_labels": _NEGATIVE_SENTIMENTS,
        "distress_tags": _DISTRESS_TAGS,
        **_bounded_window_params(ctx),
    }
    if not params["topic_names"]:
        return []
    return run_query(
        """
        CALL {
            MATCH (p:Post)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND s.label IN $negative_labels
              AND coalesce(t.proposed, false) = false
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
              AND t.name IN $topic_names
            OPTIONAL MATCH (p)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            RETURN t.name AS topic,
                   cat.name AS category,
                   coalesce(p.uuid, 'post:' + elementId(p)) AS msgId,
                   p.posted_at AS ts,
                   left(trim(coalesce(p.text, '')), 180) AS txt,
                   s.label AS primaryLabel,
                   collect(DISTINCT tag.name) AS tagNames
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND s.label IN $negative_labels
              AND coalesce(t.proposed, false) = false
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
              AND t.name IN $topic_names
            OPTIONAL MATCH (c)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            RETURN t.name AS topic,
                   cat.name AS category,
                   coalesce(c.uuid, 'comment:' + elementId(c)) AS msgId,
                   c.posted_at AS ts,
                   left(trim(coalesce(c.text, '')), 180) AS txt,
                   s.label AS primaryLabel,
                   collect(DISTINCT tag.name) AS tagNames
        }

        WITH topic, category, msgId, ts, txt, primaryLabel,
             CASE WHEN any(tag IN tagNames WHERE tag IN $distress_tags) THEN 1 ELSE 0 END AS distressHit
        WITH topic, category,
             count(DISTINCT msgId) AS affectedSignals,
             count(DISTINCT CASE WHEN primaryLabel = 'Urgent' THEN msgId END) AS urgentSignals,
             count(DISTINCT CASE WHEN distressHit = 1 THEN msgId END) AS distressSignals,
             count(DISTINCT CASE WHEN ts >= datetime($start) AND ts < datetime($end) THEN msgId END) AS affectedThisWeek,
             count(DISTINCT CASE WHEN ts >= datetime($previous_start)
                                   AND ts < datetime($previous_end) THEN msgId END) AS affectedPrevWeek,
             collect(CASE WHEN txt <> '' THEN txt END)[0] AS sampleText
        WHERE affectedSignals >= 3
        WITH topic, category, affectedSignals, urgentSignals, distressSignals, affectedThisWeek, affectedPrevWeek,
             coalesce(sampleText, '') AS sampleText,
             (affectedThisWeek + affectedPrevWeek) AS trendSupport
        RETURN topic,
               category,
               affectedSignals AS affectedUsers,
               affectedThisWeek,
               affectedPrevWeek,
               trendSupport,
               sampleText,
               CASE
                    WHEN urgentSignals >= 8 THEN 'Urgent'
                    WHEN affectedSignals >= 20
                         AND (1.0 * urgentSignals / affectedSignals) >= 0.22 THEN 'Urgent'
                    WHEN affectedSignals > 0
                         AND distressSignals >= 24
                         AND (1.0 * distressSignals / affectedSignals) >= 0.60 THEN 'Urgent'
                    ELSE 'Negative'
               END AS severity,
               CASE
                    WHEN trendSupport < 8 THEN null
                    ELSE round(100.0 * (affectedThisWeek - affectedPrevWeek) / (affectedPrevWeek + 3), 1)
               END AS trendPct
        ORDER BY (urgentSignals * 3 + distressSignals * 2 + affectedSignals) DESC
        LIMIT 20
        """,
        params,
    )


def get_service_gaps(ctx: DashboardDateContext) -> list[dict]:
    """Topics with strong demand and high dissatisfaction from message-level evidence."""
    params = {
        "noise": _NOISY_TOPIC_KEYS,
        "negative_labels": _NEGATIVE_SENTIMENTS,
        "distress_tags": _DISTRESS_TAGS,
        **_bounded_window_params(ctx),
    }
    if not params["topic_names"]:
        return []
    return run_query(
        """
        UNWIND $topic_names AS topic
        CALL {
            WITH topic
            MATCH (u:User)-[i:INTERESTED_IN]->(t:Topic)
            WHERE t.name = topic
              AND i.last_seen >= datetime($previous_start)
              AND i.last_seen < datetime($end)
            RETURN
                count(DISTINCT CASE
                    WHEN i.last_seen >= datetime($start)
                     AND i.last_seen < datetime($end)
                    THEN u
                END) AS demandThisWeek,
                count(DISTINCT CASE
                    WHEN i.last_seen >= datetime($previous_start)
                     AND i.last_seen < datetime($previous_end)
                    THEN u
                END) AS demandPrevWeek
        }
        CALL {
            WITH topic
            MATCH (p:Post)-[:TAGGED]->(t:Topic {name: topic})-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            OPTIONAL MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            OPTIONAL MATCH (p)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            WITH coalesce(p.uuid, 'post:' + elementId(p)) AS messageId,
                 max(CASE WHEN s.label IN $negative_labels THEN 1 ELSE 0 END) AS isNegative,
                 max(CASE WHEN s.label = 'Positive' THEN 1 ELSE 0 END) AS isPositive,
                 max(CASE WHEN s.label IN ['Neutral', 'Mixed'] THEN 1 ELSE 0 END) AS isNeutral,
                 max(CASE WHEN tag.name IN $distress_tags THEN 1 ELSE 0 END) AS hasDistress
            RETURN
                sum(isNegative) AS postNegCount,
                sum(isPositive) AS postPosCount,
                sum(isNeutral) AS postNeutralCount,
                sum(hasDistress) AS postDistressTagCount
        }
        CALL {
            WITH topic
            MATCH (c:Comment)-[:TAGGED]->(t:Topic {name: topic})-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            OPTIONAL MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            OPTIONAL MATCH (c)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            WITH coalesce(c.uuid, 'comment:' + elementId(c)) AS messageId,
                 max(CASE WHEN s.label IN $negative_labels THEN 1 ELSE 0 END) AS isNegative,
                 max(CASE WHEN s.label = 'Positive' THEN 1 ELSE 0 END) AS isPositive,
                 max(CASE WHEN s.label IN ['Neutral', 'Mixed'] THEN 1 ELSE 0 END) AS isNeutral,
                 max(CASE WHEN tag.name IN $distress_tags THEN 1 ELSE 0 END) AS hasDistress
            RETURN
                sum(isNegative) AS commentNegCount,
                sum(isPositive) AS commentPosCount,
                sum(isNeutral) AS commentNeutralCount,
                sum(hasDistress) AS commentDistressTagCount
        }

        WITH topic,
             demandThisWeek,
             demandPrevWeek,
             (demandThisWeek + demandPrevWeek) AS demand,
             (coalesce(postNegCount, 0) + coalesce(commentNegCount, 0)) AS negCount,
             (coalesce(postPosCount, 0) + coalesce(commentPosCount, 0)) AS posCount,
             (coalesce(postNeutralCount, 0) + coalesce(commentNeutralCount, 0)) AS neutralCount,
             (coalesce(postDistressTagCount, 0) + coalesce(commentDistressTagCount, 0)) AS distressTagCount,
             (
                coalesce(postNegCount, 0) + coalesce(commentNegCount, 0) +
                coalesce(postPosCount, 0) + coalesce(commentPosCount, 0) +
                coalesce(postNeutralCount, 0) + coalesce(commentNeutralCount, 0)
             ) AS sentimentEvidence
        WHERE sentimentEvidence > 0
          AND (demandThisWeek + demandPrevWeek) > 3
        RETURN topic,
               demand,
               negCount,
               posCount,
               demandThisWeek,
               demandPrevWeek,
               (demandThisWeek + demandPrevWeek) AS demandGrowthSupport,
               round(100.0 * (negCount + distressTagCount) / (sentimentEvidence + distressTagCount + 1), 1) AS dissatisfactionPct,
               CASE
                    WHEN (demandThisWeek + demandPrevWeek) < 8 THEN null
                    ELSE round(100.0 * (demandThisWeek - demandPrevWeek) / (demandPrevWeek + 3), 1)
               END AS demandGrowthPct
        ORDER BY dissatisfactionPct DESC, demand DESC
        LIMIT 15
        """,
        params,
    )


def get_problem_brief_candidates(
    *,
    days: int = 30,
    ctx: DashboardDateContext | None = None,
    limit_topics: int = 16,
    evidence_per_topic: int = 14,
) -> list[dict]:
    """Candidate bundles for AI problem cards (W8)."""
    safe_days = max(7, min(int(days), 90))
    safe_limit_topics = max(6, min(int(limit_topics), 32))
    safe_evidence = max(6, min(int(evidence_per_topic), 24))

    return run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND s.label IN $negative_labels
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            OPTIONAL MATCH (p)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            RETURN
                coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                'post' AS kind,
                left(trim(p.text), 2600) AS text,
                '' AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                '' AS userId,
                p.posted_at AS ts,
                s.label AS label,
                collect(DISTINCT tag.name) AS tagNames
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND s.label IN $negative_labels
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (c)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            RETURN
                coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                'comment' AS kind,
                left(trim(c.text), 2600) AS text,
                left(coalesce(p.text, ''), 1200) AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                coalesce(toString(u.telegram_user_id), '') AS userId,
                c.posted_at AS ts,
                s.label AS label,
                collect(DISTINCT tag.name) AS tagNames
        }

        WITH t, cat, evidenceId, kind, text, parentText, channel, userId, ts, label,
             CASE WHEN any(tag IN tagNames WHERE tag IN $distress_tags) THEN 1 ELSE 0 END AS distressHit
        WITH t, cat,
             collect({
                 id: evidenceId,
                 kind: kind,
                 text: text,
                 parentText: parentText,
                 channel: channel,
                 userId: userId,
                 timestamp: toString(ts),
                 label: label,
                 distressHit: distressHit,
                 ts: ts
             }) AS rows,
             count(DISTINCT evidenceId) AS signalCount,
             count(DISTINCT CASE WHEN label = 'Urgent' THEN evidenceId END) AS urgentSignals,
             count(DISTINCT CASE WHEN distressHit = 1 THEN evidenceId END) AS distressSignals,
             count(DISTINCT CASE
                 WHEN ts >= datetime($current_start) AND ts < datetime($end)
                 THEN evidenceId END) AS signals7d,
             count(DISTINCT CASE
                 WHEN ts >= datetime($previous_start) AND ts < datetime($previous_end)
                 THEN evidenceId END) AS signalsPrev7d,
             count(DISTINCT CASE
                 WHEN trim(coalesce(userId, '')) <> '' THEN userId
                 ELSE 'channel:' + toLower(trim(coalesce(channel, 'unknown')))
             END) AS uniqueUsers,
             count(DISTINCT toLower(trim(coalesce(channel, 'unknown')))) AS channelCount,
             max(ts) AS latestTs
        WHERE signalCount >= 4
        WITH t, cat, rows, signalCount, urgentSignals, distressSignals, signals7d, signalsPrev7d, uniqueUsers, channelCount, latestTs,
             CASE
                 WHEN urgentSignals >= 8 THEN 'critical'
                 WHEN signalCount >= 20
                      AND (1.0 * urgentSignals / signalCount) >= 0.22 THEN 'high'
                 WHEN distressSignals >= 16 THEN 'high'
                 WHEN signalCount >= 10 THEN 'medium'
                 ELSE 'low'
             END AS severity,
             CASE
                 WHEN (signals7d + signalsPrev7d) < 8 THEN 0
                 ELSE toInteger(round(100.0 * (signals7d - signalsPrev7d) / (signalsPrev7d + 3)))
             END AS trend7dPct
        ORDER BY (urgentSignals * 3 + distressSignals * 2 + signalCount) DESC
        LIMIT $limit_topics

        UNWIND rows AS row
        WITH t, cat, signalCount, uniqueUsers, channelCount, signals7d, signalsPrev7d, trend7dPct, severity, latestTs, row
        ORDER BY row.ts DESC
        WITH t, cat, signalCount, uniqueUsers, channelCount, signals7d, signalsPrev7d, trend7dPct, severity, latestTs,
             collect({
                id: row.id,
                kind: row.kind,
                text: row.text,
                parentText: row.parentText,
                channel: row.channel,
                userId: row.userId,
                timestamp: row.timestamp,
                label: row.label,
                distressHit: row.distressHit
             })[..$evidence_per_topic] AS evidence
        RETURN
            t.name AS topic,
            cat.name AS category,
            signalCount,
            uniqueUsers,
            channelCount,
            signals7d,
            signalsPrev7d,
            trend7dPct,
            severity,
            toString(latestTs) AS latestAt,
            evidence
        ORDER BY signalCount DESC, latestAt DESC
        """,
        {
            "limit_topics": safe_limit_topics,
            "evidence_per_topic": safe_evidence,
            "noise": _NOISY_TOPIC_KEYS,
            "negative_labels": _NEGATIVE_SENTIMENTS,
            "distress_tags": _DISTRESS_TAGS,
            **_brief_window_params(safe_days, ctx),
        },
    )


def get_service_gap_brief_candidates(
    *,
    days: int = 30,
    ctx: DashboardDateContext | None = None,
    limit_topics: int = 16,
    evidence_per_topic: int = 14,
) -> list[dict]:
    """Candidate bundles for AI service-gap cards (W9)."""
    safe_days = max(7, min(int(days), 90))
    safe_limit_topics = max(6, min(int(limit_topics), 32))
    safe_evidence = max(6, min(int(evidence_per_topic), 24))

    return run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
            WITH p, s, toLower(trim(p.text)) AS textLower
            WITH p, s,
                 CASE
                    WHEN p.text CONTAINS '?'
                      OR any(h IN $ask_hints WHERE textLower CONTAINS h)
                    THEN 1 ELSE 0
                 END AS askLike
            RETURN
                count(CASE WHEN askLike = 1 THEN 1 END) AS postAskSignals,
                count(CASE WHEN askLike = 1 AND s.label IN $negative_labels THEN 1 END) AS postNegCount,
                count(CASE WHEN askLike = 1 AND s.label = 'Positive' THEN 1 END) AS postPosCount,
                count(CASE WHEN askLike = 1 AND s.label IN ['Neutral', 'Mixed'] THEN 1 END) AS postNeutralCount,
                count(CASE WHEN askLike = 1 AND EXISTS {
                    MATCH (p)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
                    WHERE tag.name IN $distress_tags
                } THEN 1 END) AS postDistressTagCount,
                count(CASE WHEN askLike = 1 AND p.posted_at >= datetime($current_start) AND p.posted_at < datetime($end) THEN 1 END) AS postAsks7d,
                count(CASE WHEN askLike = 1 AND p.posted_at >= datetime($previous_start)
                            AND p.posted_at < datetime($previous_end) THEN 1 END) AS postAsksPrev7d,
                max(CASE WHEN askLike = 1 THEN p.posted_at END) AS latestPostTs
        }

        CALL {
            WITH t
            MATCH (c:Comment)-[:TAGGED]->(t)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (u)-[:EXHIBITS]->(intent:Intent)
            WITH c, p, s,
                 max(CASE WHEN intent.name IN ['Support / Help', 'Information Seeking'] THEN 1 ELSE 0 END) AS supportIntent,
                 toLower(trim(c.text)) AS textLower,
                 toLower(trim(coalesce(p.text, ''))) AS contextLower
            WITH c, p, s, supportIntent,
                 CASE
                    WHEN c.text CONTAINS '?'
                      OR any(h IN $ask_hints WHERE textLower CONTAINS h OR contextLower CONTAINS h)
                    THEN 1 ELSE 0
                 END AS askLike
            RETURN
                count(CASE WHEN askLike = 1 OR supportIntent = 1 THEN 1 END) AS commentAskSignals,
                count(CASE WHEN (askLike = 1 OR supportIntent = 1) AND s.label IN $negative_labels THEN 1 END) AS commentNegCount,
                count(CASE WHEN (askLike = 1 OR supportIntent = 1) AND s.label = 'Positive' THEN 1 END) AS commentPosCount,
                count(CASE WHEN (askLike = 1 OR supportIntent = 1) AND s.label IN ['Neutral', 'Mixed'] THEN 1 END) AS commentNeutralCount,
                count(CASE WHEN (askLike = 1 OR supportIntent = 1) AND EXISTS {
                    MATCH (c)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
                    WHERE tag.name IN $distress_tags
                } THEN 1 END) AS commentDistressTagCount,
                count(CASE WHEN (askLike = 1 OR supportIntent = 1) AND c.posted_at >= datetime($current_start) AND c.posted_at < datetime($end) THEN 1 END) AS commentAsks7d,
                count(CASE WHEN (askLike = 1 OR supportIntent = 1) AND c.posted_at >= datetime($previous_start)
                            AND c.posted_at < datetime($previous_end) THEN 1 END) AS commentAsksPrev7d,
                max(CASE WHEN askLike = 1 OR supportIntent = 1 THEN c.posted_at END) AS latestCommentTs
        }

        WITH t, cat,
             (postAskSignals + commentAskSignals) AS askSignals,
             (postAsks7d + commentAsks7d) AS asks7d,
             (postAsksPrev7d + commentAsksPrev7d) AS asksPrev7d,
             (postNegCount + commentNegCount) AS negCount,
             (postPosCount + commentPosCount) AS posCount,
             (postNeutralCount + commentNeutralCount) AS neutralCount,
             (postDistressTagCount + commentDistressTagCount) AS distressTagCount,
             CASE
                 WHEN latestPostTs IS NULL THEN latestCommentTs
                 WHEN latestCommentTs IS NULL THEN latestPostTs
                 WHEN latestPostTs >= latestCommentTs THEN latestPostTs
                 ELSE latestCommentTs
             END AS latestTs
        WITH t, cat, askSignals, asks7d, asksPrev7d, negCount, posCount, neutralCount, distressTagCount, latestTs,
             (negCount + posCount + neutralCount) AS sentimentEvidence
        WHERE sentimentEvidence > 0
          AND askSignals >= 2
        WITH t, cat, askSignals, asks7d, asksPrev7d, latestTs,
             round(100.0 * (negCount + distressTagCount) / (sentimentEvidence + distressTagCount + 1), 1) AS unmetPct,
             CASE
                 WHEN (asks7d + asksPrev7d) < 8 THEN 0
                 ELSE toInteger(round(100.0 * (asks7d - asksPrev7d) / (asksPrev7d + 3)))
             END AS trend7dPct
        ORDER BY unmetPct DESC, askSignals DESC
        LIMIT $limit_topics

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            OPTIONAL MATCH (p)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            WITH p, s, ch, collect(DISTINCT tag.name) AS tagNames,
                 toLower(trim(p.text)) AS textLower
            WITH p, s, ch, tagNames,
                 CASE
                    WHEN p.text CONTAINS '?'
                      OR any(h IN $ask_hints WHERE textLower CONTAINS h)
                    THEN 1 ELSE 0
                 END AS askLike
            RETURN
                coalesce(p.uuid, 'post:' + elementId(p)) AS id,
                'post' AS kind,
                left(trim(p.text), 2600) AS text,
                '' AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                '' AS userId,
                toString(p.posted_at) AS timestamp,
                p.posted_at AS ts,
                s.label AS label,
                CASE WHEN any(tagName IN tagNames WHERE tagName IN $distress_tags) THEN 1 ELSE 0 END AS distressHit,
                askLike,
                0 AS supportIntent
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (c)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            OPTIONAL MATCH (u)-[:EXHIBITS]->(intent:Intent)
            WITH c, p, u, ch, s, collect(DISTINCT tag.name) AS tagNames,
                 max(CASE WHEN intent.name IN ['Support / Help', 'Information Seeking'] THEN 1 ELSE 0 END) AS supportIntent,
                 toLower(trim(c.text)) AS textLower,
                 toLower(trim(coalesce(p.text, ''))) AS contextLower
            WITH c, p, u, ch, s, tagNames, supportIntent,
                 CASE
                    WHEN c.text CONTAINS '?'
                      OR any(h IN $ask_hints WHERE textLower CONTAINS h OR contextLower CONTAINS h)
                    THEN 1 ELSE 0
                 END AS askLike
            RETURN
                coalesce(c.uuid, 'comment:' + elementId(c)) AS id,
                'comment' AS kind,
                left(trim(c.text), 2600) AS text,
                left(coalesce(p.text, ''), 1200) AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                coalesce(toString(u.telegram_user_id), '') AS userId,
                toString(c.posted_at) AS timestamp,
                c.posted_at AS ts,
                s.label AS label,
                CASE WHEN any(tagName IN tagNames WHERE tagName IN $distress_tags) THEN 1 ELSE 0 END AS distressHit,
                askLike,
                supportIntent
        }
        WITH t, cat, askSignals, asks7d, asksPrev7d, trend7dPct, unmetPct, latestTs,
             collect({
                id: id,
                kind: kind,
                text: text,
                parentText: parentText,
                channel: channel,
                userId: userId,
                timestamp: timestamp,
                label: label,
                distressHit: distressHit,
                askLike: askLike,
                supportIntent: supportIntent,
                ts: ts
             }) AS rows
        UNWIND rows AS row
        WITH t, cat, askSignals, asks7d, asksPrev7d, trend7dPct, unmetPct, latestTs, row
        WHERE row.askLike = 1 OR row.supportIntent = 1
        ORDER BY row.ts DESC
        WITH t, cat, askSignals, asks7d, asksPrev7d, trend7dPct, unmetPct, latestTs,
             count(DISTINCT CASE
                 WHEN trim(coalesce(row.userId, '')) <> '' THEN row.userId
                 ELSE 'channel:' + toLower(trim(coalesce(row.channel, 'unknown')))
             END) AS uniqueUsers,
             count(DISTINCT toLower(trim(coalesce(row.channel, 'unknown')))) AS channelCount,
             collect({
                id: row.id,
                kind: row.kind,
                text: row.text,
                parentText: row.parentText,
                channel: row.channel,
                userId: row.userId,
                timestamp: row.timestamp,
                label: row.label,
                distressHit: row.distressHit,
                askLike: row.askLike,
                supportIntent: row.supportIntent
             })[..$evidence_per_topic] AS evidence
        RETURN
            t.name AS topic,
            cat.name AS category,
            askSignals AS signalCount,
            uniqueUsers,
            channelCount,
            asks7d AS signals7d,
            asksPrev7d AS signalsPrev7d,
            trend7dPct,
            unmetPct,
            toString(latestTs) AS latestAt,
            evidence
        ORDER BY unmetPct DESC, signalCount DESC, latestAt DESC
        """,
        {
            "limit_topics": safe_limit_topics,
            "evidence_per_topic": safe_evidence,
            "noise": _NOISY_TOPIC_KEYS,
            "negative_labels": _NEGATIVE_SENTIMENTS,
            "distress_tags": _DISTRESS_TAGS,
            "ask_hints": _SERVICE_REQUEST_HINTS,
            **_brief_window_params(safe_days, ctx),
        },
    )


def get_satisfaction_areas(ctx: DashboardDateContext) -> list[dict]:
    """Confidence-weighted satisfaction scores per Topic from message-level sentiment."""
    params = {"noise": _NOISY_TOPIC_KEYS, **_bounded_window_params(ctx)}
    if not params["topic_names"]:
        return []
    return run_query(
        """
        CALL () {
            MATCH (p:Post)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
            WHERE p.posted_at >= datetime($previous_start)
              AND p.posted_at < datetime($end)
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
              AND coalesce(t.proposed, false) = false
              AND t.name IN $topic_names
            OPTIONAL MATCH (p)-[hs:HAS_SENTIMENT]->(s:Sentiment)
            WITH t.name AS topic,
                 coalesce(p.uuid, 'post:' + elementId(p)) AS msgId,
                 p.posted_at AS ts,
                 s.label AS label,
                 coalesce(hs.last_seen, hs.first_seen, p.posted_at) AS signalTs,
                 CASE s.label
                    WHEN 'Urgent' THEN 6
                    WHEN 'Negative' THEN 5
                    WHEN 'Sarcastic' THEN 4
                    WHEN 'Mixed' THEN 3
                    WHEN 'Positive' THEN 2
                    WHEN 'Neutral' THEN 1
                    ELSE 0
                 END AS precedence
            ORDER BY msgId, signalTs DESC, precedence DESC
            WITH topic, msgId, ts, collect(label)[0] AS resolvedLabel
            WHERE resolvedLabel IS NOT NULL
            RETURN topic, msgId, ts, resolvedLabel AS label
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
            WHERE c.posted_at >= datetime($previous_start)
              AND c.posted_at < datetime($end)
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
              AND coalesce(t.proposed, false) = false
              AND t.name IN $topic_names
            OPTIONAL MATCH (c)-[hs:HAS_SENTIMENT]->(s:Sentiment)
            WITH t.name AS topic,
                 coalesce(c.uuid, 'comment:' + elementId(c)) AS msgId,
                 c.posted_at AS ts,
                 s.label AS label,
                 coalesce(hs.last_seen, hs.first_seen, c.posted_at) AS signalTs,
                 CASE s.label
                    WHEN 'Urgent' THEN 6
                    WHEN 'Negative' THEN 5
                    WHEN 'Sarcastic' THEN 4
                    WHEN 'Mixed' THEN 3
                    WHEN 'Positive' THEN 2
                    WHEN 'Neutral' THEN 1
                    ELSE 0
                 END AS precedence
            ORDER BY msgId, signalTs DESC, precedence DESC
            WITH topic, msgId, ts, collect(label)[0] AS resolvedLabel
            WHERE resolvedLabel IS NOT NULL
            RETURN topic, msgId, ts, resolvedLabel AS label
        }
        WITH topic AS category,
             count(DISTINCT CASE WHEN label = 'Positive' THEN msgId END) AS pos,
             count(DISTINCT CASE WHEN label IN ['Negative', 'Urgent', 'Sarcastic'] THEN msgId END) AS neg,
             count(DISTINCT CASE WHEN label IN ['Neutral', 'Mixed'] THEN msgId END) AS neu,
             count(DISTINCT CASE WHEN ts >= datetime($start) AND ts < datetime($end) AND label = 'Positive' THEN msgId END) AS posCurrent,
             count(DISTINCT CASE WHEN ts >= datetime($start)
                                   AND ts < datetime($end)
                                   AND label IN ['Negative', 'Urgent', 'Sarcastic'] THEN msgId END) AS negCurrent,
             count(DISTINCT CASE WHEN ts >= datetime($start)
                                   AND ts < datetime($end)
                                   AND label IN ['Neutral', 'Mixed'] THEN msgId END) AS neuCurrent,
             count(DISTINCT CASE WHEN ts >= datetime($previous_start)
                                   AND ts < datetime($previous_end)
                                   AND label = 'Positive' THEN msgId END) AS posPrevious,
             count(DISTINCT CASE WHEN ts >= datetime($previous_start)
                                   AND ts < datetime($previous_end)
                                   AND label IN ['Negative', 'Urgent', 'Sarcastic'] THEN msgId END) AS negPrevious,
             count(DISTINCT CASE WHEN ts >= datetime($previous_start)
                                   AND ts < datetime($previous_end)
                                   AND label IN ['Neutral', 'Mixed'] THEN msgId END) AS neuPrevious
        WITH category,
             pos,
             neg,
             neu,
             (pos + neg + neu) AS volume,
             (posCurrent + negCurrent + neuCurrent) AS currentVolume,
             (posPrevious + negPrevious + neuPrevious) AS previousVolume,
             round(100.0 * (pos + 2.0) / ((pos + neg + neu) + 6.0), 1) AS satisfactionPct,
             round(
                (100.0 * (posCurrent + 2.0) / ((posCurrent + negCurrent + neuCurrent) + 6.0))
                - (100.0 * (posPrevious + 2.0) / ((posPrevious + negPrevious + neuPrevious) + 6.0)),
                1
             ) AS trendPct
        WHERE volume >= 8
        WITH collect({
            category: category,
            pos: pos,
            neg: neg,
            neu: neu,
            volume: volume,
            currentVolume: currentVolume,
            previousVolume: previousVolume,
            satisfactionPct: satisfactionPct,
            trendPct: trendPct
        }) AS allTopics
        
        UNWIND allTopics AS tPos
        WITH allTopics, tPos ORDER BY tPos.satisfactionPct DESC, tPos.volume DESC
        WITH allTopics, collect(tPos)[..8] AS topPos
        
        UNWIND allTopics AS tNeg
        WITH topPos, tNeg ORDER BY tNeg.satisfactionPct ASC, tNeg.volume DESC
        WITH topPos, collect(tNeg)[..8] AS topNeg
        
        UNWIND (topPos + topNeg) AS row
        WITH DISTINCT row.category AS category,
             row.pos AS pos,
             row.neg AS neg,
             row.neu AS neu,
             row.satisfactionPct AS satisfactionPct,
             row.trendPct AS trendPct,
             row.currentVolume AS currentVolume,
             row.previousVolume AS previousVolume,
             row.volume AS volume
        RETURN category, pos, neg, neu, volume, currentVolume, previousVolume, satisfactionPct, trendPct
        ORDER BY volume DESC
        """,
        params,
    )


def get_mood_data(ctx: DashboardDateContext) -> list[dict]:
    """Selected-window refined mood buckets for mood-over-time chart."""
    return run_query(
        """
        CALL () {
            MATCH (p:Post)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            OPTIONAL MATCH (p)-[hs:HAS_SENTIMENT]->(s:Sentiment)
            OPTIONAL MATCH (p)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            WITH coalesce(p.uuid, 'post:' + elementId(p)) AS msgId,
                 toString(date(p.posted_at)) AS bucket,
                 s.label AS sentiment,
                 coalesce(hs.last_seen, hs.first_seen, p.posted_at) AS signalTs,
                 CASE s.label
                    WHEN 'Urgent' THEN 6
                    WHEN 'Negative' THEN 5
                    WHEN 'Sarcastic' THEN 4
                    WHEN 'Mixed' THEN 3
                    WHEN 'Positive' THEN 2
                    WHEN 'Neutral' THEN 1
                    ELSE 0
                 END AS precedence,
                 collect(DISTINCT tag.name) AS tags
            ORDER BY msgId, signalTs DESC, precedence DESC
            WITH msgId, bucket,
                 head(collect(sentiment)) AS sentiment,
                 [tagName IN reduce(allTags = [], tagSet IN collect(tags) | allTags + tagSet) WHERE tagName IS NOT NULL] AS tags
            WHERE sentiment IS NOT NULL
            WITH bucket,
                 sentiment,
                 tags,
                 any(tagName IN tags WHERE tagName IN ['Hopeful', 'Solidarity']) AS has_positive_energy,
                 any(tagName IN tags WHERE tagName = 'Trusting') AS has_trusting_signal,
                 any(tagName IN tags WHERE tagName IN ['Anxious', 'Confused', 'Exhausted', 'Grief']) AS has_anxiety_signal,
                 any(tagName IN tags WHERE tagName IN ['Frustrated', 'Angry', 'Distrustful']) AS has_conflict_signal
            RETURN
                bucket,
                CASE
                    WHEN sentiment = 'Positive' AND has_positive_energy THEN 0.65
                    WHEN sentiment = 'Positive' AND has_trusting_signal THEN 0.20
                    WHEN sentiment = 'Mixed' AND has_positive_energy THEN 0.15
                    ELSE 0.0
                END AS excited,
                CASE
                    WHEN sentiment = 'Positive' AND has_positive_energy THEN 0.35
                    WHEN sentiment = 'Positive' AND has_trusting_signal THEN 0.80
                    WHEN sentiment = 'Positive' THEN 1.0
                    WHEN sentiment = 'Mixed' AND has_positive_energy THEN 0.25
                    WHEN sentiment = 'Mixed' AND has_trusting_signal THEN 0.20
                    ELSE 0.0
                END AS satisfied,
                CASE
                    WHEN sentiment = 'Neutral' THEN 1.0
                    WHEN sentiment = 'Mixed' AND has_positive_energy THEN 0.60
                    WHEN sentiment = 'Mixed' AND has_trusting_signal THEN 0.80
                    WHEN sentiment = 'Mixed' AND has_anxiety_signal THEN 0.55
                    WHEN sentiment = 'Mixed' AND has_conflict_signal THEN 0.65
                    WHEN sentiment = 'Mixed' THEN 1.0
                    WHEN sentiment = 'Sarcastic' THEN 0.20
                    ELSE 0.0
                END AS neutral,
                CASE
                    WHEN sentiment = 'Negative' AND has_anxiety_signal THEN 0.35
                    WHEN sentiment = 'Negative' THEN 0.80
                    WHEN sentiment = 'Sarcastic' THEN 0.80
                    WHEN sentiment = 'Mixed' AND has_anxiety_signal THEN 0.20
                    WHEN sentiment = 'Mixed' AND has_conflict_signal THEN 0.35
                    ELSE 0.0
                END AS frustrated,
                CASE
                    WHEN sentiment = 'Urgent' THEN 1.0
                    WHEN sentiment = 'Negative' AND has_anxiety_signal THEN 0.65
                    WHEN sentiment = 'Negative' THEN 0.20
                    WHEN sentiment = 'Mixed' AND has_anxiety_signal THEN 0.25
                    ELSE 0.0
                END AS anxious
            UNION ALL
            MATCH (c:Comment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            OPTIONAL MATCH (c)-[hs:HAS_SENTIMENT]->(s:Sentiment)
            OPTIONAL MATCH (c)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            WITH coalesce(c.uuid, 'comment:' + elementId(c)) AS msgId,
                 toString(date(c.posted_at)) AS bucket,
                 s.label AS sentiment,
                 coalesce(hs.last_seen, hs.first_seen, c.posted_at) AS signalTs,
                 CASE s.label
                    WHEN 'Urgent' THEN 6
                    WHEN 'Negative' THEN 5
                    WHEN 'Sarcastic' THEN 4
                    WHEN 'Mixed' THEN 3
                    WHEN 'Positive' THEN 2
                    WHEN 'Neutral' THEN 1
                    ELSE 0
                 END AS precedence,
                 collect(DISTINCT tag.name) AS tags
            ORDER BY msgId, signalTs DESC, precedence DESC
            WITH msgId, bucket,
                 head(collect(sentiment)) AS sentiment,
                 [tagName IN reduce(allTags = [], tagSet IN collect(tags) | allTags + tagSet) WHERE tagName IS NOT NULL] AS tags
            WHERE sentiment IS NOT NULL
            WITH bucket,
                 sentiment,
                 tags,
                 any(tagName IN tags WHERE tagName IN ['Hopeful', 'Solidarity']) AS has_positive_energy,
                 any(tagName IN tags WHERE tagName = 'Trusting') AS has_trusting_signal,
                 any(tagName IN tags WHERE tagName IN ['Anxious', 'Confused', 'Exhausted', 'Grief']) AS has_anxiety_signal,
                 any(tagName IN tags WHERE tagName IN ['Frustrated', 'Angry', 'Distrustful']) AS has_conflict_signal
            RETURN
                bucket,
                CASE
                    WHEN sentiment = 'Positive' AND has_positive_energy THEN 0.65
                    WHEN sentiment = 'Positive' AND has_trusting_signal THEN 0.20
                    WHEN sentiment = 'Mixed' AND has_positive_energy THEN 0.15
                    ELSE 0.0
                END AS excited,
                CASE
                    WHEN sentiment = 'Positive' AND has_positive_energy THEN 0.35
                    WHEN sentiment = 'Positive' AND has_trusting_signal THEN 0.80
                    WHEN sentiment = 'Positive' THEN 1.0
                    WHEN sentiment = 'Mixed' AND has_positive_energy THEN 0.25
                    WHEN sentiment = 'Mixed' AND has_trusting_signal THEN 0.20
                    ELSE 0.0
                END AS satisfied,
                CASE
                    WHEN sentiment = 'Neutral' THEN 1.0
                    WHEN sentiment = 'Mixed' AND has_positive_energy THEN 0.60
                    WHEN sentiment = 'Mixed' AND has_trusting_signal THEN 0.80
                    WHEN sentiment = 'Mixed' AND has_anxiety_signal THEN 0.55
                    WHEN sentiment = 'Mixed' AND has_conflict_signal THEN 0.65
                    WHEN sentiment = 'Mixed' THEN 1.0
                    WHEN sentiment = 'Sarcastic' THEN 0.20
                    ELSE 0.0
                END AS neutral,
                CASE
                    WHEN sentiment = 'Negative' AND has_anxiety_signal THEN 0.35
                    WHEN sentiment = 'Negative' THEN 0.80
                    WHEN sentiment = 'Sarcastic' THEN 0.80
                    WHEN sentiment = 'Mixed' AND has_anxiety_signal THEN 0.20
                    WHEN sentiment = 'Mixed' AND has_conflict_signal THEN 0.35
                    ELSE 0.0
                END AS frustrated,
                CASE
                    WHEN sentiment = 'Urgent' THEN 1.0
                    WHEN sentiment = 'Negative' AND has_anxiety_signal THEN 0.65
                    WHEN sentiment = 'Negative' THEN 0.20
                    WHEN sentiment = 'Mixed' AND has_anxiety_signal THEN 0.25
                    ELSE 0.0
                END AS anxious
        }
        WITH bucket,
             round(sum(excited), 2) AS excited_raw,
             round(sum(satisfied), 2) AS satisfied_raw,
             round(sum(neutral), 2) AS neutral_raw,
             round(sum(frustrated), 2) AS frustrated_raw,
             round(sum(anxious), 2) AS anxious_raw
        WITH bucket,
             excited_raw,
             satisfied_raw,
             neutral_raw,
             frustrated_raw,
             anxious_raw,
             (excited_raw + satisfied_raw + neutral_raw + frustrated_raw + anxious_raw) AS total_raw
        WITH bucket,
             excited_raw,
             satisfied_raw,
             neutral_raw,
             frustrated_raw,
             anxious_raw,
             CASE
                 WHEN total_raw < 8 THEN 0.55
                 WHEN total_raw < 20 THEN 0.80
                 ELSE 1.00
             END AS certainty
        RETURN bucket,
               round(excited_raw * certainty, 2) AS excited,
               round(satisfied_raw * certainty, 2) AS satisfied,
               round(neutral_raw + ((1.0 - certainty) * (excited_raw + satisfied_raw + frustrated_raw + anxious_raw)), 2) AS neutral,
               round(frustrated_raw * certainty, 2) AS frustrated,
               round(anxious_raw * certainty, 2) AS anxious
        ORDER BY bucket
        """,
        _window_params(ctx),
    )


def get_urgency_brief_candidates(days: int = 14, limit_topics: int = 15, evidence_per_topic: int = 6) -> list[dict]:
    """Clusters of explicitly urgent messages for AI synthesis."""
    return run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE NOT toLower(trim(coalesce(t.name, ''))) IN $noise
          AND coalesce(t.proposed, false) = false
        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment {label: 'Urgent'})
            WHERE p.posted_at > datetime() - duration('P' + toString($days) + 'D')
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            RETURN
                coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                'post' AS kind,
                left(trim(p.text), 2600) AS text,
                '' AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                '' AS userId,
                p.posted_at AS ts
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment {label: 'Urgent'})
            WHERE c.posted_at > datetime() - duration('P' + toString($days) + 'D')
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            RETURN
                coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                'comment' AS kind,
                left(trim(c.text), 2600) AS text,
                left(coalesce(p.text, ''), 1200) AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                coalesce(toString(u.telegram_user_id), '') AS userId,
                c.posted_at AS ts
        }
        WITH t, cat,
             collect({
                 id: evidenceId,
                 kind: kind,
                 text: text,
                 parentText: parentText,
                 channel: channel,
                 userId: userId,
                 timestamp: toString(ts),
                 ts: ts
             }) AS rows,
             count(DISTINCT evidenceId) AS messages,
             count(DISTINCT CASE WHEN trim(coalesce(userId, '')) <> '' THEN userId ELSE 'channel:' + toLower(trim(coalesce(channel, 'unknown'))) END) AS uniqueUsers,
             count(DISTINCT toLower(trim(coalesce(channel, 'unknown')))) AS channels,
             count(DISTINCT CASE WHEN ts > datetime() - duration('P7D') THEN evidenceId END) AS asks7d,
             count(DISTINCT CASE WHEN ts > datetime() - duration('P14D') AND ts <= datetime() - duration('P7D') THEN evidenceId END) AS asksPrev7d,
             max(ts) AS latestAt
        WHERE messages >= 1
        WITH t, cat, rows, messages, uniqueUsers, channels, asks7d, asksPrev7d, latestAt,
             CASE
                 WHEN (asks7d + asksPrev7d) < 8 THEN 0
                 ELSE toInteger(round(100.0 * (asks7d - asksPrev7d) / (asksPrev7d + 3)))
             END AS trend7dPct
        UNWIND rows AS row
        WITH t, cat, messages, uniqueUsers, channels, trend7dPct, latestAt, row
        ORDER BY row.ts DESC
        WITH t, cat, messages, uniqueUsers, channels, trend7dPct, latestAt,
             collect({
                id: row.id,
                kind: row.kind,
                message: row.text,
                context: row.parentText,
                channel: row.channel,
                timestamp: row.timestamp
             })[..$evidence_per_topic] AS evidence
        RETURN
            elementId(t) AS clusterId,
            t.name AS topic,
            cat.name AS category,
            messages,
            uniqueUsers,
            channels,
            trend7dPct,
            evidence AS signals,
            toString(latestAt) AS latestAt
        ORDER BY messages DESC
        LIMIT $limit_topics
        """,
        {
            "noise": _NOISY_TOPIC_KEYS,
            "days": days,
            "limit_topics": limit_topics,
            "evidence_per_topic": evidence_per_topic,
        },
    )
