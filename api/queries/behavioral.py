"""
behavioral.py — Tier 3: Problems & Satisfaction (pain point monitoring)

Provides: problems, serviceGaps, satisfactionAreas, moodData, urgencySignals
"""
from __future__ import annotations
from api.db import run_query


_NOISY_TOPIC_KEYS = ["", "null", "unknown", "none", "n/a", "na"]
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


def get_problems() -> list[dict]:
    """Topic-level problem signals from message-level sentiment evidence."""
    return run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE NOT toLower(trim(coalesce(t.name, ''))) IN $noise
          AND coalesce(t.proposed, false) = false

        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at > datetime() - duration('P30D')
              AND s.label IN $negative_labels
            OPTIONAL MATCH (p)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            RETURN coalesce(p.uuid, 'post:' + elementId(p)) AS msgId,
                   p.posted_at AS ts,
                   left(trim(coalesce(p.text, '')), 180) AS txt,
                   s.label AS primaryLabel,
                   collect(DISTINCT tag.name) AS tagNames
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at > datetime() - duration('P30D')
              AND s.label IN $negative_labels
            OPTIONAL MATCH (c)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            RETURN coalesce(c.uuid, 'comment:' + elementId(c)) AS msgId,
                   c.posted_at AS ts,
                   left(trim(coalesce(c.text, '')), 180) AS txt,
                   s.label AS primaryLabel,
                   collect(DISTINCT tag.name) AS tagNames
        }

        WITH t, cat, msgId, ts, txt, primaryLabel,
             CASE WHEN any(tag IN tagNames WHERE tag IN $distress_tags) THEN 1 ELSE 0 END AS distressHit
        WITH t, cat,
             count(DISTINCT msgId) AS affectedSignals,
             count(DISTINCT CASE WHEN primaryLabel = 'Urgent' THEN msgId END) AS urgentSignals,
             count(DISTINCT CASE WHEN distressHit = 1 THEN msgId END) AS distressSignals,
             count(DISTINCT CASE WHEN ts > datetime() - duration('P7D') THEN msgId END) AS affectedThisWeek,
             count(DISTINCT CASE WHEN ts > datetime() - duration('P14D')
                                   AND ts <= datetime() - duration('P7D') THEN msgId END) AS affectedPrevWeek,
             collect(CASE WHEN txt <> '' THEN txt END)[0] AS sampleText
        WHERE affectedSignals >= 3
        WITH t, cat, affectedSignals, urgentSignals, distressSignals, affectedThisWeek, affectedPrevWeek,
             coalesce(sampleText, '') AS sampleText,
             (affectedThisWeek + affectedPrevWeek) AS trendSupport
        RETURN t.name AS topic,
               cat.name AS category,
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
        {
            "noise": _NOISY_TOPIC_KEYS,
            "negative_labels": _NEGATIVE_SENTIMENTS,
            "distress_tags": _DISTRESS_TAGS,
        },
    )


def get_service_gaps() -> list[dict]:
    """Topics with strong demand and high dissatisfaction from message-level evidence."""
    return run_query(
        """
        MATCH (t:Topic)
        WHERE NOT toLower(trim(coalesce(t.name, ''))) IN $noise
          AND coalesce(t.proposed, false) = false

        CALL (t) {
            OPTIONAL MATCH (u:User)-[i:INTERESTED_IN]->(t)
            WHERE i.last_seen > datetime() - duration('P30D')
            RETURN count(DISTINCT u) AS demand,
                   count(DISTINCT CASE WHEN i.last_seen > datetime() - duration('P7D') THEN u END) AS demandThisWeek,
                   count(DISTINCT CASE WHEN i.last_seen > datetime() - duration('P14D')
                                         AND i.last_seen <= datetime() - duration('P7D') THEN u END) AS demandPrevWeek
        }

        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at > datetime() - duration('P30D')
            RETURN s.label AS label, count(*) AS cnt
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at > datetime() - duration('P30D')
            RETURN s.label AS label, count(*) AS cnt
        }

        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
            MATCH (p)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            WHERE p.posted_at > datetime() - duration('P30D')
              AND tag.name IN $distress_tags
            RETURN count(*) AS distressCnt
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
            MATCH (c)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            WHERE c.posted_at > datetime() - duration('P30D')
              AND tag.name IN $distress_tags
            RETURN count(*) AS distressCnt
        }

        WITH t.name AS topic,
             demand,
             demandThisWeek,
             demandPrevWeek,
             sum(CASE WHEN label IN $negative_labels THEN cnt ELSE 0 END) AS negCount,
             sum(CASE WHEN label = 'Positive' THEN cnt ELSE 0 END) AS posCount,
             sum(CASE WHEN label IN ['Neutral', 'Mixed'] THEN cnt ELSE 0 END) AS neutralCount,
             sum(distressCnt) AS distressTagCount
        WHERE demand > 3

        WITH topic,
             demand,
             demandThisWeek,
             demandPrevWeek,
             negCount,
             posCount,
             neutralCount,
             distressTagCount,
             (negCount + posCount + neutralCount) AS sentimentEvidence
        WHERE sentimentEvidence > 0
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
        {
            "noise": _NOISY_TOPIC_KEYS,
            "negative_labels": _NEGATIVE_SENTIMENTS,
            "distress_tags": _DISTRESS_TAGS,
        },
    )


def get_problem_brief_candidates(
    *,
    days: int = 30,
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

        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at > datetime() - duration({days: $days})
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
            WHERE c.posted_at > datetime() - duration({days: $days})
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
             count(DISTINCT CASE WHEN ts > datetime() - duration('P7D') THEN evidenceId END) AS signals7d,
             count(DISTINCT CASE WHEN ts > datetime() - duration('P14D')
                                  AND ts <= datetime() - duration('P7D') THEN evidenceId END) AS signalsPrev7d,
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
            "days": safe_days,
            "limit_topics": safe_limit_topics,
            "evidence_per_topic": safe_evidence,
            "noise": _NOISY_TOPIC_KEYS,
            "negative_labels": _NEGATIVE_SENTIMENTS,
            "distress_tags": _DISTRESS_TAGS,
        },
    )


def get_service_gap_brief_candidates(
    *,
    days: int = 30,
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

        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at > datetime() - duration({days: $days})
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            OPTIONAL MATCH (p)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            WITH p, s, ch, collect(DISTINCT tag.name) AS tagNames,
                 toLower(trim(p.text)) AS textLower
            RETURN
                coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                'post' AS kind,
                left(trim(p.text), 2600) AS text,
                '' AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                '' AS userId,
                p.posted_at AS ts,
                s.label AS label,
                tagNames,
                CASE
                    WHEN p.text CONTAINS '?'
                      OR any(h IN $ask_hints WHERE textLower CONTAINS h)
                    THEN 1
                    ELSE 0
                END AS askLike,
                0 AS supportIntent
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at > datetime() - duration({days: $days})
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (c)-[:HAS_SENTIMENT_TAG]->(tag:SentimentTag)
            OPTIONAL MATCH (u)-[:EXHIBITS]->(intent:Intent)
            WITH c, p, u, ch, s, collect(DISTINCT tag.name) AS tagNames,
                 max(CASE WHEN intent.name IN ['Support / Help', 'Information Seeking'] THEN 1 ELSE 0 END) AS supportIntent
            WITH c, p, u, ch, s, tagNames, supportIntent,
                 toLower(trim(c.text)) AS textLower,
                 toLower(trim(coalesce(p.text, ''))) AS contextLower
            RETURN
                coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                'comment' AS kind,
                left(trim(c.text), 2600) AS text,
                left(coalesce(p.text, ''), 1200) AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                coalesce(toString(u.telegram_user_id), '') AS userId,
                c.posted_at AS ts,
                s.label AS label,
                tagNames,
                CASE
                    WHEN c.text CONTAINS '?'
                      OR any(h IN $ask_hints WHERE textLower CONTAINS h OR contextLower CONTAINS h)
                    THEN 1
                    ELSE 0
                END AS askLike,
                supportIntent
        }

        WITH t, cat, evidenceId, kind, text, parentText, channel, userId, ts, label, askLike, supportIntent,
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
                 askLike: askLike,
                 supportIntent: supportIntent,
                 ts: ts
             }) AS rows,
             count(DISTINCT CASE WHEN askLike = 1 OR supportIntent = 1 THEN evidenceId END) AS askSignals,
             count(DISTINCT CASE
                 WHEN askLike = 1 OR supportIntent = 1
                 THEN CASE WHEN trim(coalesce(userId, '')) <> '' THEN userId ELSE 'channel:' + toLower(trim(coalesce(channel, 'unknown'))) END
             END) AS uniqueUsers,
             count(DISTINCT CASE WHEN askLike = 1 OR supportIntent = 1 THEN toLower(trim(coalesce(channel, 'unknown'))) END) AS channelCount,
             count(DISTINCT CASE WHEN ts > datetime() - duration('P7D') AND (askLike = 1 OR supportIntent = 1) THEN evidenceId END) AS asks7d,
             count(DISTINCT CASE WHEN ts > datetime() - duration('P14D')
                                  AND ts <= datetime() - duration('P7D')
                                  AND (askLike = 1 OR supportIntent = 1) THEN evidenceId END) AS asksPrev7d,
             count(DISTINCT CASE
                 WHEN (askLike = 1 OR supportIntent = 1) AND label IN $negative_labels
                 THEN evidenceId
             END) AS negCount,
             count(DISTINCT CASE
                 WHEN (askLike = 1 OR supportIntent = 1) AND label = 'Positive'
                 THEN evidenceId
             END) AS posCount,
             count(DISTINCT CASE
                 WHEN (askLike = 1 OR supportIntent = 1) AND label IN ['Neutral', 'Mixed']
                 THEN evidenceId
             END) AS neutralCount,
             count(DISTINCT CASE
                 WHEN (askLike = 1 OR supportIntent = 1) AND distressHit = 1
                 THEN evidenceId
             END) AS distressTagCount,
             max(ts) AS latestTs
        WITH t, cat, rows, askSignals, uniqueUsers, channelCount, asks7d, asksPrev7d,
             negCount, posCount, neutralCount, distressTagCount, latestTs,
             (negCount + posCount + neutralCount) AS sentimentEvidence
        WHERE sentimentEvidence > 0
          AND askSignals >= 2
        WITH t, cat, rows, askSignals, uniqueUsers, channelCount, asks7d, asksPrev7d,
             negCount, distressTagCount, latestTs,
             round(100.0 * (negCount + distressTagCount) / (sentimentEvidence + distressTagCount + 1), 1) AS unmetPct,
             CASE
                 WHEN (asks7d + asksPrev7d) < 8 THEN 0
                 ELSE toInteger(round(100.0 * (asks7d - asksPrev7d) / (asksPrev7d + 3)))
             END AS trend7dPct
        ORDER BY unmetPct DESC, askSignals DESC
        LIMIT $limit_topics

        UNWIND rows AS row
        WITH t, cat, askSignals, uniqueUsers, channelCount, asks7d, asksPrev7d, trend7dPct, unmetPct, latestTs, row
        WHERE row.askLike = 1 OR row.supportIntent = 1
        ORDER BY row.ts DESC
        WITH t, cat, askSignals, uniqueUsers, channelCount, asks7d, asksPrev7d, trend7dPct, unmetPct, latestTs,
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
            "days": safe_days,
            "limit_topics": safe_limit_topics,
            "evidence_per_topic": safe_evidence,
            "noise": _NOISY_TOPIC_KEYS,
            "negative_labels": _NEGATIVE_SENTIMENTS,
            "distress_tags": _DISTRESS_TAGS,
            "ask_hints": _SERVICE_REQUEST_HINTS,
        },
    )


def get_satisfaction_areas() -> list[dict]:
    """Satisfaction scores per Topic from message-level sentiment."""
    return run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
        WHERE NOT toLower(trim(coalesce(t.name, ''))) IN $noise
          AND coalesce(t.proposed, false) = false
        CALL (t) {
            MATCH (p:Post)-[:TAGGED]->(t)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at > datetime() - duration('P30D')
            RETURN s.label AS label, count(*) AS cnt
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at > datetime() - duration('P30D')
            RETURN s.label AS label, count(*) AS cnt
        }
        WITH t.name AS category,
             sum(CASE WHEN label = 'Positive' THEN cnt ELSE 0 END) AS pos,
             sum(CASE WHEN label IN ['Negative', 'Urgent', 'Sarcastic'] THEN cnt ELSE 0 END) AS neg,
             sum(CASE WHEN label IN ['Neutral', 'Mixed'] THEN cnt ELSE 0 END) AS neu
        WITH category, pos, neg, neu, (pos + neg + neu) AS volume
        WHERE volume > 2
        WITH category, pos, neg, neu, volume, round(100.0 * pos / (volume + 0.001), 1) AS satisfactionPct
        WITH collect({category: category, pos: pos, neg: neg, neu: neu, volume: volume, satisfactionPct: satisfactionPct}) AS allTopics
        
        UNWIND allTopics AS tPos
        WITH allTopics, tPos ORDER BY tPos.satisfactionPct DESC, tPos.volume DESC
        WITH allTopics, collect(tPos)[..8] AS topPos
        
        UNWIND allTopics AS tNeg
        WITH topPos, tNeg ORDER BY tNeg.satisfactionPct ASC, tNeg.volume DESC
        WITH topPos, collect(tNeg)[..8] AS topNeg
        
        UNWIND (topPos + topNeg) AS row
        WITH DISTINCT row.category AS category, row.pos AS pos, row.neg AS neg, row.neu AS neu, row.satisfactionPct AS satisfactionPct, row.volume AS volume
        RETURN category, pos, neg, neu, satisfactionPct
        ORDER BY volume DESC
        """,
        {"noise": _NOISY_TOPIC_KEYS},
    )


def get_mood_data() -> list[dict]:
    """Weekly sentiment distribution for mood-over-time chart from message-level edges."""
    return run_query(
        """
        CALL () {
            MATCH (p:Post)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at > datetime() - duration('P84D')
            RETURN date(p.posted_at).year AS year,
                   date(p.posted_at).week AS week,
                   s.label AS sentiment,
                   count(*) AS count
            UNION ALL
            MATCH (c:Comment)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at > datetime() - duration('P84D')
            RETURN date(c.posted_at).year AS year,
                   date(c.posted_at).week AS week,
                   s.label AS sentiment,
                   count(*) AS count
        }
        RETURN year, week, sentiment, sum(count) AS count
        ORDER BY year, week
        """
    )


def get_urgency_brief_candidates(days: int = 14, limit_topics: int = 15, evidence_per_topic: int = 6) -> list[dict]:
    """Clusters of explicitly urgent messages for AI synthesis."""
    return run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE NOT toLower(trim(coalesce(t.name, ''))) IN $noise
          AND coalesce(t.proposed, false) = false
        CALL (t) {
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
