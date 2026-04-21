"""
strategic.py — Tier 2: What People Talk About (deep topic intelligence)

Provides: topicBubbles, trendLines, heatmap, questionCategories,
questionBriefCandidates, lifecycleStages
"""
from __future__ import annotations
import threading
import time
from datetime import timedelta

import config
from api.dashboard_dates import DashboardDateContext
from api.db import run_query


RETENTION_DAYS = max(8, int(getattr(config, "GRAPH_ANALYTICS_RETENTION_DAYS", 15)))
COMPARE_DAYS = 7
LIFECYCLE_BASELINE_DAYS = max(1, RETENTION_DAYS - COMPARE_DAYS)
TOPIC_SCOPE_LIMIT = 24
TOPIC_SCOPE_TTL_SECONDS = 120.0
_NOISE_TOPICS = ["", "null", "unknown", "none", "n/a", "na"]
_TOPIC_SCOPE_CACHE: dict[tuple[str, int], tuple[float, list[str]]] = {}
_TOPIC_SCOPE_LOCK = threading.Lock()

def _strategic_window_params(ctx: DashboardDateContext) -> dict[str, object]:
    compare_days = min(COMPARE_DAYS, max(1, ctx.days - 1))
    current_start = max(ctx.start_at, ctx.end_at - timedelta(days=compare_days))
    previous_end = current_start
    previous_start = max(ctx.start_at, previous_end - timedelta(days=compare_days))
    baseline_days = max(1, int((previous_end - previous_start).total_seconds() // 86400))
    return {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "current_start": current_start.isoformat(),
        "previous_start": previous_start.isoformat(),
        "previous_end": previous_end.isoformat(),
        "compare_days": compare_days,
        "baseline_days": baseline_days,
        "total_days": ctx.days,
    }


def _window_topic_names(ctx: DashboardDateContext, *, limit: int = TOPIC_SCOPE_LIMIT) -> list[str]:
    cache_key = (ctx.cache_key, int(limit))
    now = time.time()
    with _TOPIC_SCOPE_LOCK:
        cached = _TOPIC_SCOPE_CACHE.get(cache_key)
        if cached and (now - cached[0]) < TOPIC_SCOPE_TTL_SECONDS:
            return list(cached[1])
        stale_keys = [key for key, (ts, _value) in _TOPIC_SCOPE_CACHE.items() if (now - ts) >= TOPIC_SCOPE_TTL_SECONDS]
        for stale_key in stale_keys:
            _TOPIC_SCOPE_CACHE.pop(stale_key, None)

    params = _strategic_window_params(ctx)
    params.update({"noise": _NOISE_TOPICS, "topic_limit": max(1, int(limit))})
    rows = run_query("""
        CALL {
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
    """, params)
    topic_names = [str(row.get("topic") or "").strip() for row in rows if str(row.get("topic") or "").strip()]
    with _TOPIC_SCOPE_LOCK:
        _TOPIC_SCOPE_CACHE[cache_key] = (time.time(), list(topic_names))
    return topic_names


def _bounded_window_params(ctx: DashboardDateContext, *, limit: int = TOPIC_SCOPE_LIMIT) -> dict[str, object]:
    topic_names = _window_topic_names(ctx, limit=limit)
    params = _strategic_window_params(ctx)
    params.update({"noise": _NOISE_TOPICS, "topic_names": topic_names})
    return params


def get_topic_bubbles(ctx: DashboardDateContext) -> list[dict]:
    """Topic bubble chart: recent topic prominence + reliable short-term growth."""
    params = _bounded_window_params(ctx)
    if not params["topic_names"]:
        return []
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']
          AND t.name IN $topic_names
        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN
                count(DISTINCT p) AS postMentionsRecent,
                count(DISTINCT CASE
                    WHEN p.posted_at >= datetime($current_start)
                     AND p.posted_at < datetime($end)
                    THEN p
                END) AS postsCurrent,
                count(DISTINCT CASE
                    WHEN p.posted_at >= datetime($previous_start)
                     AND p.posted_at < datetime($previous_end)
                    THEN p
                END) AS postsPrevious
        }
        CALL {
            WITH t
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN
                count(DISTINCT c) AS commentMentionsRecent,
                count(DISTINCT CASE
                    WHEN c.posted_at >= datetime($current_start)
                     AND c.posted_at < datetime($end)
                    THEN c
                END) AS commentsCurrent,
                count(DISTINCT CASE
                    WHEN c.posted_at >= datetime($previous_start)
                     AND c.posted_at < datetime($previous_end)
                    THEN c
                END) AS commentsPrevious
        }
        OPTIONAL MATCH (u:User)-[i:INTERESTED_IN]->(t)
        WITH t.name AS name, cat.name AS category,
             postMentionsRecent, commentMentionsRecent,
             count(DISTINCT u) AS userInterest,
             coalesce(sum(i.count), 0) AS totalInteractions,
             (postsCurrent + commentsCurrent) AS mentions7d,
             (postsPrevious + commentsPrevious) AS mentionsPrev7d,
             (postMentionsRecent + commentMentionsRecent) AS mentionCountRecent
        WITH name, category, postMentionsRecent, commentMentionsRecent, mentionCountRecent,
             userInterest, totalInteractions, mentions7d, mentionsPrev7d
        WHERE mentionCountRecent > 0
        RETURN name, category, postMentionsRecent AS postMentions, commentMentionsRecent AS commentMentions,
               mentionCountRecent AS mentionCount,
               userInterest, totalInteractions,
               mentions7d, mentionsPrev7d,
               (mentions7d + mentionsPrev7d) AS growthSupport,
               CASE
                    WHEN (mentions7d + mentionsPrev7d) < 8 THEN null
                    ELSE round(100.0 * (mentions7d - mentionsPrev7d) / (mentionsPrev7d + 3), 1)
                END AS growth7dPct
        ORDER BY mentionCount DESC
        LIMIT 40
    """, params)


def get_trend_lines(ctx: DashboardDateContext) -> list[dict]:
    """Daily conversation counts (posts + comments) per topic for the clean retention window."""
    params = _bounded_window_params(ctx, limit=12)
    if not params["topic_names"]:
        return []
    return run_query("""
        CALL {
            MATCH (p:Post)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND coalesce(t.proposed, false) = false
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
              AND t.name IN $topic_names
            RETURN t.name AS topic,
                   toString(date(p.posted_at)) AS bucket,
                   count(DISTINCT p) AS mentions
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND coalesce(t.proposed, false) = false
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
              AND t.name IN $topic_names
            RETURN t.name AS topic,
                   toString(date(c.posted_at)) AS bucket,
                   count(DISTINCT c) AS mentions
        }
        WITH topic, bucket, sum(mentions) AS posts
        WHERE posts > 0
        RETURN topic, bucket, posts
        ORDER BY topic, bucket
    """, params)


def get_heatmap(ctx: DashboardDateContext) -> list[dict]:
    """Content type × topic matrix (media_type distribution per topic)."""
    params = _bounded_window_params(ctx, limit=12)
    if not params["topic_names"]:
        return []
    return run_query("""
        MATCH (p:Post)-[:TAGGED]->(t:Topic)
        WHERE p.posted_at >= datetime($start)
          AND p.posted_at < datetime($end)
          AND t.name IN $topic_names
        WITH t.name AS topic, coalesce(p.media_type, 'text') AS mediaType,
             count(p) AS count
        RETURN topic, mediaType, count
        ORDER BY topic, count DESC
    """, params)


def get_question_categories(ctx: DashboardDateContext) -> list[dict]:
    """Real question messages by topic with deduplication and response proxy."""
    params = _bounded_window_params(ctx, limit=18)
    if not params["topic_names"]:
        return []
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']
          AND t.name IN $topic_names

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
              AND p.text CONTAINS '?'
            RETURN 'post:' + coalesce(p.uuid, elementId(p)) AS msg_key,
                   coalesce(p.uuid, 'post:' + elementId(p)) AS sample_id,
                   trim(p.text) AS txt,
                   p.posted_at AS ts
            UNION
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
              AND c.text CONTAINS '?'
            RETURN 'comment:' + coalesce(c.uuid, elementId(c)) AS msg_key,
                   coalesce(c.uuid, 'comment:' + elementId(c)) AS sample_id,
                   trim(c.text) AS txt,
                   c.posted_at AS ts
        }
        WITH t, cat, msg_key, sample_id, txt, ts,
             toLower(replace(replace(replace(replace(txt, '\n', ' '), '\r', ' '), '  ', ' '), '**', '')) AS normalized
        ORDER BY ts DESC
        WITH t, cat, normalized,
             count(DISTINCT msg_key) AS asks_per_form,
             max(ts) AS last_seen,
             head(collect(txt)) AS sample_question,
             head(collect(sample_id)) AS sample_question_id
        ORDER BY asks_per_form DESC, last_seen DESC
        WITH t, cat,
             collect({
                 form: normalized,
                 asks: asks_per_form,
                 sample: sample_question,
                 sample_id: sample_question_id,
                 last_seen: last_seen
             }) AS forms,
             sum(asks_per_form) AS times_asked
        WHERE times_asked > 0

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
              AND p.text CONTAINS '?'
            RETURN coalesce(p.uuid, 'post:' + elementId(p)) AS sample_id,
                   trim(p.text) AS txt,
                   p.posted_at AS ts
            UNION
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
              AND c.text CONTAINS '?'
            RETURN coalesce(c.uuid, 'comment:' + elementId(c)) AS sample_id,
                   trim(c.text) AS txt,
                   c.posted_at AS ts
        }
        WITH t, cat, forms, times_asked, sample_id, txt, ts
        ORDER BY ts DESC
        WITH t, cat, forms, times_asked,
             head(collect(sample_id)) AS latest_sample_id,
             head(collect(txt)) AS latest_sample

        CALL {
            WITH t
            OPTIONAL MATCH (u:User)-[:EXHIBITS]->(:Intent {name: 'Information Seeking'})
            WHERE EXISTS { MATCH (u)-[:INTERESTED_IN]->(t) }
            OPTIONAL MATCH (:User)-[r:REPLIED_TO_USER]->(u)
            RETURN
                count(DISTINCT u) AS intent_seekers,
                count(DISTINCT CASE
                    WHEN r.last_seen >= datetime($previous_start) THEN u
                END) AS responded_intent_seekers
        }

        WITH t, cat, forms, times_asked, latest_sample_id, latest_sample, intent_seekers, responded_intent_seekers

        WITH cat.name AS category,
             t.name AS topic,
             times_asked,
             forms,
             latest_sample_id,
             latest_sample,
             intent_seekers,
             responded_intent_seekers,
             CASE
               WHEN intent_seekers > 0
               THEN toInteger(round(times_asked * (toFloat(responded_intent_seekers) / toFloat(intent_seekers))))
               ELSE 0
             END AS responded_ask_estimate

        RETURN
            category,
            topic,
            times_asked AS seekers,
            CASE
              WHEN responded_ask_estimate > times_asked THEN times_asked
              WHEN responded_ask_estimate < 0 THEN 0
              ELSE responded_ask_estimate
            END AS respondedSeekers,
            CASE
                WHEN times_asked > 0
                THEN round(100.0 * toFloat(CASE
                    WHEN responded_ask_estimate > times_asked THEN times_asked
                    WHEN responded_ask_estimate < 0 THEN 0
                    ELSE responded_ask_estimate
                END) / toFloat(times_asked), 1)
                ELSE 0.0
            END AS coveragePct,
            coalesce(latest_sample, forms[0].sample) AS sampleQuestion,
            coalesce(latest_sample_id, forms[0].sample_id) AS sampleQuestionId
        ORDER BY seekers DESC, category, topic
        LIMIT 32
    """, params)


def get_question_brief_candidates(
    *,
    days: int = RETENTION_DAYS,
    limit_topics: int = 14,
    evidence_per_topic: int = 14,
) -> list[dict]:
    """Topic-scoped candidate bundles for AI-generated question briefs."""
    safe_days = max(7, min(int(days), 90))
    safe_limit_topics = max(4, min(int(limit_topics), 40))
    safe_evidence = max(6, min(int(evidence_per_topic), 24))

    return run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration({days: $days})
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
              AND p.text CONTAINS '?'
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            RETURN
                count(*) AS postSignals,
                count(CASE WHEN p.posted_at > datetime() - duration('P7D') THEN 1 END) AS postSignals7d,
                count(CASE WHEN p.posted_at > datetime() - duration('P14D')
                            AND p.posted_at <= datetime() - duration('P7D') THEN 1 END) AS postSignalsPrev7d,
                collect(DISTINCT coalesce(ch.title, ch.username, '')) AS postChannels
        }

        CALL {
            WITH t
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at > datetime() - duration({days: $days})
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
              AND c.text CONTAINS '?'
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            RETURN
                count(*) AS commentSignals,
                count(DISTINCT toString(u.telegram_user_id)) AS uniqueUsers,
                count(CASE WHEN c.posted_at > datetime() - duration('P7D') THEN 1 END) AS commentSignals7d,
                count(CASE WHEN c.posted_at > datetime() - duration('P14D')
                            AND c.posted_at <= datetime() - duration('P7D') THEN 1 END) AS commentSignalsPrev7d,
                collect(DISTINCT coalesce(ch.title, ch.username, '')) AS commentChannels
        }

        WITH t, cat,
             postSignals,
             commentSignals,
             uniqueUsers,
             (postSignals + commentSignals) AS signalCount,
             (postSignals7d + commentSignals7d) AS signals7d,
             (postSignalsPrev7d + commentSignalsPrev7d) AS signalsPrev7d,
             [x IN (postChannels + commentChannels)
                WHERE trim(coalesce(toString(x), '')) <> ''
                | toString(x)] AS channelList
        WHERE signalCount > 0

        WITH t, cat, signalCount, uniqueUsers, signals7d, signalsPrev7d,
             reduce(acc = [], item IN channelList |
                CASE WHEN item IN acc THEN acc ELSE acc + item END) AS uniqueChannels
        ORDER BY signalCount DESC
        LIMIT $limit_topics

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at > datetime() - duration({days: $days})
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
              AND p.text CONTAINS '?'
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
            WHERE c.posted_at > datetime() - duration({days: $days})
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
              AND c.text CONTAINS '?'
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            RETURN
                coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                'comment' AS kind,
                left(trim(c.text), 2600) AS text,
                left(coalesce(p.text, ''), 1200) AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                coalesce(toString(u.telegram_user_id), '') AS userId,
                c.posted_at AS ts
        }

        WITH t, cat, signalCount, uniqueUsers, signals7d, signalsPrev7d, uniqueChannels,
             evidenceId, kind, text, parentText, channel, userId, ts
        ORDER BY ts DESC

        WITH t, cat, signalCount, uniqueUsers, signals7d, signalsPrev7d, uniqueChannels,
             collect({
                id: evidenceId,
                kind: kind,
                text: text,
                parentText: parentText,
                channel: channel,
                userId: userId,
                timestamp: toString(ts)
             })[..$evidence_per_topic] AS evidence,
             max(ts) AS latestTs

        RETURN
            t.name AS topic,
            cat.name AS category,
            signalCount,
            uniqueUsers,
            size(uniqueChannels) AS channelCount,
            signals7d,
            signalsPrev7d,
            toString(latestTs) AS latestAt,
            evidence
        ORDER BY signalCount DESC, latestAt DESC
        """,
        {
            "days": safe_days,
            "limit_topics": safe_limit_topics,
            "evidence_per_topic": safe_evidence,
        },
    )


def get_lifecycle_stages(ctx: DashboardDateContext) -> list[dict]:
    """Topic lifecycle using direct-message signals inside the clean retention window."""
    params = _bounded_window_params(ctx, limit=18)
    if not params["topic_names"]:
        return []
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN ['', 'null', 'unknown', 'none', 'n/a', 'na']
          AND t.name IN $topic_names

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN
                collect({
                    id: coalesce(p.uuid, elementId(p)),
                    day: toString(date(p.posted_at)),
                    ts: p.posted_at
                }) AS postRows
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN
                collect({
                    id: coalesce(c.uuid, elementId(c)),
                    day: toString(date(c.posted_at)),
                    ts: c.posted_at
                }) AS postRows
        }
        WITH t,
             reduce(allRows = [], chunk IN collect(postRows) | allRows + chunk) AS rows
        WHERE size(rows) > 0
        WITH t, rows,
             size(rows) AS totalSignals,
             size([r IN rows WHERE r.ts >= datetime($current_start) AND r.ts < datetime($end) | 1]) AS recentSignals,
             size([
                r IN rows
                WHERE r.ts >= datetime($previous_start)
                  AND r.ts < datetime($previous_end)
                | 1
             ]) AS previousSignals,
             reduce(minTs = null, r IN rows |
                CASE
                    WHEN minTs IS NULL OR r.ts < minTs THEN r.ts
                    ELSE minTs
                END
             ) AS firstSeen,
             reduce(maxTs = null, r IN rows |
                CASE
                    WHEN maxTs IS NULL OR r.ts > maxTs THEN r.ts
                    ELSE maxTs
                END
             ) AS lastSeen
        UNWIND rows AS row
        WITH t, totalSignals, recentSignals, previousSignals, firstSeen, lastSeen,
             row.day AS day, count(*) AS signalsPerDay
        WITH t.name AS topic,
             totalSignals,
             count(day) AS activeDays,
             max(signalsPerDay) AS peakDay,
             firstSeen,
             lastSeen,
             recentSignals,
             previousSignals
        WITH topic, totalSignals, activeDays, peakDay, firstSeen, lastSeen, recentSignals, previousSignals,
             duration.between(coalesce(firstSeen, datetime($end)), datetime($end)).days AS ageDays,
             round(
                 100.0 * (
                     toFloat(recentSignals) / toFloat(CASE WHEN peakDay > 0 THEN peakDay ELSE 1 END)
                 ),
                 1
             ) AS peakRatioPct,
             round(
                 100.0 * (
                     toFloat(activeDays) / toFloat($total_days)
                 ),
                 1
             ) AS persistencePct,
             round(
                 100.0 * (toFloat(recentSignals - previousSignals) / toFloat(previousSignals + 3)),
                 1
             ) AS rollingGrowthPct,
             round(
                 100.0 * (
                     0.5 * CASE WHEN totalSignals >= 40 THEN 1.0 ELSE toFloat(totalSignals) / 40.0 END +
                     0.3 * CASE WHEN activeDays >= 8 THEN 1.0 ELSE toFloat(activeDays) / 8.0 END +
                     0.2 * CASE
                         WHEN abs(toFloat(recentSignals - previousSignals)) >= 12 THEN 1.0
                         ELSE abs(toFloat(recentSignals - previousSignals)) / 12.0
                     END
                 ),
                 1
             ) AS stageConfidence,
             round(toFloat(recentSignals), 1) AS avg2w,
             round(toFloat(totalSignals) / toFloat($total_days), 1) AS avg4w,
             round(toFloat(previousSignals) / toFloat($baseline_days), 1) AS avgPrev4w,
             0.0 AS wowPct,
             0.0 AS accelerationPct
        WITH topic, totalSignals, activeDays, firstSeen, lastSeen, recentSignals, previousSignals,
             ageDays, peakRatioPct, persistencePct, rollingGrowthPct, stageConfidence,
             avg2w, avg4w, avgPrev4w, wowPct, accelerationPct,
             CASE
               WHEN totalSignals < 4 OR recentSignals = 0 THEN 'declining'
               WHEN recentSignals >= previousSignals
                    AND (
                        rollingGrowthPct >= 10
                        OR (recentSignals - previousSignals) >= 3
                        OR (previousSignals = 0 AND recentSignals >= 4)
                    )
                 THEN 'growing'
               ELSE 'declining'
             END AS stage
        RETURN
            topic,
            stage,
            toString(firstSeen) AS firstSeen,
            toString(lastSeen) AS lastSeen,
            toInteger(ageDays) AS ageDays,
            toInteger(round(toFloat(ageDays) / 7.0)) AS ageWeeks,
            toInteger(totalSignals) AS totalSignals12w,
            toInteger(activeDays) AS activeWeeks,
            toInteger(recentSignals) AS weeklyCurrent,
            toInteger(previousSignals) AS weeklyPrev,
            toInteger(recentSignals - previousSignals) AS weeklyDelta,
            round(avg2w, 1) AS avg2w,
            round(avg4w, 1) AS avg4w,
            round(avgPrev4w, 1) AS avgPrev4w,
            wowPct,
            rollingGrowthPct,
            accelerationPct,
            persistencePct,
            peakRatioPct,
            stageConfidence
        ORDER BY
            CASE stage
              WHEN 'growing' THEN 1
              WHEN 'declining' THEN 2
              ELSE 9
            END,
            weeklyCurrent DESC,
            stageConfidence DESC
        LIMIT 24
    """, params)
