"""
pulse.py - Tier 1: Community Pulse (consumer-first explainable snapshot)

Provides:
- communityHealth: explainable community climate score
- trendingTopics: evidence-backed top discussed topics
- communityBrief: analysis-volume and intent snapshot
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import threading
import time
from typing import Any

from api.db import run_query, run_single
from api.dashboard_dates import DashboardDateContext
from buffer.supabase_writer import SupabaseWriter
from loguru import logger
from utils.taxonomy import TAXONOMY_DOMAINS, iter_topics


_SUPABASE_WRITER: SupabaseWriter | None = None

_NOISY_TOPIC_NAMES = {
    "",
    "null",
    "unknown",
    "none",
    "n/a",
    "na",
}

_POSITIVE_INTENT_HINTS = (
    "inform",
    "information",
    "support",
    "help",
    "job",
    "question",
    "clarification",
    "analysis",
    "analyze",
    "observation",
    "report",
    "discuss",
    "gratitude",
    "solution",
)

_NEGATIVE_INTENT_HINTS = (
    "vent",
    "critique",
    "critic",
    "complaint",
    "condemn",
    "sarcasm",
    "protest",
    "hostile",
    "mock",
    "conflict",
)

_CANONICAL_TOPIC_NAMES = tuple(iter_topics())
_CANONICAL_TOPIC_SET = set(_CANONICAL_TOPIC_NAMES)
_TOPIC_WIDGET_CACHE_TTL_SECONDS = 180.0
_TOPIC_WIDGET_CACHE: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}
_TOPIC_WIDGET_CACHE_LOCK = threading.Lock()
_TOPIC_WIDGET_EVIDENCE_LIMIT = 3
_TRENDING_MIN_MENTIONS = 3
_TRENDING_MIN_EVIDENCE = 2
_TRENDING_MIN_CHANNELS = 2
_TRENDING_MIN_USERS = 2
_TRENDING_NEW_MIN_USERS = 3
_TRENDING_NEW_MAX_PREVIOUS = 1
_TREND_RELIABLE_MIN_SUPPORT = 6


def _structure_key(value: Any) -> str:
    text = str(value or "").strip().lower().replace("&", " and ")
    if not text:
        return ""
    compact = "".join(ch if ch.isalnum() else " " for ch in text)
    parts = [part for part in compact.split() if part and part != "and"]
    return "".join(parts)


_CATEGORY_LABEL_KEYS = {
    _structure_key(category)
    for categories in TAXONOMY_DOMAINS.values()
    for category in categories.keys()
}
_DOMAIN_LABEL_KEYS = {_structure_key(domain) for domain in TAXONOMY_DOMAINS.keys()}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _supabase() -> SupabaseWriter:
    global _SUPABASE_WRITER
    if _SUPABASE_WRITER is None:
        _SUPABASE_WRITER = SupabaseWriter()
    return _SUPABASE_WRITER


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


_row_cache: dict[tuple[str, str], tuple[float, list[dict]]] = {}
_row_cache_lock = threading.Lock()
_summary_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
_summary_cache_lock = threading.Lock()
_health_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_health_cache_lock = threading.Lock()
_health_inflight: dict[str, threading.Event] = {}


def _fetch_analysis_rows_cached(
    *, start: datetime, end: datetime, max_rows: int = 12000
) -> list[dict]:
    """Cached wrapper. Returns same list for same window within 30 seconds."""
    key = (start.isoformat(), end.isoformat())
    now = time.monotonic()
    with _row_cache_lock:
        stale_keys = [cache_key for cache_key, (ts, _rows) in _row_cache.items() if (now - ts) >= 30.0]
        for stale_key in stale_keys:
            _row_cache.pop(stale_key, None)
        entry = _row_cache.get(key)
        if entry and (now - entry[0]) < 30.0:
            return entry[1]
    rows = _fetch_analysis_rows(start=start, end=end, max_rows=max_rows)
    with _row_cache_lock:
        _row_cache[key] = (now, rows)
    return rows


def _fetch_analysis_rows(*, start: datetime, end: datetime, max_rows: int = 12000) -> list[dict]:
    """Fetch recent analysis rows from Supabase in a bounded paginated scan."""
    started_at = time.perf_counter()
    try:
        writer = _supabase()
        page_size = 1000
        rows: list[dict] = []
        offset = 0
        while len(rows) < max_rows:
            upper = offset + page_size - 1
            resp = (
                writer.client.table("ai_analysis")
                .select(
                    "content_type,content_id,channel_id,telegram_user_id,"
                    "primary_intent,sentiment_score,created_at"
                )
                .gte("created_at", start.isoformat())
                .lt("created_at", end.isoformat())
                .order("created_at", desc=False)
                .range(offset, upper)
                .execute()
            )
            chunk = resp.data or []
            if not chunk:
                break
            rows.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
        bounded_rows = rows[:max_rows]
        logger.info(
            "Pulse ai_analysis fetch | start={} end={} rows={} pages={} max_rows={} elapsed_ms={}",
            start.isoformat(),
            end.isoformat(),
            len(bounded_rows),
            max(1, (len(rows) + page_size - 1) // page_size) if rows else 0,
            max_rows,
            round((time.perf_counter() - started_at) * 1000, 2),
        )
        return bounded_rows
    except Exception as exc:
        logger.warning(
            "Pulse ai_analysis fetch failed | start={} end={} max_rows={} elapsed_ms={} error={}",
            start.isoformat(),
            end.isoformat(),
            max_rows,
            round((time.perf_counter() - started_at) * 1000, 2),
            exc,
        )
        return []


def _analysis_summary_from_rows(rows: list[dict]) -> dict[str, Any]:
    intent = _window_intent_stats(rows)
    volume = _analysis_volume(rows)
    return {
        "analysis_units": int(intent["total"]),
        "positive": int(intent["positive"]),
        "negative": int(intent["negative"]),
        "neutral": int(intent["neutral"]),
        "unique_users": int(volume["unique_users"]),
        "posts_analyzed": int(volume["posts_analyzed"]),
        "comment_scopes_analyzed": int(volume["comment_scopes_analyzed"]),
    }


def _fetch_analysis_summary_cached(*, start: datetime, end: datetime) -> dict[str, Any]:
    key = (start.isoformat(), end.isoformat())
    now = time.monotonic()
    with _summary_cache_lock:
        stale_keys = [cache_key for cache_key, (ts, _summary) in _summary_cache.items() if (now - ts) >= 30.0]
        for stale_key in stale_keys:
            _summary_cache.pop(stale_key, None)
        entry = _summary_cache.get(key)
        if entry and (now - entry[0]) < 30.0:
            return entry[1]
    summary = _fetch_analysis_summary(start=start, end=end)
    with _summary_cache_lock:
        _summary_cache[key] = (now, summary)
    return summary


def _fetch_analysis_summary(*, start: datetime, end: datetime) -> dict[str, Any]:
    started_at = time.perf_counter()
    try:
        writer = _supabase()
        response = writer.client.rpc(
            "dashboard_ai_analysis_window_summary",
            {"p_start": start.isoformat(), "p_end": end.isoformat()},
        ).execute()
        row = (response.data or [{}])[0]
        summary = {
            "analysis_units": int(row.get("analysis_units") or 0),
            "positive": int(row.get("positive_rows") or 0),
            "negative": int(row.get("negative_rows") or 0),
            "neutral": int(row.get("neutral_rows") or 0),
            "unique_users": int(row.get("unique_users") or 0),
            "posts_analyzed": int(row.get("posts_analyzed") or 0),
            "comment_scopes_analyzed": int(row.get("comment_scopes_analyzed") or 0),
        }
        logger.info(
            "Pulse ai_analysis summary rpc | start={} end={} analysis_units={} elapsed_ms={}",
            start.isoformat(),
            end.isoformat(),
            summary["analysis_units"],
            round((time.perf_counter() - started_at) * 1000, 2),
        )
        return summary
    except Exception as exc:
        logger.warning(
            "Pulse ai_analysis summary rpc failed; falling back to row scan | start={} end={} elapsed_ms={} error={}",
            start.isoformat(),
            end.isoformat(),
            round((time.perf_counter() - started_at) * 1000, 2),
            exc,
        )
        return _analysis_summary_from_rows(_fetch_analysis_rows(start=start, end=end))


def _summary_to_intent_stats(summary: dict[str, Any]) -> dict[str, Any]:
    total = int(summary.get("analysis_units") or 0)
    positive = int(summary.get("positive") or 0)
    negative = int(summary.get("negative") or 0)
    neutral = int(summary.get("neutral") or 0)
    if total <= 0:
        return {
            "total": 0,
            "positive": 0,
            "negative": 0,
            "neutral": 0,
            "positive_share": 0.0,
            "negative_share": 0.0,
            "neutral_share": 0.0,
            "avg_sentiment": 0.0,
        }
    return {
        "total": total,
        "positive": positive,
        "negative": negative,
        "neutral": neutral,
        "positive_share": positive / total,
        "negative_share": negative / total,
        "neutral_share": neutral / total,
        "avg_sentiment": 0.0,
    }


def _summary_to_volume(summary: dict[str, Any]) -> dict[str, int]:
    return {
        "posts_analyzed": int(summary.get("posts_analyzed") or 0),
        "comment_scopes_analyzed": int(summary.get("comment_scopes_analyzed") or 0),
        "analysis_units": int(summary.get("analysis_units") or 0),
        "unique_users": int(summary.get("unique_users") or 0),
    }


def _intent_bucket(intent: str | None, sentiment_score: float) -> str:
    score = _to_float(sentiment_score, 0.0)
    if score >= 0.2:
        return "positive"
    if score <= -0.2:
        return "negative"

    text = (intent or "").strip().lower()
    if any(token in text for token in _NEGATIVE_INTENT_HINTS):
        return "negative"
    if any(token in text for token in _POSITIVE_INTENT_HINTS):
        return "positive"
    return "neutral"


def _window_intent_stats(rows: list[dict]) -> dict:
    total = len(rows)
    positive = 0
    negative = 0
    neutral = 0
    sentiment_sum = 0.0

    for row in rows:
        score = _to_float(row.get("sentiment_score"), 0.0)
        sentiment_sum += score
        bucket = _intent_bucket(str(row.get("primary_intent") or ""), score)
        if bucket == "positive":
            positive += 1
        elif bucket == "negative":
            negative += 1
        else:
            neutral += 1

    if total <= 0:
        return {
            "total": 0,
            "positive": 0,
            "negative": 0,
            "neutral": 0,
            "positive_share": 0.0,
            "negative_share": 0.0,
            "neutral_share": 0.0,
            "avg_sentiment": 0.0,
        }

    return {
        "total": total,
        "positive": positive,
        "negative": negative,
        "neutral": neutral,
        "positive_share": positive / total,
        "negative_share": negative / total,
        "neutral_share": neutral / total,
        "avg_sentiment": sentiment_sum / total,
    }


def _analysis_volume(rows: list[dict]) -> dict:
    post_ids: set[str] = set()
    post_fallback = 0
    scope_ids: set[tuple[str, str, str]] = set()
    scope_fallback = 0
    unique_users: set[str] = set()

    for row in rows:
        user_id = str(row.get("telegram_user_id") or "").strip()
        if user_id:
            unique_users.add(user_id)

        content_type = str(row.get("content_type") or "").strip().lower()
        content_id = str(row.get("content_id") or "").strip()
        channel_id = str(row.get("channel_id") or "").strip()

        if content_type == "post":
            if content_id:
                post_ids.add(content_id)
            else:
                post_fallback += 1
            continue

        if content_type == "batch":
            user_key = user_id or "none"
            channel_key = channel_id or "none"
            content_key = content_id or "none"
            if content_id or channel_id or user_id:
                scope_ids.add((user_key, channel_key, content_key))
            else:
                scope_fallback += 1

    posts_analyzed = len(post_ids) + post_fallback
    comment_scopes_analyzed = len(scope_ids) + scope_fallback

    return {
        "posts_analyzed": posts_analyzed,
        "comment_scopes_analyzed": comment_scopes_analyzed,
        "analysis_units": posts_analyzed + comment_scopes_analyzed,
        "unique_users": len(unique_users),
    }


def _topic_diversity_score(*, start: datetime, end: datetime) -> dict:
    rows = run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
        WHERE coalesce(t.proposed,false) = false
          AND NOT toLower(trim(coalesce(t.name,''))) IN $noise
        CALL {
            WITH t
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start) AND p.posted_at < datetime($end)
            RETURN count(p) AS postMentions
        }
        CALL {
            WITH t
            OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start) AND c.posted_at < datetime($end)
            RETURN count(c) AS commentMentions
        }
        WITH t, (postMentions + commentMentions) AS mentions
        WHERE mentions > 0
        RETURN t.name AS topic, mentions
        ORDER BY mentions DESC
        LIMIT 50
        """,
        {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "noise": list(_NOISY_TOPIC_NAMES),
        },
    )

    counts = [int(r.get("mentions") or 0) for r in rows if int(r.get("mentions") or 0) > 0]
    if not counts:
        return {
            "score": 0,
            "dominant_topic": None,
            "dominant_share": 0.0,
            "topic_count": 0,
        }

    total = sum(counts)
    dominant = max(counts)
    dominant_share = dominant / max(total, 1)
    score = int(round((1.0 - dominant_share) * 100.0))

    dominant_topic = None
    if rows:
        dominant_topic = rows[0].get("topic")

    return {
        "score": int(_clamp(score, 0, 100)),
        "dominant_topic": dominant_topic,
        "dominant_share": dominant_share,
        "topic_count": len(counts),
    }


def _conversation_depth_score(*, posts_analyzed: int, comment_scopes_analyzed: int) -> int:
    if posts_analyzed <= 0 and comment_scopes_analyzed <= 0:
        return 0
    # Target: ~1.5 analyzed comment scopes per analyzed post is considered healthy depth.
    ratio = comment_scopes_analyzed / max(posts_analyzed, 1)
    score = int(round(_clamp((ratio / 1.5) * 100.0, 0.0, 100.0)))
    return score


def _confidence_label(analysis_units: int) -> str:
    if analysis_units >= 200:
        return "high"
    if analysis_units >= 60:
        return "medium"
    return "low"


def _history_points(current_score: int, previous_score: int, label_suffix: str) -> list[dict]:
    points = []
    for idx in range(7):
        ratio = idx / 6 if 6 else 1
        score = int(round(previous_score + (current_score - previous_score) * ratio))
        label = "Now" if idx == 6 else f"{6 - idx}{label_suffix}"
        points.append({"time": label, "score": int(_clamp(score, 0, 100))})
    return points


def _trim_widget_text(value: Any, limit: int = 240) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _is_widget_topic_name_allowed(name: Any) -> bool:
    topic_name = str(name or "").strip()
    if not topic_name:
        return False
    if topic_name not in _CANONICAL_TOPIC_SET:
        return False
    lowered = topic_name.lower()
    if lowered in _NOISY_TOPIC_NAMES:
        return False
    key = _structure_key(topic_name)
    if key in _CATEGORY_LABEL_KEYS or key in _DOMAIN_LABEL_KEYS:
        return False
    return True


def _compute_trend_pct(current_mentions: int, previous_mentions: int) -> float:
    support = max(0, current_mentions) + max(0, previous_mentions)
    if support < _TREND_RELIABLE_MIN_SUPPORT:
        return 0.0
    return round(100.0 * (current_mentions - previous_mentions) / (previous_mentions + 3), 1)


def _quality_tier(
    *,
    mentions: int,
    evidence_count: int,
    distinct_users: int,
    distinct_channels: int,
) -> str:
    if mentions >= 8 and evidence_count >= 3 and distinct_users >= 3 and distinct_channels >= 2:
        return "high"
    if mentions >= 4 and evidence_count >= 2 and distinct_channels >= 2:
        return "medium"
    return "low"


def _emergence_score(row: dict[str, Any]) -> float:
    current_mentions = int(row.get("currentMentions") or 0)
    previous_mentions = int(row.get("previousMentions") or 0)
    delta_mentions = current_mentions - previous_mentions
    distinct_users = int(row.get("distinctUsers") or 0)
    distinct_channels = int(row.get("distinctChannels") or 0)
    evidence_count = int(row.get("evidenceCount") or 0)
    novelty_bonus = 4.0 if previous_mentions <= 0 else (2.0 if previous_mentions <= 1 else 0.0)
    return round(
        (current_mentions * 4.0)
        + (max(delta_mentions, 0) * 6.0)
        + (distinct_users * 3.5)
        + (distinct_channels * 2.5)
        + (evidence_count * 1.5)
        + novelty_bonus,
        2,
    )


def _topic_widget_cache_key(ctx: DashboardDateContext, evidence_limit: int) -> tuple[str, int]:
    return (ctx.cache_key, max(1, int(evidence_limit)))


def _query_topic_widget_rows(
    ctx: DashboardDateContext,
    *,
    evidence_limit: int = _TOPIC_WIDGET_EVIDENCE_LIMIT,
) -> list[dict[str, Any]]:
    rows = run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND t.name IN $canonical_topics
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise

        CALL {
            WITH t
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                  AND p.text IS NOT NULL
                  AND trim(p.text) <> ''
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN
                    coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                    'post' AS kind,
                    left(trim(p.text), 320) AS text,
                    coalesce(ch.title, ch.username, 'unknown') AS channel,
                    '' AS userId,
                    toString(p.posted_at) AS postedAt,
                    p.posted_at AS ts
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                  AND c.text IS NOT NULL
                  AND trim(c.text) <> ''
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN
                    coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                    'comment' AS kind,
                    left(trim(c.text), 320) AS text,
                    coalesce(ch.title, ch.username, 'unknown') AS channel,
                    coalesce(toString(u.telegram_user_id), '') AS userId,
                    toString(c.posted_at) AS postedAt,
                    c.posted_at AS ts
            }
            WITH evidenceId, kind, text, channel, userId, postedAt, ts
            WHERE text <> ''
            ORDER BY ts DESC, evidenceId DESC
            RETURN
                collect({
                    id: evidenceId,
                    kind: kind,
                    text: text,
                    channel: channel,
                    userId: userId,
                    postedAt: postedAt
                })[..$evidence_limit] AS evidence,
                count(DISTINCT evidenceId) AS currentMentions,
                count(DISTINCT CASE WHEN kind = 'post' THEN evidenceId END) AS distinctPosts,
                count(DISTINCT CASE WHEN kind = 'comment' THEN evidenceId END) AS distinctComments,
                count(DISTINCT CASE WHEN trim(userId) <> '' THEN userId END) AS distinctUsers,
                count(DISTINCT CASE WHEN trim(channel) <> '' THEN channel END) AS distinctChannels,
                max(ts) AS latestTs
        }

        CALL {
            WITH t
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($previous_start)
                  AND p.posted_at < datetime($previous_end)
                RETURN count(DISTINCT coalesce(p.uuid, 'post:' + elementId(p))) AS hitCount
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($previous_start)
                  AND c.posted_at < datetime($previous_end)
                RETURN count(DISTINCT coalesce(c.uuid, 'comment:' + elementId(c))) AS hitCount
            }
            RETURN sum(hitCount) AS previousMentions
        }

        WITH t, cat, evidence, currentMentions, distinctPosts, distinctComments, distinctUsers, distinctChannels, latestTs, previousMentions
        WHERE currentMentions > 0
          AND size(evidence) > 0
        RETURN
            t.name AS topic,
            cat.name AS category,
            currentMentions,
            coalesce(previousMentions, 0) AS previousMentions,
            distinctPosts,
            distinctComments,
            distinctUsers,
            distinctChannels,
            evidence,
            toString(latestTs) AS latestAt
        ORDER BY currentMentions DESC, distinctChannels DESC, latestAt DESC
        """,
        {
            "canonical_topics": list(_CANONICAL_TOPIC_NAMES),
            "noise": list(_NOISY_TOPIC_NAMES),
            "start": ctx.start_at.isoformat(),
            "end": ctx.end_at.isoformat(),
            "previous_start": ctx.previous_start_at.isoformat(),
            "previous_end": ctx.previous_end_at.isoformat(),
            "evidence_limit": max(1, min(int(evidence_limit), 6)),
        },
    )
    output: list[dict[str, Any]] = []
    for row in rows:
        topic_name = str(row.get("topic") or "").strip()
        if not _is_widget_topic_name_allowed(topic_name):
            continue

        evidence_rows = []
        for evidence in row.get("evidence") or []:
            if not isinstance(evidence, dict):
                continue
            text = _trim_widget_text(evidence.get("text"), 240)
            if not text:
                continue
            evidence_rows.append(
                {
                    "id": str(evidence.get("id") or "").strip(),
                    "kind": str(evidence.get("kind") or "message").strip() or "message",
                    "text": text,
                    "channel": str(evidence.get("channel") or "unknown").strip() or "unknown",
                    "userId": str(evidence.get("userId") or "").strip(),
                    "postedAt": str(evidence.get("postedAt") or "").strip(),
                }
            )

        if not evidence_rows:
            continue

        current_mentions = int(row.get("currentMentions") or 0)
        previous_mentions = int(row.get("previousMentions") or 0)
        growth_support = current_mentions + previous_mentions
        sample_evidence_id = evidence_rows[0]["id"] if evidence_rows else ""
        output.append(
            {
                "name": topic_name,
                "category": str(row.get("category") or "General").strip() or "General",
                "mentions": current_mentions,
                "currentMentions": current_mentions,
                "previousMentions": previous_mentions,
                "deltaMentions": current_mentions - previous_mentions,
                "growthSupport": growth_support,
                "trendReliable": (
                    growth_support >= _TREND_RELIABLE_MIN_SUPPORT
                    and len(evidence_rows) >= _TRENDING_MIN_EVIDENCE
                    and int(row.get("distinctChannels") or 0) >= _TRENDING_MIN_CHANNELS
                ),
                "trendPct": _compute_trend_pct(current_mentions, previous_mentions),
                "sampleEvidenceId": sample_evidence_id,
                "sampleQuote": evidence_rows[0]["text"],
                "evidence": evidence_rows,
                "evidenceCount": len(evidence_rows),
                "distinctPosts": int(row.get("distinctPosts") or 0),
                "distinctComments": int(row.get("distinctComments") or 0),
                "distinctUsers": int(row.get("distinctUsers") or 0),
                "distinctChannels": int(row.get("distinctChannels") or 0),
                "qualityTier": _quality_tier(
                    mentions=current_mentions,
                    evidence_count=len(evidence_rows),
                    distinct_users=int(row.get("distinctUsers") or 0),
                    distinct_channels=int(row.get("distinctChannels") or 0),
                ),
                "latestAt": str(row.get("latestAt") or "").strip(),
                "sourceTopic": topic_name,
            }
        )
    return output


def _build_topic_widget_snapshot(
    ctx: DashboardDateContext,
    *,
    evidence_limit: int = _TOPIC_WIDGET_EVIDENCE_LIMIT,
) -> dict[str, Any]:
    cache_key = _topic_widget_cache_key(ctx, evidence_limit)
    now = time.monotonic()
    with _TOPIC_WIDGET_CACHE_LOCK:
        cached = _TOPIC_WIDGET_CACHE.get(cache_key)
        if cached and (now - cached[0]) < _TOPIC_WIDGET_CACHE_TTL_SECONDS:
            return cached[1]

    rows = _query_topic_widget_rows(ctx, evidence_limit=evidence_limit)
    diagnostics = {
        "scannedCanonicalTopics": len(rows),
        "excludedCounts": {
            "structure_label": 0,
            "proposed_only": 0,
            "low_evidence": 0,
            "low_breadth": 0,
            "not_new": 0,
        },
    }
    trending_rows: list[dict[str, Any]] = []
    trending_new_rows: list[dict[str, Any]] = []

    for row in rows:
        evidence_count = int(row.get("evidenceCount") or 0)
        distinct_channels = int(row.get("distinctChannels") or 0)
        distinct_users = int(row.get("distinctUsers") or 0)
        current_mentions = int(row.get("currentMentions") or 0)
        previous_mentions = int(row.get("previousMentions") or 0)

        trending_reasons: list[str] = []
        if current_mentions < _TRENDING_MIN_MENTIONS or evidence_count < _TRENDING_MIN_EVIDENCE:
            trending_reasons.append("low_evidence")
        if distinct_channels < _TRENDING_MIN_CHANNELS or distinct_users < _TRENDING_MIN_USERS:
            trending_reasons.append("low_breadth")
        if not trending_reasons:
            trending_rows.append(row)
        else:
            for reason in set(trending_reasons):
                diagnostics["excludedCounts"][reason] += 1

        trending_new_reasons: list[str] = []
        if current_mentions < _TRENDING_MIN_MENTIONS or evidence_count < _TRENDING_MIN_EVIDENCE:
            trending_new_reasons.append("low_evidence")
        if distinct_channels < _TRENDING_MIN_CHANNELS or distinct_users < _TRENDING_NEW_MIN_USERS:
            trending_new_reasons.append("low_breadth")
        if previous_mentions > _TRENDING_NEW_MAX_PREVIOUS:
            trending_new_reasons.append("not_new")
        if not trending_new_reasons:
            enriched = dict(row)
            enriched["trendScore"] = _emergence_score(row)
            trending_new_rows.append(enriched)
        else:
            for reason in set(trending_new_reasons):
                diagnostics["excludedCounts"][reason] += 1

    trending_rows.sort(
        key=lambda row: (
            int(bool(row.get("trendReliable"))),
            int(row.get("currentMentions") or 0),
            int(row.get("distinctChannels") or 0),
            int(row.get("distinctUsers") or 0),
            int(row.get("evidenceCount") or 0),
            str(row.get("latestAt") or ""),
        ),
        reverse=True,
    )
    trending_new_rows.sort(
        key=lambda row: (
            int(bool(row.get("trendReliable"))),
            float(row.get("trendScore") or 0.0),
            int(row.get("deltaMentions") or 0),
            int(row.get("distinctUsers") or 0),
            int(row.get("distinctChannels") or 0),
            int(row.get("evidenceCount") or 0),
            str(row.get("latestAt") or ""),
        ),
        reverse=True,
    )

    snapshot = {
        "generatedAt": _utc_now().isoformat(),
        "trendingTopics": trending_rows,
        "trendingNewTopics": trending_new_rows,
        "diagnostics": {
            **diagnostics,
            "eligibleTrendingTopics": len(trending_rows),
            "eligibleTrendingNewTopics": len(trending_new_rows),
            "overlapTopics": len(
                {
                    str(item.get("sourceTopic") or item.get("name") or "").strip()
                    for item in trending_rows
                }.intersection(
                    {
                        str(item.get("sourceTopic") or item.get("name") or "").strip()
                        for item in trending_new_rows
                    }
                )
            ),
        },
    }

    with _TOPIC_WIDGET_CACHE_LOCK:
        _TOPIC_WIDGET_CACHE[cache_key] = (now, snapshot)
    return snapshot


def get_community_health(ctx: DashboardDateContext) -> dict:
    """
    Explainable community climate score (0-100) based on:
    - constructive intent share,
    - emotional pressure (negative share inverse),
    - discussion diversity,
    - conversation depth.
    """
    now = time.monotonic()
    wait_event: threading.Event | None = None
    should_build = False
    with _health_cache_lock:
        stale_keys = [cache_key for cache_key, (ts, _payload) in _health_cache.items() if (now - ts) >= 30.0]
        for stale_key in stale_keys:
            _health_cache.pop(stale_key, None)
        cached = _health_cache.get(ctx.cache_key)
        if cached is not None:
            return cached[1]
        wait_event = _health_inflight.get(ctx.cache_key)
        if wait_event is None:
            wait_event = threading.Event()
            _health_inflight[ctx.cache_key] = wait_event
            should_build = True
    if not should_build:
        assert wait_event is not None
        wait_event.wait(timeout=30.0)
        with _health_cache_lock:
            cached = _health_cache.get(ctx.cache_key)
            if cached is not None:
                return cached[1]
    try:
        current_summary = _fetch_analysis_summary_cached(start=ctx.start_at, end=ctx.end_at)
        previous_summary = _fetch_analysis_summary_cached(start=ctx.previous_start_at, end=ctx.previous_end_at)

        current_intent = _summary_to_intent_stats(current_summary)
        previous_intent = _summary_to_intent_stats(previous_summary)

        current_volume = _summary_to_volume(current_summary)
        previous_volume = _summary_to_volume(previous_summary)

        current_diversity = _topic_diversity_score(start=ctx.start_at, end=ctx.end_at)
        previous_diversity = _topic_diversity_score(start=ctx.previous_start_at, end=ctx.previous_end_at)

        current_constructive = int(round(current_intent["positive_share"] * 100.0))
        previous_constructive = int(round(previous_intent["positive_share"] * 100.0))

        current_pressure_inverse = int(round((1.0 - current_intent["negative_share"]) * 100.0))
        previous_pressure_inverse = int(round((1.0 - previous_intent["negative_share"]) * 100.0))

        current_depth = _conversation_depth_score(
            posts_analyzed=current_volume["posts_analyzed"],
            comment_scopes_analyzed=current_volume["comment_scopes_analyzed"],
        )
        previous_depth = _conversation_depth_score(
            posts_analyzed=previous_volume["posts_analyzed"],
            comment_scopes_analyzed=previous_volume["comment_scopes_analyzed"],
        )

        current_score = int(round(
            0.35 * current_constructive
            + 0.30 * current_pressure_inverse
            + 0.20 * current_diversity["score"]
            + 0.15 * current_depth
        ))
        previous_score = int(round(
            0.35 * previous_constructive
            + 0.30 * previous_pressure_inverse
            + 0.20 * previous_diversity["score"]
            + 0.15 * previous_depth
        ))

        current_score = int(_clamp(current_score, 0, 100))
        previous_score = int(_clamp(previous_score, 0, 100))
        delta = current_score - previous_score

        components = [
            {
                "label": "Constructive Intent",
                "value": current_constructive,
                "trend": current_constructive - previous_constructive,
                "desc": "Share of constructive intent in analyzed messages",
            },
            {
                "label": "Emotional Stability",
                "value": current_pressure_inverse,
                "trend": current_pressure_inverse - previous_pressure_inverse,
                "desc": "Inverse of negative-intent pressure",
            },
            {
                "label": "Discussion Diversity",
                "value": current_diversity["score"],
                "trend": current_diversity["score"] - previous_diversity["score"],
                "desc": "How concentrated discussions are around few topics",
            },
            {
                "label": "Conversation Depth",
                "value": current_depth,
                "trend": current_depth - previous_depth,
                "desc": "Comment-scope depth per analyzed post",
            },
        ]

        trend = "flat"
        if delta >= 2:
            trend = "up"
        elif delta <= -2:
            trend = "down"

        payload = {
            "score": current_score,
            "trend": trend,
            "previousScore": previous_score,
            "windowHours": ctx.days * 24,
            "components": components,
            "history": _history_points(current_score, previous_score, "d ago" if ctx.days > 1 else "h ago"),
            "confidence": {
                "label": _confidence_label(current_volume["analysis_units"]),
                "analysisUnits": current_volume["analysis_units"],
            },
            "dominantTopic": current_diversity["dominant_topic"],
            "dominantTopicSharePct": round(current_diversity["dominant_share"] * 100.0, 1),
            "windowDays": ctx.days,
        }
        with _health_cache_lock:
            _health_cache[ctx.cache_key] = (time.monotonic(), payload)
        return payload
    finally:
        with _health_cache_lock:
            inflight = _health_inflight.pop(ctx.cache_key, None)
        if inflight is not None:
            inflight.set()


def get_trending_topics(ctx: DashboardDateContext, limit: int = 10) -> list[dict]:
    """Evidence-backed top canonical topics for the selected window."""
    snapshot = _build_topic_widget_snapshot(ctx)
    return list(snapshot.get("trendingTopics") or [])[: max(1, int(limit))]


def get_trending_new_topics(ctx: DashboardDateContext, limit: int = 10) -> list[dict]:
    """Evidence-backed newly emerging canonical topics for the selected window."""
    snapshot = _build_topic_widget_snapshot(ctx)
    return list(snapshot.get("trendingNewTopics") or [])[: max(1, int(limit))]


def get_trending_widget_diagnostics(ctx: DashboardDateContext) -> dict[str, Any]:
    """Diagnostics for widget QA, including candidate counts and evidence integrity."""
    snapshot = _build_topic_widget_snapshot(ctx)
    trending_rows = list(snapshot.get("trendingTopics") or [])
    trending_new_rows = list(snapshot.get("trendingNewTopics") or [])

    def _sample_integrity(rows: list[dict]) -> dict[str, int]:
        total = len(rows)
        exact = 0
        missing = 0
        for row in rows:
            sample_id = str(row.get("sampleEvidenceId") or "").strip()
            sample_quote = str(row.get("sampleQuote") or "").strip()
            evidence_rows = row.get("evidence") or []
            if not sample_id or not sample_quote:
                missing += 1
                continue
            matched = next((ev for ev in evidence_rows if str(ev.get("id") or "").strip() == sample_id), None)
            if matched and str(matched.get("text") or "").strip() == sample_quote:
                exact += 1
            else:
                missing += 1
        return {
            "total": total,
            "exactMatches": exact,
            "mismatches": missing,
        }

    return {
        "generatedAt": snapshot.get("generatedAt"),
        "windowDays": ctx.days,
        "diagnostics": snapshot.get("diagnostics") or {},
        "sampleEvidenceIntegrity": {
            "trending": _sample_integrity(trending_rows),
            "trendingNew": _sample_integrity(trending_new_rows),
        },
    }


def _latest_analysis_minutes_ago() -> int:
    try:
        resp = _supabase().client.table("ai_analysis").select("created_at").order("created_at", desc=True).limit(1).execute()
        row = (resp.data or [None])[0]
        dt = _parse_dt((row or {}).get("created_at"))
        if not dt:
            return 0
        minutes = int(max(0.0, (_utc_now() - dt).total_seconds()) // 60)
        return minutes
    except Exception:
        return 0


def _neo4j_brief_fallback(ctx: DashboardDateContext) -> dict:
    stats = run_single(
        """
        MATCH (p:Post)
        WHERE p.posted_at >= datetime($start) AND p.posted_at < datetime($end)
        WITH count(p) AS postsCount
        OPTIONAL MATCH (c:Comment)
        WHERE c.posted_at >= datetime($start) AND c.posted_at < datetime($end)
        WITH postsCount, count(c) AS commentsCount
        OPTIONAL MATCH (u:User)
        WHERE u.last_seen >= datetime($start) AND u.last_seen < datetime($end)
        RETURN postsCount, commentsCount, count(u) AS activeUsersCount
        """,
        {"start": ctx.start_at.isoformat(), "end": ctx.end_at.isoformat()},
    ) or {}
    posts = int(stats.get("postsCount") or 0)
    comments = int(stats.get("commentsCount") or 0)
    return {
        "postsAnalyzed24h": posts,
        "commentScopesAnalyzed24h": comments,
        "positiveIntentPct24h": 0,
        "negativeIntentPct24h": 0,
        "neutralIntentPct24h": 100 if (posts + comments) > 0 else 0,
        "totalAnalyses24h": posts + comments,
        "uniqueUsers24h": int(stats.get("activeUsersCount") or 0),
        "refreshedMinutesAgo": 0,
        "topTopics": [t.get("name") for t in get_trending_topics(ctx, 5) if t.get("name")],
        "topTopicRows": get_trending_topics(ctx, 5),
        # Backward compatibility keys
        "postsLast24h": posts,
        "commentsLast24h": comments,
        "activeUsersLast24h": int(stats.get("activeUsersCount") or 0),
        "windowDays": ctx.days,
    }


def get_community_brief(ctx: DashboardDateContext) -> dict:
    """Community pulse snapshot for non-analyst consumers (simple + evidence-ready)."""
    summary = _fetch_analysis_summary_cached(start=ctx.start_at, end=ctx.end_at)

    if int(summary.get("analysis_units") or 0) <= 0:
        return _neo4j_brief_fallback(ctx)

    volume = _summary_to_volume(summary)
    intent = _summary_to_intent_stats(summary)
    top_topic_rows = get_trending_topics(ctx, 5)
    top_topic_names = [str(item.get("name") or "").strip() for item in top_topic_rows if item.get("name")]

    positive_pct = int(round(intent["positive_share"] * 100.0))
    negative_pct = int(round(intent["negative_share"] * 100.0))
    neutral_pct = int(round(intent["neutral_share"] * 100.0))

    return {
        "postsAnalyzed24h": volume["posts_analyzed"],
        "commentScopesAnalyzed24h": volume["comment_scopes_analyzed"],
        "positiveIntentPct24h": positive_pct,
        "negativeIntentPct24h": negative_pct,
        "neutralIntentPct24h": neutral_pct,
        "totalAnalyses24h": volume["analysis_units"],
        "uniqueUsers24h": volume["unique_users"],
        "refreshedMinutesAgo": _latest_analysis_minutes_ago(),
        "topTopics": top_topic_names,
        "topTopicRows": top_topic_rows,
        # Backward compatibility keys
        "postsLast24h": volume["posts_analyzed"],
        "commentsLast24h": volume["comment_scopes_analyzed"],
        "activeUsersLast24h": volume["unique_users"],
        "windowDays": ctx.days,
    }
