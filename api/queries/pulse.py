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
        return rows[:max_rows]
    except Exception:
        return []


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
        CALL (t) {
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start) AND p.posted_at < datetime($end)
            RETURN count(p) AS postMentions
        }
        CALL (t) {
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


def get_community_health(ctx: DashboardDateContext) -> dict:
    """
    Explainable community climate score (0-100) based on:
    - constructive intent share,
    - emotional pressure (negative share inverse),
    - discussion diversity,
    - conversation depth.
    """
    current_rows = _fetch_analysis_rows_cached(start=ctx.start_at, end=ctx.end_at)
    previous_rows = _fetch_analysis_rows_cached(start=ctx.previous_start_at, end=ctx.previous_end_at)

    current_intent = _window_intent_stats(current_rows)
    previous_intent = _window_intent_stats(previous_rows)

    current_volume = _analysis_volume(current_rows)
    previous_volume = _analysis_volume(previous_rows)

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

    return {
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


def get_trending_topics(ctx: DashboardDateContext, limit: int = 10) -> list[dict]:
    """Evidence-backed top topics by mentions in the selected window with same-length comparison."""
    rows = run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed,false) = false
          AND NOT toLower(trim(coalesce(t.name,''))) IN $noise
        CALL (t) {
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($previous_start) AND p.posted_at < datetime($end)
            WITH p
            RETURN
                count(CASE WHEN p.posted_at >= datetime($start) AND p.posted_at < datetime($end) THEN 1 END) AS postCurrent,
                count(CASE WHEN p.posted_at >= datetime($previous_start)
                             AND p.posted_at < datetime($previous_end) THEN 1 END) AS postPrev,
                head([
                    txt IN collect(
                        CASE
                            WHEN p.posted_at >= datetime($start)
                             AND p.posted_at < datetime($end)
                             AND p.text IS NOT NULL
                             AND trim(p.text) <> ''
                            THEN left(trim(p.text), 180)
                            ELSE null
                        END
                    )
                    WHERE txt IS NOT NULL
                ]) AS postSample
        }
        CALL (t) {
            OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($previous_start) AND c.posted_at < datetime($end)
            WITH c
            RETURN
                count(CASE WHEN c.posted_at >= datetime($start) AND c.posted_at < datetime($end) THEN 1 END) AS commentCurrent,
                count(CASE WHEN c.posted_at >= datetime($previous_start)
                             AND c.posted_at < datetime($previous_end) THEN 1 END) AS commentPrev,
                head([
                    txt IN collect(
                        CASE
                            WHEN c.posted_at >= datetime($start)
                             AND c.posted_at < datetime($end)
                             AND c.text IS NOT NULL
                             AND trim(c.text) <> ''
                            THEN left(trim(c.text), 180)
                            ELSE null
                        END
                    )
                    WHERE txt IS NOT NULL
                ]) AS commentSample
        }
        WITH t, cat,
             (postCurrent + commentCurrent) AS mentions,
             (postPrev + commentPrev) AS previousMentions,
             coalesce(postSample, commentSample, '') AS sampleQuote
        WHERE mentions > 0
        RETURN t.name AS topic,
               cat.name AS category,
               mentions,
               mentions AS currentMentions,
               previousMentions,
               (mentions + previousMentions) AS growthSupport,
               CASE
                   WHEN (mentions + previousMentions) >= 8 THEN true
                   ELSE false
               END AS trendReliable,
               CASE
                   WHEN (mentions + previousMentions) < 8 THEN null
                   ELSE round(100.0 * (mentions - previousMentions) / (previousMentions + 3), 1)
               END AS trendPct,
               sampleQuote
        ORDER BY mentions DESC
        LIMIT $limit
        """,
        {
            "noise": list(_NOISY_TOPIC_NAMES),
            "limit": limit,
            "start": ctx.start_at.isoformat(),
            "end": ctx.end_at.isoformat(),
            "previous_start": ctx.previous_start_at.isoformat(),
            "previous_end": ctx.previous_end_at.isoformat(),
        },
    )

    output: list[dict] = []
    for row in rows:
        output.append(
            {
                "name": row.get("topic"),
                "category": row.get("category") or "General",
                "mentions": int(row.get("mentions") or 0),
                "trendPct": _to_float(row.get("trendPct"), 0.0),
                "currentMentions": int(row.get("currentMentions") or 0),
                "previousMentions": int(row.get("previousMentions") or 0),
                "growthSupport": int(row.get("growthSupport") or 0),
                "trendReliable": bool(row.get("trendReliable")),
                "sampleQuote": str(row.get("sampleQuote") or "").strip(),
            }
        )
    return output


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
    rows = _fetch_analysis_rows_cached(start=ctx.start_at, end=ctx.end_at)

    if not rows:
        return _neo4j_brief_fallback(ctx)

    volume = _analysis_volume(rows)
    intent = _window_intent_stats(rows)
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
