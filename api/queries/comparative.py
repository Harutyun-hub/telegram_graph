"""
comparative.py — Tier 8: Comparative Analytics & Deep Dive

Provides: weeklyShifts, sentimentByTopic, topPosts, contentTypePerformance,
          vitalityIndicators, allTopics, allChannels, allAudience
"""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import os
import threading
import time
from typing import Any, Iterable

from api.dashboard_dates import DashboardDateContext, build_dashboard_date_context
from api.db import run_query, run_single
from api.queries import predictive, pulse
from buffer.supabase_writer import SupabaseWriter
from utils.topic_normalizer import classify_topic, normalize_model_topics
from utils.topic_presentation import topic_group_for_category


_SUPABASE_WRITER: SupabaseWriter | None = None
_SUPABASE_PAGE_SIZE = 500
_SUPABASE_IN_FILTER_CHUNK = 150
_SENTIMENT_CANON = {
    "positive": "Positive",
    "negative": "Negative",
    "neutral": "Neutral",
    "mixed": "Mixed",
    "urgent": "Urgent",
    "sarcastic": "Sarcastic",
}
_NEGATIVE_SENTIMENTS = {"Negative", "Urgent", "Sarcastic"}
_NOISY_TOPIC_KEYS = {"", "null", "unknown", "none", "n/a", "na"}
_TOPICS_PAGE_GROUP_KEYS = {"Living", "Work", "Family", "Finance", "Lifestyle", "Integration", "Admin"}
# The v1 topic detail/evidence path is not valid on the current Neo4j runtime
# used by staging, so keep the working v2 path active until both environments
# share one proven query family again.
USE_TOPIC_QUERY_V2 = True
_GLOBAL_COUNTS_TTL_SECONDS = 300.0
_GLOBAL_COUNTS_CACHE: tuple[float, dict[str, int]] | None = None
_GLOBAL_COUNTS_LOCK = threading.Lock()


def _supabase() -> SupabaseWriter:
    global _SUPABASE_WRITER
    if _SUPABASE_WRITER is None:
        _SUPABASE_WRITER = SupabaseWriter()
    return _SUPABASE_WRITER


def _paginate(query_factory) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    total_count: int | None = None
    while True:
        response = query_factory(offset, offset + _SUPABASE_PAGE_SIZE - 1).execute()
        batch = response.data or []
        if total_count is None:
            raw_count = getattr(response, "count", None)
            total_count = int(raw_count) if raw_count is not None else None
        if not batch:
            break
        rows.extend(batch)
        if total_count is not None and len(rows) >= total_count:
            break
        offset += len(batch)
    return rows


def _chunked(values: Iterable[str], size: int = _SUPABASE_IN_FILTER_CHUNK) -> list[list[str]]:
    chunk: list[str] = []
    output: list[list[str]] = []
    for value in values:
        chunk.append(str(value))
        if len(chunk) >= max(1, int(size)):
            output.append(chunk)
            chunk = []
    if chunk:
        output.append(chunk)
    return output


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return fallback


def _normalize_sentiment_label(value: Any) -> str | None:
    if value is None:
        return None
    key = str(value).strip().lower()
    if not key or key in {"null", "none", "unknown", "n/a"}:
        return None
    return _SENTIMENT_CANON.get(key)


def _bucket_sentiment(label: Any, sentiment_score: Any = None) -> str | None:
    normalized = _normalize_sentiment_label(label)
    if normalized == "Positive":
        return "Positive"
    if normalized in _NEGATIVE_SENTIMENTS:
        return "Negative"
    if normalized == "Neutral":
        return "Neutral"
    if normalized == "Mixed":
        score = _safe_float(sentiment_score, 0.0)
        if score >= 0.2:
            return "Positive"
        if score <= -0.2:
            return "Negative"
        return "Neutral"

    if label is None and sentiment_score is not None:
        score = _safe_float(sentiment_score, 0.0)
        if score >= 0.2:
            return "Positive"
        if score <= -0.2:
            return "Negative"
        return "Neutral"
    return None


def _topic_names_from_payload(raw_topics: Any) -> list[str]:
    if not isinstance(raw_topics, list):
        return []

    normalized = normalize_model_topics(raw_topics)
    if not normalized:
        normalized = normalize_model_topics([str(item) for item in raw_topics if isinstance(item, str) and item.strip()])

    names: list[str] = []
    seen: set[str] = set()
    for item in normalized:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        if name.lower() in _NOISY_TOPIC_KEYS or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def _is_topics_page_topic_allowed(topic_name: str, category: str) -> bool:
    clean_topic = str(topic_name or "").strip()
    clean_category = str(category or "").strip()
    if not clean_topic or not clean_category or clean_category == "General":
        return False
    if classify_topic(clean_topic) is None:
        return False
    return topic_group_for_category(clean_category) in _TOPICS_PAGE_GROUP_KEYS


def _decorate_topics_page_row(row: dict[str, Any]) -> dict[str, Any]:
    decorated = dict(row)
    name = str(decorated.get("name") or "").strip()
    category = str(decorated.get("category") or "").strip()
    mention_count = _safe_int(decorated.get("mentionCount"))
    previous_mentions = _safe_int(decorated.get("prev7Mentions"), _safe_int(decorated.get("previousMentions")))
    sample_evidence = decorated.get("sampleEvidence") if isinstance(decorated.get("sampleEvidence"), dict) else {}
    sample_evidence_id = str(
        decorated.get("sampleEvidenceId")
        or sample_evidence.get("id")
        or ""
    ).strip()
    sample_quote = str(
        decorated.get("sampleQuote")
        or sample_evidence.get("text")
        or ""
    ).strip()
    evidence_count = _safe_int(decorated.get("evidenceCount"))
    distinct_users = _safe_int(decorated.get("distinctUsers"), _safe_int(decorated.get("userCount")))
    distinct_channels = _safe_int(decorated.get("distinctChannels"))
    topic_group = topic_group_for_category(category)

    decorated["sourceTopic"] = name
    decorated["topicGroup"] = topic_group
    decorated["sampleEvidenceId"] = sample_evidence_id
    decorated["sampleQuote"] = sample_quote
    decorated["evidenceCount"] = evidence_count
    decorated["distinctUsers"] = distinct_users
    decorated["distinctChannels"] = distinct_channels
    decorated["currentMentions"] = mention_count
    decorated["previousMentions"] = previous_mentions
    decorated["deltaMentions"] = mention_count - previous_mentions
    decorated["trendReliable"] = bool(
        mention_count > 0
        and evidence_count > 0
        and distinct_users > 0
        and topic_group in _TOPICS_PAGE_GROUP_KEYS
    )
    decorated["userCount"] = distinct_users
    decorated["totalInteractions"] = _safe_int(decorated.get("totalInteractions"), mention_count)
    decorated["last7Mentions"] = mention_count
    decorated["prev7Mentions"] = previous_mentions
    if not isinstance(decorated.get("topChannels"), list):
        decorated["topChannels"] = []
    return decorated


def _is_topics_page_row_allowed(row: dict[str, Any]) -> bool:
    topic_name = str(row.get("name") or "").strip()
    category = str(row.get("category") or "").strip()
    if not _is_topics_page_topic_allowed(topic_name, category):
        return False
    if _safe_int(row.get("mentionCount")) <= 0:
        return False
    if _safe_int(row.get("evidenceCount")) <= 0:
        return False
    if not str(row.get("sampleEvidenceId") or "").strip():
        return False
    if not str(row.get("sampleQuote") or "").strip():
        return False
    return True


def _topics_page_sort_key(row: dict[str, Any]) -> tuple:
    return (
        -_safe_int(row.get("mentionCount")),
        -_safe_int(row.get("distinctUsers")),
        -_safe_int(row.get("evidenceCount")),
        str(row.get("name") or ""),
    )


def _message_topic_map(raw: dict) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for item in raw.get("message_topics") or []:
        if not isinstance(item, dict):
            continue
        comment_id = str(item.get("comment_id") or "").strip()
        if not comment_id or comment_id in result:
            continue
        topic_names = _topic_names_from_payload(item.get("topics") or [])
        if topic_names:
            result[comment_id] = topic_names
    return result


def _message_sentiment_map(raw: dict) -> dict[str, tuple[str, float]]:
    result: dict[str, tuple[str, float]] = {}
    for item in raw.get("message_sentiments") or []:
        if not isinstance(item, dict):
            continue
        comment_id = str(item.get("comment_id") or "").strip()
        if not comment_id or comment_id in result:
            continue
        bucket = _bucket_sentiment(item.get("sentiment"), item.get("sentiment_score"))
        if not bucket:
            continue
        result[comment_id] = (bucket, _safe_float(item.get("sentiment_score"), 0.0))
    return result


def _exclude_thread_anchors(query):
    return query.neq("entry_kind", "thread_anchor")
def _fetch_window_posts(ctx: DashboardDateContext) -> list[dict]:
    return _paginate(
        lambda from_idx, to_idx: _exclude_thread_anchors(
            _supabase().client.table("telegram_posts")
            .select("id, posted_at", count="exact")
            .gte("posted_at", ctx.start_at.isoformat())
            .lt("posted_at", ctx.end_at.isoformat())
        )
        .order("posted_at", desc=False)
        .range(from_idx, to_idx)
    )


def _fetch_window_comments(ctx: DashboardDateContext) -> list[dict]:
    return _paginate(
        lambda from_idx, to_idx: _supabase().client.table("telegram_comments")
        .select("id, post_id, channel_id, telegram_user_id, posted_at", count="exact")
        .gte("posted_at", ctx.start_at.isoformat())
        .lt("posted_at", ctx.end_at.isoformat())
        .not_.is_("telegram_user_id", "null")
        .order("posted_at", desc=False)
        .range(from_idx, to_idx)
    )


def _fetch_post_analyses(post_ids: list[str]) -> dict[str, dict]:
    latest_by_post: dict[str, dict] = {}
    if not post_ids:
        return latest_by_post

    for chunk in _chunked(post_ids):
        rows = _paginate(
            lambda from_idx, to_idx, ids=chunk: _supabase().client.table("ai_analysis")
            .select("content_id, raw_llm_response, created_at", count="exact")
            .eq("content_type", "post")
            .in_("content_id", ids)
            .order("created_at", desc=True)
            .range(from_idx, to_idx)
        )
        for row in rows:
            post_id = str(row.get("content_id") or "").strip()
            if not post_id or post_id in latest_by_post:
                continue
            latest_by_post[post_id] = row
    return latest_by_post


def _fetch_batch_analyses(post_ids: list[str]) -> dict[tuple[str, str, str], dict]:
    latest_by_scope: dict[tuple[str, str, str], dict] = {}
    if not post_ids:
        return latest_by_scope

    for chunk in _chunked(post_ids):
        rows = _paginate(
            lambda from_idx, to_idx, ids=chunk: _supabase().client.table("ai_analysis")
            .select("content_id, channel_id, telegram_user_id, raw_llm_response, created_at", count="exact")
            .eq("content_type", "batch")
            .not_.is_("channel_id", "null")
            .not_.is_("telegram_user_id", "null")
            .in_("content_id", ids)
            .order("created_at", desc=True)
            .range(from_idx, to_idx)
        )
        for row in rows:
            post_id = str(row.get("content_id") or "").strip()
            channel_id = str(row.get("channel_id") or "").strip()
            user_id = str(row.get("telegram_user_id") or "").strip()
            if not post_id or not channel_id or not user_id:
                continue
            key = (post_id, channel_id, user_id)
            if key not in latest_by_scope:
                latest_by_scope[key] = row
    return latest_by_scope


def _build_window_context(current_start, current_end, days: int) -> DashboardDateContext:
    previous_end = current_start
    previous_start = current_start - timedelta(days=days)
    from_date = current_start.date()
    to_date = (current_end - timedelta(days=1)).date()
    return DashboardDateContext(
        from_date=from_date,
        to_date=to_date,
        start_at=current_start,
        end_at=current_end,
        previous_start_at=previous_start,
        previous_end_at=previous_end,
        days=days,
        cache_key=f"{from_date.isoformat()}:{to_date.isoformat()}",
    )


def _range_params(ctx: DashboardDateContext) -> dict[str, str]:
    return {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
    }


def _default_detail_context() -> DashboardDateContext:
    current_end_date = datetime.now(timezone.utc).date()
    current_start_date = current_end_date - timedelta(days=14)
    return build_dashboard_date_context(current_start_date.isoformat(), current_end_date.isoformat())


def _safe_int(value, fallback: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return fallback


def _safe_str(value: Any, fallback: str = "") -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return fallback
    return str(value)


def _safe_pct(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        return 0
    return int(round((float(numerator) / float(denominator)) * 100.0))


def _global_graph_counts() -> dict[str, int]:
    global _GLOBAL_COUNTS_CACHE
    now = time.monotonic()
    with _GLOBAL_COUNTS_LOCK:
        if _GLOBAL_COUNTS_CACHE is not None and (now - _GLOBAL_COUNTS_CACHE[0]) < _GLOBAL_COUNTS_TTL_SECONDS:
            return dict(_GLOBAL_COUNTS_CACHE[1])
    counts = run_single("""
        CALL {
            MATCH (u:User)
            RETURN count(u) AS totalUsers
        }
        CALL {
            MATCH (t:Topic)
            RETURN count(t) AS totalTopics
        }
        RETURN totalUsers, totalTopics
    """) or {}
    payload = {
        "totalUsers": _safe_int(counts.get("totalUsers")),
        "totalTopics": _safe_int(counts.get("totalTopics")),
    }
    with _GLOBAL_COUNTS_LOCK:
        _GLOBAL_COUNTS_CACHE = (time.monotonic(), payload)
    return dict(payload)


def _weekly_metric_row(
    metric_key: str,
    current: int,
    previous: int,
    *,
    unit: str = "",
    category: str = "general",
    is_inverse: bool = False,
) -> dict:
    return {
        "metricKey": metric_key,
        "current": current,
        "previous": previous,
        "unit": unit,
        "category": category,
        "isInverse": is_inverse,
    }


def get_weekly_shifts(ctx: DashboardDateContext) -> list[dict]:
    """Exact week-over-week metric rows for the comparative widget."""
    stats = run_single("""
        CALL {
            OPTIONAL MATCH (p:Post)
            WHERE p.posted_at >= datetime($previous_start)
              AND p.posted_at < datetime($current_end)
            RETURN
                count(CASE
                    WHEN p.posted_at >= datetime($current_start)
                     AND p.posted_at < datetime($current_end)
                    THEN 1
                END) AS currentPosts,
                count(CASE
                    WHEN p.posted_at >= datetime($previous_start)
                     AND p.posted_at < datetime($previous_end)
                    THEN 1
                END) AS previousPosts,
                count(CASE
                    WHEN p.posted_at >= datetime($current_start)
                     AND p.posted_at < datetime($current_end)
                     AND p.text IS NOT NULL
                     AND trim(p.text) <> ''
                     AND p.text CONTAINS '?'
                    THEN 1
                END) AS currentPostQuestions,
                count(CASE
                    WHEN p.posted_at >= datetime($previous_start)
                     AND p.posted_at < datetime($previous_end)
                     AND p.text IS NOT NULL
                     AND trim(p.text) <> ''
                     AND p.text CONTAINS '?'
                    THEN 1
                END) AS previousPostQuestions
        }
        CALL {
            OPTIONAL MATCH (c:Comment)
            WHERE c.posted_at >= datetime($previous_start)
              AND c.posted_at < datetime($current_end)
            RETURN
                count(CASE
                    WHEN c.posted_at >= datetime($current_start)
                     AND c.posted_at < datetime($current_end)
                    THEN 1
                END) AS currentComments,
                count(CASE
                    WHEN c.posted_at >= datetime($previous_start)
                     AND c.posted_at < datetime($previous_end)
                    THEN 1
                END) AS previousComments,
                count(CASE
                    WHEN c.posted_at >= datetime($current_start)
                     AND c.posted_at < datetime($current_end)
                     AND c.text IS NOT NULL
                     AND trim(c.text) <> ''
                     AND c.text CONTAINS '?'
                    THEN 1
                END) AS currentCommentQuestions,
                count(CASE
                    WHEN c.posted_at >= datetime($previous_start)
                     AND c.posted_at < datetime($previous_end)
                     AND c.text IS NOT NULL
                     AND trim(c.text) <> ''
                     AND c.text CONTAINS '?'
                    THEN 1
                END) AS previousCommentQuestions
        }
        CALL {
            CALL () {
                MATCH (u:User)-[i:INTERESTED_IN]->(:Topic)
                WHERE i.last_seen >= datetime($previous_start)
                  AND i.last_seen < datetime($current_end)
                RETURN u AS user, i.last_seen AS ts
                UNION ALL
                MATCH (u:User)-[:WROTE]->(c:Comment)
                WHERE c.posted_at >= datetime($previous_start)
                  AND c.posted_at < datetime($current_end)
                RETURN u AS user, c.posted_at AS ts
            }
            RETURN
                count(DISTINCT CASE
                    WHEN ts >= datetime($current_start)
                     AND ts < datetime($current_end)
                    THEN user
                END) AS currentActiveUsers,
                count(DISTINCT CASE
                    WHEN ts >= datetime($previous_start)
                     AND ts < datetime($previous_end)
                    THEN user
                END) AS previousActiveUsers
        }
        CALL {
            MATCH (u:User)-[:WROTE]->(c:Comment)
            WHERE u.telegram_user_id IS NOT NULL
            WITH u, min(c.posted_at) AS firstVoiceAt
            RETURN
                count(DISTINCT CASE
                    WHEN firstVoiceAt >= datetime($current_start)
                     AND firstVoiceAt < datetime($current_end)
                    THEN u
                END) AS currentNewVoices,
                count(DISTINCT CASE
                    WHEN firstVoiceAt >= datetime($previous_start)
                     AND firstVoiceAt < datetime($previous_end)
                    THEN u
                END) AS previousNewVoices
        }
        CALL {
            CALL () {
                MATCH (p:Post)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE p.posted_at >= datetime($previous_start)
                  AND p.posted_at < datetime($current_end)
                RETURN p.posted_at AS ts, toLower(trim(coalesce(s.label, ''))) AS label
                UNION ALL
                MATCH (c:Comment)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE c.posted_at >= datetime($previous_start)
                  AND c.posted_at < datetime($current_end)
                RETURN c.posted_at AS ts, toLower(trim(coalesce(s.label, ''))) AS label
            }
            WITH ts, label
            WHERE label <> ''
            RETURN
                count(CASE
                    WHEN ts >= datetime($current_start)
                     AND ts < datetime($current_end)
                    THEN 1
                END) AS currentSentimentTotal,
                count(CASE
                    WHEN ts >= datetime($current_start)
                     AND ts < datetime($current_end)
                     AND label = 'positive'
                    THEN 1
                END) AS currentPositiveSentiment,
                count(CASE
                    WHEN ts >= datetime($previous_start)
                     AND ts < datetime($previous_end)
                    THEN 1
                END) AS previousSentimentTotal,
                count(CASE
                    WHEN ts >= datetime($previous_start)
                     AND ts < datetime($previous_end)
                     AND label = 'positive'
                    THEN 1
                END) AS previousPositiveSentiment
        }
        RETURN
            currentPosts,
            previousPosts,
            currentComments,
            previousComments,
            currentPostQuestions + currentCommentQuestions AS currentQuestionsAsked,
            previousPostQuestions + previousCommentQuestions AS previousQuestionsAsked,
            currentActiveUsers,
            previousActiveUsers,
            currentNewVoices,
            previousNewVoices,
            currentPositiveSentiment,
            currentSentimentTotal,
            previousPositiveSentiment,
            previousSentimentTotal
    """, {
        "current_start": ctx.start_at.isoformat(),
        "current_end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
    }) or {}

    health = pulse.get_community_health(ctx)
    previous_ctx = _build_window_context(ctx.previous_start_at, ctx.previous_end_at, ctx.days)
    current_churn_count = len(predictive.get_churn_signals(ctx))
    previous_churn_count = len(predictive.get_churn_signals(previous_ctx))

    return [
        _weekly_metric_row(
            "community_health_score",
            _safe_int(health.get("score")),
            _safe_int(health.get("previousScore")),
            unit="/100",
            category="health",
        ),
        _weekly_metric_row(
            "active_members",
            _safe_int(stats.get("currentActiveUsers")),
            _safe_int(stats.get("previousActiveUsers")),
            category="audience",
        ),
        _weekly_metric_row(
            "new_voices",
            _safe_int(stats.get("currentNewVoices")),
            _safe_int(stats.get("previousNewVoices")),
            category="growth",
        ),
        _weekly_metric_row(
            "posts",
            _safe_int(stats.get("currentPosts")),
            _safe_int(stats.get("previousPosts")),
            category="content",
        ),
        _weekly_metric_row(
            "comments",
            _safe_int(stats.get("currentComments")),
            _safe_int(stats.get("previousComments")),
            category="content",
        ),
        _weekly_metric_row(
            "questions_asked",
            _safe_int(stats.get("currentQuestionsAsked")),
            _safe_int(stats.get("previousQuestionsAsked")),
            category="engagement",
        ),
        _weekly_metric_row(
            "positive_sentiment",
            _safe_pct(
                _safe_int(stats.get("currentPositiveSentiment")),
                _safe_int(stats.get("currentSentimentTotal")),
            ),
            _safe_pct(
                _safe_int(stats.get("previousPositiveSentiment")),
                _safe_int(stats.get("previousSentimentTotal")),
            ),
            unit="%",
            category="mood",
        ),
        _weekly_metric_row(
            "churn_signals",
            current_churn_count,
            previous_churn_count,
            category="risk",
            is_inverse=True,
        ),
    ]


def get_sentiment_by_topic_legacy(ctx: DashboardDateContext) -> list[dict]:
    """Legacy Supabase-backed sentiment reconstruction retained for comparison debugging."""
    topic_sentiment_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"Positive": 0, "Neutral": 0, "Negative": 0})

    posts = _fetch_window_posts(ctx)
    post_ids = [str(row.get("id") or "").strip() for row in posts if row.get("id")]
    post_analyses = _fetch_post_analyses(post_ids)
    seen_post_topic_pairs: set[tuple[str, str]] = set()

    for post in posts:
        post_id = str(post.get("id") or "").strip()
        analysis = post_analyses.get(post_id)
        raw = analysis.get("raw_llm_response") if isinstance(analysis, dict) and isinstance(analysis.get("raw_llm_response"), dict) else {}
        if not raw:
            continue

        sentiment_bucket = _bucket_sentiment(raw.get("sentiment"), raw.get("sentiment_score"))
        if not sentiment_bucket:
            continue

        for topic_name in _topic_names_from_payload(raw.get("topics") or []):
            pair = (post_id, topic_name)
            if pair in seen_post_topic_pairs:
                continue
            seen_post_topic_pairs.add(pair)
            topic_sentiment_counts[topic_name][sentiment_bucket] += 1

    comments = _fetch_window_comments(ctx)
    comment_post_ids = list({str(row.get("post_id") or "").strip() for row in comments if row.get("post_id")})
    batch_analyses = _fetch_batch_analyses(comment_post_ids)
    seen_comment_topic_pairs: set[tuple[str, str]] = set()

    for comment in comments:
        comment_id = str(comment.get("id") or "").strip()
        post_id = str(comment.get("post_id") or "").strip()
        channel_id = str(comment.get("channel_id") or "").strip()
        user_id = str(comment.get("telegram_user_id") or "").strip()
        if not comment_id or not post_id or not channel_id or not user_id:
            continue

        analysis = batch_analyses.get((post_id, channel_id, user_id))
        raw = analysis.get("raw_llm_response") if isinstance(analysis, dict) and isinstance(analysis.get("raw_llm_response"), dict) else {}
        if not raw:
            continue

        topic_map = _message_topic_map(raw)
        topic_names = topic_map.get(comment_id) or []
        if not topic_names:
            continue

        sentiment_map = _message_sentiment_map(raw)
        message_sentiment = sentiment_map.get(comment_id)
        sentiment_bucket = (
            message_sentiment[0]
            if message_sentiment
            else _bucket_sentiment(raw.get("sentiment"), raw.get("sentiment_score"))
        )
        if not sentiment_bucket:
            continue

        for topic_name in topic_names:
            pair = (comment_id, topic_name)
            if pair in seen_comment_topic_pairs:
                continue
            seen_comment_topic_pairs.add(pair)
            topic_sentiment_counts[topic_name][sentiment_bucket] += 1

    rows: list[dict] = []
    for topic_name, counts in topic_sentiment_counts.items():
        total = sum(counts.values())
        if total <= 0:
            continue
        for sentiment in ("Positive", "Neutral", "Negative"):
            count = int(counts.get(sentiment, 0) or 0)
            if count <= 0:
                continue
            rows.append({"topic": topic_name, "sentiment": sentiment, "count": count})

    rows.sort(key=lambda row: (-int(row.get("count") or 0), str(row.get("topic") or ""), str(row.get("sentiment") or "")))
    return rows


def get_sentiment_by_topic(ctx: DashboardDateContext) -> list[dict]:
    """Range-filtered topic sentiment counts using graph-native topic and sentiment edges."""
    rows = run_query("""
        CALL () {
            MATCH (p:Post)-[:TAGGED]->(t:Topic)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN trim(coalesce(t.name, '')) AS topic,
                   toLower(trim(coalesce(s.label, ''))) AS label,
                   count(DISTINCT p) AS score
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t:Topic)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN trim(coalesce(t.name, '')) AS topic,
                   toLower(trim(coalesce(s.label, ''))) AS label,
                   count(DISTINCT c) AS score
        }
        WITH topic, label, sum(score) AS score
        WHERE topic <> ''
          AND label <> ''
          AND NOT toLower(topic) IN $noise
        WITH topic,
             CASE
                 WHEN label = 'positive' THEN 'Positive'
                 WHEN label = 'neutral' THEN 'Neutral'
                 WHEN label IN ['negative', 'urgent', 'sarcastic'] THEN 'Negative'
                 ELSE NULL
             END AS sentiment,
             score
        WHERE sentiment IS NOT NULL
        RETURN topic, sentiment, toInteger(sum(score)) AS count
        ORDER BY count DESC, topic ASC, sentiment ASC
    """, {
        **_range_params(ctx),
        "noise": sorted(_NOISY_TOPIC_KEYS),
    })
    return [
        {
            "topic": str(row.get("topic") or "").strip(),
            "sentiment": str(row.get("sentiment") or "").strip(),
            "count": _safe_int(row.get("count")),
        }
        for row in rows
        if str(row.get("topic") or "").strip() and str(row.get("sentiment") or "").strip()
    ]


def compare_sentiment_by_topic(ctx: DashboardDateContext) -> dict[str, object]:
    legacy_rows = get_sentiment_by_topic_legacy(ctx)
    graph_rows = get_sentiment_by_topic(ctx)

    def _to_map(rows: list[dict]) -> dict[tuple[str, str], int]:
        return {
            (str(row.get("topic") or "").strip(), str(row.get("sentiment") or "").strip()): _safe_int(row.get("count"))
            for row in rows
            if str(row.get("topic") or "").strip() and str(row.get("sentiment") or "").strip()
        }

    legacy_map = _to_map(legacy_rows)
    graph_map = _to_map(graph_rows)
    all_keys = sorted(set(legacy_map).union(graph_map))
    mismatches = []
    for key in all_keys:
        legacy_count = legacy_map.get(key, 0)
        graph_count = graph_map.get(key, 0)
        if legacy_count == graph_count:
            continue
        mismatches.append({
            "topic": key[0],
            "sentiment": key[1],
            "legacyCount": legacy_count,
            "graphCount": graph_count,
            "delta": graph_count - legacy_count,
        })

    return {
        "legacyRows": len(legacy_rows),
        "graphRows": len(graph_rows),
        "mismatchCount": len(mismatches),
        "mismatches": mismatches[:100],
    }


def get_top_posts(ctx: DashboardDateContext) -> list[dict]:
    """Highest-engagement posts."""
    return run_query("""
        MATCH (p:Post)-[:IN_CHANNEL]->(ch:Channel)
        WHERE p.posted_at >= datetime($start) AND p.posted_at < datetime($end)
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
    """, {"start": ctx.start_at.isoformat(), "end": ctx.end_at.isoformat()})


def get_content_type_performance(ctx: DashboardDateContext) -> list[dict]:
    """Average engagement by content/media type."""
    return run_query("""
        MATCH (p:Post)
        WHERE p.posted_at >= datetime($start) AND p.posted_at < datetime($end)
        WITH coalesce(p.media_type, 'text') AS mediaType,
             count(p) AS count,
             avg(p.views) AS avgViews,
             avg(p.forwards) AS avgForwards
        RETURN mediaType, count,
               round(avgViews) AS avgViews,
               round(avgForwards) AS avgForwards
        ORDER BY avgViews DESC
    """, {"start": ctx.start_at.isoformat(), "end": ctx.end_at.isoformat()})


def get_vitality_indicators(ctx: DashboardDateContext) -> dict:
    """Composite community health indicators."""
    global_counts = _global_graph_counts()
    stats = run_single("""
        CALL {
            CALL {
                MATCH (u:User)-[:WROTE]->(c:Comment)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                RETURN u AS user
                UNION
                MATCH (u:User)-[i:INTERESTED_IN]->(:Topic)
                WHERE i.last_seen >= datetime($start)
                  AND i.last_seen < datetime($end)
                RETURN u AS user
            }
            RETURN count(DISTINCT user) AS activeUsers
        }
        CALL {
            MATCH (p:Post)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN count(p) AS totalPosts
        }
        CALL {
            MATCH (c:Comment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN count(c) AS totalComments
        }
        RETURN activeUsers, totalPosts, totalComments
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
    }, op_name="comparative.vitality_indicators") or {}
    total_users = global_counts.get("totalUsers", 0)
    active_users = stats.get("activeUsers", 0)
    total_topics = global_counts.get("totalTopics", 0)
    total_posts = stats.get("totalPosts", 0)
    total_comments = stats.get("totalComments", 0)
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

def get_all_topics(page: int = 0, size: int = 50, ctx: DashboardDateContext | None = None) -> list[dict]:
    """Compact topic summaries for the Topics detail page."""
    resolved_ctx = ctx or _default_detail_context()
    rows = run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
        CALL {
            WITH t
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN count(p) AS postCount
        }
        CALL {
            WITH t
            OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN count(c) AS commentCount
        }
        CALL {
            WITH t
            OPTIONAL MATCH (u:User)-[i:INTERESTED_IN]->(t)
            RETURN count(DISTINCT u) AS userCount,
                   coalesce(sum(i.count), 0) AS totalInteractions
        }
        CALL {
            WITH t
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($previous_start)
              AND p.posted_at < datetime($previous_end)
            RETURN count(p) AS postsPrev
        }
        CALL {
            WITH t
            OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($previous_start)
              AND c.posted_at < datetime($previous_end)
            RETURN count(c) AS commentsPrev
        }
        CALL {
            WITH t
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                RETURN toLower(coalesce(s.label, '')) AS label, count(*) AS score
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                RETURN toLower(coalesce(s.label, '')) AS label, count(*) AS score
            }
            WITH label, sum(score) AS score
            WHERE label <> ''
            RETURN
                sum(CASE WHEN label = 'positive' THEN score ELSE 0 END) AS positiveScore,
                sum(CASE WHEN label = 'neutral' THEN score ELSE 0 END) AS neutralScore,
                sum(CASE WHEN label IN ['negative', 'urgent', 'sarcastic'] THEN score ELSE 0 END) AS negativeScore
        }
        CALL {
            WITH t
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                       'message' AS kind,
                       coalesce(ch.username, ch.title, 'unknown') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(p.text, ''), 1200) AS text,
                       toString(p.posted_at) AS timestamp,
                       coalesce(ch.username, ch.title, 'unknown') AS actorKey,
                       coalesce(p.views, 0) AS reactions,
                       coalesce(p.comment_count, 0) AS replies
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                       'reply' AS kind,
                       coalesce(toString(u.telegram_user_id), 'anonymous') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(c.text, ''), 1200) AS text,
                       toString(c.posted_at) AS timestamp,
                       coalesce(toString(u.telegram_user_id), coalesce(ch.username, ch.title, 'anonymous')) AS actorKey,
                       0 AS reactions,
                       0 AS replies
            }
            WITH evidenceId, kind, author, channel, text, timestamp, actorKey, reactions, replies
            WHERE text <> ''
            ORDER BY timestamp DESC, evidenceId DESC
            WITH collect({
                id: evidenceId,
                type: kind,
                author: author,
                channel: channel,
                text: text,
                timestamp: timestamp,
                reactions: reactions,
                replies: replies
            }) AS evidenceRows,
            collect(DISTINCT CASE WHEN actorKey <> '' THEN actorKey END) AS actorKeys,
            collect(DISTINCT CASE WHEN channel <> '' AND channel <> 'unknown' THEN channel END) AS channels
            RETURN head(evidenceRows) AS sampleEvidence,
                   size(evidenceRows) AS evidenceCount,
                   size([actor IN actorKeys WHERE actor IS NOT NULL]) AS distinctUsers,
                   size([channel IN channels WHERE channel IS NOT NULL]) AS distinctChannels
        }
        CALL {
            WITH t
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(ch.title, ch.username, 'unknown') AS channel
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(ch.title, ch.username, 'unknown') AS channel
            }
            WITH channel, count(*) AS mentions
            WHERE channel <> '' AND channel <> 'unknown'
            ORDER BY mentions DESC, channel ASC
            RETURN collect(channel)[..3] AS topChannels
        }
        WITH t, cat, postCount, commentCount, userCount, totalInteractions,
             postsPrev, commentsPrev,
             sampleEvidence, evidenceCount, distinctUsers, distinctChannels, topChannels,
             coalesce(positiveScore, 0) AS positiveScore,
             coalesce(neutralScore, 0) AS neutralScore,
             coalesce(negativeScore, 0) AS negativeScore,
             (postCount + commentCount) AS mentionCount,
             (postsPrev + commentsPrev) AS prevMentions,
             coalesce(positiveScore, 0) + coalesce(neutralScore, 0) + coalesce(negativeScore, 0) AS sentimentTotal
        RETURN t.name AS name,
               cat.name AS category,
               postCount,
               commentCount,
               mentionCount,
               distinctUsers AS userCount,
               mentionCount AS totalInteractions,
               mentionCount AS last7Mentions,
               prevMentions AS prev7Mentions,
               sampleEvidence,
               evidenceCount,
               distinctUsers,
               distinctChannels,
               topChannels,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * positiveScore / sentimentTotal)) ELSE 0 END AS sentimentPositive,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * neutralScore / sentimentTotal)) ELSE 0 END AS sentimentNeutral,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * negativeScore / sentimentTotal)) ELSE 0 END AS sentimentNegative,
               CASE
                   WHEN prevMentions > 0 THEN round(100.0 * (mentionCount - prevMentions) / prevMentions, 1)
                   WHEN mentionCount > 0 THEN 100.0
                   ELSE 0.0
               END AS growth7dPct
        ORDER BY mentionCount DESC, distinctUsers DESC, evidenceCount DESC, t.name ASC
    """, {
        **_range_params(resolved_ctx),
        "noise": sorted(_NOISY_TOPIC_KEYS),
    })
    filtered = [_decorate_topics_page_row(row) for row in rows]
    filtered = [row for row in filtered if _is_topics_page_row_allowed(row)]
    filtered.sort(key=_topics_page_sort_key)
    start_idx = max(0, int(page)) * max(1, int(size))
    end_idx = start_idx + max(1, int(size))
    return filtered[start_idx:end_idx]


def _topic_overview_quality_tier(
    *,
    mentions: int,
    evidence_count: int,
    distinct_users: int,
    distinct_channels: int,
) -> str:
    if mentions >= 40 and evidence_count >= 12 and distinct_users >= 8 and distinct_channels >= 3:
        return "high"
    if mentions >= 18 and evidence_count >= 8 and distinct_users >= 4 and distinct_channels >= 2:
        return "medium"
    return "low"


def _map_topic_overview_evidence(rows: Any, *, limit: int) -> list[dict]:
    output: list[dict] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        text = _safe_str(row.get("text"), "").strip()
        if not text:
            continue
        output.append(
            {
                "id": _safe_str(row.get("id"), ""),
                "type": _safe_str(row.get("type"), "message"),
                "author": _safe_str(row.get("author"), "unknown"),
                "channel": _safe_str(row.get("channel"), "unknown"),
                "text": text,
                "timestamp": _safe_str(row.get("timestamp"), ""),
                "reactions": _safe_int(row.get("reactions")),
                "replies": _safe_int(row.get("replies")),
            }
        )
        if len(output) >= max(1, int(limit)):
            break
    return output


def get_topic_overview_candidates(
    ctx: DashboardDateContext | None = None,
    *,
    limit: int = 24,
    evidence_limit: int = 5,
    question_limit: int = 3,
) -> list[dict]:
    """Compact per-topic inputs for background topic-overview materialization."""
    resolved_ctx = ctx or _default_detail_context()
    safe_limit = max(1, min(int(limit), 48))
    safe_evidence_limit = max(1, min(int(evidence_limit), 8))
    safe_question_limit = max(0, min(int(question_limit), 5))

    rows = run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
        CALL {
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                OPTIONAL MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
                WITH p, ch, max(toLower(coalesce(s.label, ''))) AS sentimentLabel
                RETURN {
                    id: coalesce(p.uuid, 'post:' + elementId(p)),
                    type: 'message',
                    contentType: 'post',
                    author: coalesce(ch.username, ch.title, 'unknown'),
                    channel: coalesce(ch.title, ch.username, 'unknown'),
                    text: left(coalesce(p.text, ''), 1200),
                    timestamp: toString(p.posted_at),
                    occurredAt: p.posted_at,
                    actorKey: coalesce(ch.username, ch.title, 'unknown'),
                    reactions: coalesce(p.views, 0),
                    replies: coalesce(p.comment_count, 0),
                    sentimentLabel: coalesce(sentimentLabel, ''),
                    isQuestion: p.text IS NOT NULL AND trim(p.text) <> '' AND p.text CONTAINS '?'
                } AS event
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                OPTIONAL MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
                WITH c, u, ch, max(toLower(coalesce(s.label, ''))) AS sentimentLabel
                RETURN {
                    id: coalesce(c.uuid, 'comment:' + elementId(c)),
                    type: 'reply',
                    contentType: 'comment',
                    author: coalesce(toString(u.telegram_user_id), 'anonymous'),
                    channel: coalesce(ch.title, ch.username, 'unknown'),
                    text: left(coalesce(c.text, ''), 1200),
                    timestamp: toString(c.posted_at),
                    occurredAt: c.posted_at,
                    actorKey: coalesce(toString(u.telegram_user_id), coalesce(ch.username, ch.title, 'anonymous')),
                    reactions: 0,
                    replies: 0,
                    sentimentLabel: coalesce(sentimentLabel, ''),
                    isQuestion: c.text IS NOT NULL AND trim(c.text) <> '' AND c.text CONTAINS '?'
                } AS event
            }
            WITH event
            WHERE event.text IS NOT NULL AND trim(event.text) <> ''
            ORDER BY event.occurredAt DESC, event.id DESC
            RETURN collect(event) AS currentRows
        }
        CALL (t) {
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($previous_start)
                  AND p.posted_at < datetime($previous_end)
                RETURN 1 AS hit
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($previous_start)
                  AND c.posted_at < datetime($previous_end)
                RETURN 1 AS hit
            }
            RETURN count(hit) AS prevMentions
        }
        WITH t, cat, currentRows, prevMentions
        WHERE size(currentRows) > 0
        CALL {
            WITH currentRows
            UNWIND currentRows AS row
            WITH row.channel AS channel, count(*) AS mentions
            WHERE channel <> '' AND channel <> 'unknown'
            ORDER BY mentions DESC, channel ASC
            RETURN collect(channel)[..3] AS topChannels
        }
        WITH t, cat, currentRows, prevMentions, topChannels,
             size([row IN currentRows WHERE row.contentType = 'post' | 1]) AS postCount,
             size([row IN currentRows WHERE row.contentType = 'comment' | 1]) AS commentCount,
             size([row IN currentRows WHERE row.sentimentLabel = 'positive' | 1]) AS positiveScore,
             size([row IN currentRows WHERE row.sentimentLabel = 'neutral' | 1]) AS neutralScore,
             size([row IN currentRows WHERE row.sentimentLabel IN ['negative', 'urgent', 'sarcastic'] | 1]) AS negativeScore,
             size(reduce(acc = [], row IN currentRows | CASE WHEN row.actorKey <> '' AND NOT (row.actorKey IN acc) THEN acc + row.actorKey ELSE acc END)) AS distinctUsers,
             size(reduce(acc = [], row IN currentRows | CASE WHEN row.channel <> '' AND row.channel <> 'unknown' AND NOT (row.channel IN acc) THEN acc + row.channel ELSE acc END)) AS distinctChannels,
             [row IN currentRows[..$evidence_limit] | {
                 id: row.id,
                 type: row.type,
                 author: row.author,
                 channel: row.channel,
                 text: row.text,
                 timestamp: row.timestamp,
                 reactions: row.reactions,
                 replies: row.replies
             }] AS evidence,
             [row IN currentRows WHERE row.isQuestion | {
                 id: row.id,
                 type: row.type,
                 author: row.author,
                 channel: row.channel,
                 text: row.text,
                 timestamp: row.timestamp,
                 reactions: row.reactions,
                 replies: row.replies
             }][..$question_limit] AS questionEvidence,
             head([row IN currentRows | {
                 id: row.id,
                 type: row.type,
                 author: row.author,
                 channel: row.channel,
                 text: row.text,
                 timestamp: row.timestamp,
                 reactions: row.reactions,
                 replies: row.replies
             }]) AS sampleEvidence,
             head(currentRows).timestamp AS latestAt,
             size(currentRows) AS mentionCount
        WITH t, cat, postCount, commentCount, mentionCount, prevMentions, latestAt, topChannels,
             sampleEvidence, evidence, questionEvidence, distinctUsers, distinctChannels,
             positiveScore, neutralScore, negativeScore,
             positiveScore + neutralScore + negativeScore AS sentimentTotal
        RETURN t.name AS name,
               cat.name AS category,
               postCount,
               commentCount,
               mentionCount,
               prevMentions AS prev7Mentions,
               latestAt,
               topChannels,
               sampleEvidence,
               evidence,
               questionEvidence,
               mentionCount AS evidenceCount,
               distinctUsers,
               distinctChannels,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * positiveScore / sentimentTotal)) ELSE 0 END AS sentimentPositive,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * neutralScore / sentimentTotal)) ELSE 0 END AS sentimentNeutral,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * negativeScore / sentimentTotal)) ELSE 0 END AS sentimentNegative,
               CASE
                   WHEN prevMentions > 0 THEN round(100.0 * (mentionCount - prevMentions) / prevMentions, 1)
                   WHEN mentionCount > 0 THEN 100.0
                   ELSE 0.0
               END AS growth7dPct
        ORDER BY mentionCount DESC, distinctUsers DESC, distinctChannels DESC, name ASC
        LIMIT $limit
    """, {
        **_range_params(resolved_ctx),
        "noise": sorted(_NOISY_TOPIC_KEYS),
        "limit": safe_limit,
        "evidence_limit": safe_evidence_limit,
        "question_limit": safe_question_limit,
    })

    candidates: list[dict] = []
    for row in rows:
        decorated = _decorate_topics_page_row(row)
        if not _is_topics_page_row_allowed(decorated):
            continue
        evidence = _map_topic_overview_evidence(row.get("evidence"), limit=safe_evidence_limit)
        if not evidence:
            continue
        question_rows = row.get("questionEvidence") if safe_question_limit > 0 else []
        question_evidence = _map_topic_overview_evidence(question_rows, limit=max(1, safe_question_limit or 1))
        mentions = _safe_int(decorated.get("mentionCount"), _safe_int(decorated.get("currentMentions")))
        previous_mentions = _safe_int(decorated.get("prev7Mentions"), _safe_int(decorated.get("previousMentions")))
        distinct_users = _safe_int(decorated.get("distinctUsers"))
        distinct_channels = _safe_int(decorated.get("distinctChannels"))
        evidence_count = _safe_int(decorated.get("evidenceCount"), len(evidence))
        candidates.append(
            {
                "topic": _safe_str(decorated.get("name"), ""),
                "sourceTopic": _safe_str(decorated.get("sourceTopic"), _safe_str(decorated.get("name"), "")),
                "category": _safe_str(decorated.get("category"), "General"),
                "topicGroup": _safe_str(decorated.get("topicGroup"), ""),
                "mentions": mentions,
                "previousMentions": previous_mentions,
                "growth": _safe_int(round(float(row.get("growth7dPct") or 0.0))),
                "distinctUsers": distinct_users,
                "distinctChannels": distinct_channels,
                "evidenceCount": evidence_count,
                "latestAt": _safe_str(row.get("latestAt"), ""),
                "topChannels": [_safe_str(ch) for ch in (row.get("topChannels") or []) if _safe_str(ch)],
                "sentimentPositive": _safe_int(row.get("sentimentPositive")),
                "sentimentNeutral": _safe_int(row.get("sentimentNeutral")),
                "sentimentNegative": _safe_int(row.get("sentimentNegative")),
                "qualityTier": _topic_overview_quality_tier(
                    mentions=mentions,
                    evidence_count=evidence_count,
                    distinct_users=distinct_users,
                    distinct_channels=distinct_channels,
                ),
                "evidence": evidence,
                "questionEvidence": question_evidence,
            }
        )
    return candidates


def get_topic_detail_v1(topic_name: str, category: str | None = None, ctx: DashboardDateContext | None = None) -> dict | None:
    """Full topic detail with evidence for the selected topic."""
    resolved_ctx = ctx or _default_detail_context()
    rows = run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND t.name = $topic_name
          AND ($category = '' OR cat.name = $category)
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
        CALL {
            WITH t
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN count(p) AS postCount
        }
        CALL {
            WITH t
            OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN count(c) AS commentCount
        }
        CALL {
            WITH t
            OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($previous_start)
              AND p.posted_at < datetime($previous_end)
            RETURN count(p) AS postsPrev
        }
        CALL {
            WITH t
            OPTIONAL MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($previous_start)
              AND c.posted_at < datetime($previous_end)
            RETURN count(c) AS commentsPrev
        }
        CALL (t) {
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                RETURN toString(date(p.posted_at)) AS day
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                RETURN toString(date(c.posted_at)) AS day
            }
            WITH day, count(*) AS count
            WHERE day IS NOT NULL
            ORDER BY day
            RETURN collect({day: day, count: count}) AS dailyRows
        }
        CALL {
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                RETURN date(p.posted_at).year AS year, date(p.posted_at).week AS week
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                RETURN date(c.posted_at).year AS year, date(c.posted_at).week AS week
            }
            WITH year, week, count(*) AS count
            WHERE week IS NOT NULL
            ORDER BY year, week
            RETURN collect({year: year, week: week, count: count}) AS weeklyRows
        }
        CALL {
            WITH t
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                RETURN toLower(coalesce(s.label, '')) AS label, count(*) AS score
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                RETURN toLower(coalesce(s.label, '')) AS label, count(*) AS score
            }
            WITH label, sum(score) AS score
            WHERE label <> ''
            RETURN
                sum(CASE WHEN label = 'positive' THEN score ELSE 0 END) AS positiveScore,
                sum(CASE WHEN label = 'neutral' THEN score ELSE 0 END) AS neutralScore,
                sum(CASE WHEN label IN ['negative', 'urgent', 'sarcastic'] THEN score ELSE 0 END) AS negativeScore
        }
        CALL {
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(ch.title, ch.username, 'unknown') AS channel, count(DISTINCT p) AS mentions
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(ch.title, ch.username, 'unknown') AS channel, count(DISTINCT c) AS mentions
            }
            WITH channel, sum(mentions) AS mentions
            WHERE channel <> ''
            ORDER BY mentions DESC, channel ASC
            RETURN collect(channel)[..3] AS topChannels
        }
        CALL {
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                       'message' AS kind,
                       coalesce(ch.username, ch.title, 'unknown') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(p.text, ''), 1200) AS text,
                       toString(p.posted_at) AS timestamp,
                       coalesce(ch.username, ch.title, 'unknown') AS actorKey,
                       coalesce(p.views, 0) AS reactions,
                       coalesce(p.comment_count, 0) AS replies
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                       'reply' AS kind,
                       coalesce(toString(u.telegram_user_id), 'anonymous') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(c.text, ''), 1200) AS text,
                       toString(c.posted_at) AS timestamp,
                       coalesce(toString(u.telegram_user_id), coalesce(ch.username, ch.title, 'anonymous')) AS actorKey,
                       0 AS reactions,
                       0 AS replies
            }
            WITH evidenceId, kind, author, channel, text, timestamp, actorKey, reactions, replies
            WHERE text <> ''
            ORDER BY timestamp DESC, evidenceId DESC
            WITH collect({
                id: evidenceId,
                type: kind,
                author: author,
                channel: channel,
                text: text,
                timestamp: timestamp,
                reactions: reactions,
                replies: replies
            }) AS evidenceRows,
            collect(DISTINCT CASE WHEN actorKey <> '' THEN actorKey END) AS actorKeys,
            collect(DISTINCT CASE WHEN channel <> '' AND channel <> 'unknown' THEN channel END) AS channels
            RETURN evidenceRows[..6] AS evidence,
                   head(evidenceRows) AS sampleEvidence,
                   size(evidenceRows) AS evidenceCount,
                   size([actor IN actorKeys WHERE actor IS NOT NULL]) AS distinctUsers,
                   size([channel IN channels WHERE channel IS NOT NULL]) AS distinctChannels
        }
        CALL {
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                  AND p.text IS NOT NULL
                  AND trim(p.text) <> ''
                  AND p.text CONTAINS '?'
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                       'message' AS kind,
                       coalesce(ch.username, ch.title, 'unknown') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(p.text, ''), 1200) AS text,
                       toString(p.posted_at) AS timestamp,
                       coalesce(p.views, 0) AS reactions,
                       coalesce(p.comment_count, 0) AS replies
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                  AND c.text IS NOT NULL
                  AND trim(c.text) <> ''
                  AND c.text CONTAINS '?'
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                       'reply' AS kind,
                       coalesce(toString(u.telegram_user_id), 'anonymous') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(c.text, ''), 1200) AS text,
                       toString(c.posted_at) AS timestamp,
                       0 AS reactions,
                       0 AS replies
            }
            WITH evidenceId, kind, author, channel, text, timestamp, reactions, replies
            WHERE text <> ''
            ORDER BY timestamp DESC, evidenceId DESC
            RETURN collect({
                id: evidenceId,
                type: kind,
                author: author,
                channel: channel,
                text: text,
                timestamp: timestamp,
                reactions: reactions,
                replies: replies
            })[..12] AS questionEvidence
        }
        WITH t, cat, postCount, commentCount,
             postsPrev, commentsPrev, dailyRows, weeklyRows, topChannels,
             sampleEvidence, evidenceCount, distinctUsers, distinctChannels,
             coalesce(positiveScore, 0) AS positiveScore,
             coalesce(neutralScore, 0) AS neutralScore,
             coalesce(negativeScore, 0) AS negativeScore,
             (postCount + commentCount) AS mentionCount,
             (postsPrev + commentsPrev) AS prevMentions,
             coalesce(positiveScore, 0) + coalesce(neutralScore, 0) + coalesce(negativeScore, 0) AS sentimentTotal,
             evidence,
             questionEvidence
        RETURN t.name AS name,
               cat.name AS category,
               postCount,
               commentCount,
               mentionCount,
               distinctUsers AS userCount,
               mentionCount AS totalInteractions,
               mentionCount AS last7Mentions,
               prevMentions AS prev7Mentions,
               dailyRows,
               weeklyRows,
               topChannels,
               sampleEvidence,
               evidenceCount,
               distinctUsers,
               distinctChannels,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * positiveScore / sentimentTotal)) ELSE 0 END AS sentimentPositive,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * neutralScore / sentimentTotal)) ELSE 0 END AS sentimentNeutral,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * negativeScore / sentimentTotal)) ELSE 0 END AS sentimentNegative,
               CASE
                   WHEN prevMentions > 0 THEN round(100.0 * (mentionCount - prevMentions) / prevMentions, 1)
                   WHEN mentionCount > 0 THEN 100.0
                   ELSE 0.0
               END AS growth7dPct,
               evidence,
               questionEvidence
        ORDER BY mentionCount DESC
        LIMIT 1
    """, {
        **_range_params(resolved_ctx),
        "topic_name": topic_name,
        "category": category or "",
        "noise": sorted(_NOISY_TOPIC_KEYS),
    })
    if not rows:
        return None
    decorated = _decorate_topics_page_row(rows[0])
    if not _is_topics_page_row_allowed(decorated):
        return None
    return decorated


def get_topic_evidence_page_v1(
    topic_name: str,
    category: str | None = None,
    view: str = "all",
    page: int = 0,
    size: int = 20,
    focus_id: str | None = None,
    ctx: DashboardDateContext | None = None,
) -> dict | None:
    """Paginated topic evidence feed for the selected timeframe."""
    resolved_ctx = ctx or _default_detail_context()
    detail = get_topic_detail(topic_name, category, resolved_ctx)
    if detail is None or not _is_topics_page_row_allowed(detail):
        return None
    safe_page = max(0, int(page))
    safe_size = max(1, min(int(size), 50))
    skip = safe_page * safe_size
    end = skip + safe_size
    questions_only = (view or "all").strip().lower() == "questions"
    rows = run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND t.name = $topic_name
          AND ($category = '' OR cat.name = $category)
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
        CALL {
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                  AND ($questions_only = false OR (p.text IS NOT NULL AND trim(p.text) <> '' AND p.text CONTAINS '?'))
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                       'message' AS kind,
                       coalesce(ch.username, ch.title, 'unknown') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(p.text, ''), 1200) AS text,
                       toString(p.posted_at) AS timestamp,
                       coalesce(p.views, 0) AS reactions,
                       coalesce(p.comment_count, 0) AS replies
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                  AND ($questions_only = false OR (c.text IS NOT NULL AND trim(c.text) <> '' AND c.text CONTAINS '?'))
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                       'reply' AS kind,
                       coalesce(toString(u.telegram_user_id), 'anonymous') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(c.text, ''), 1200) AS text,
                       toString(c.posted_at) AS timestamp,
                       0 AS reactions,
                       0 AS replies
            }
            WITH evidenceId, kind, author, channel, text, timestamp, reactions, replies
            WHERE text <> ''
            RETURN count(*) AS total
        }
        CALL {
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                  AND ($questions_only = false OR (p.text IS NOT NULL AND trim(p.text) <> '' AND p.text CONTAINS '?'))
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                       'message' AS kind,
                       coalesce(ch.username, ch.title, 'unknown') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(p.text, ''), 1200) AS text,
                       toString(p.posted_at) AS timestamp,
                       coalesce(p.views, 0) AS reactions,
                       coalesce(p.comment_count, 0) AS replies
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                  AND ($questions_only = false OR (c.text IS NOT NULL AND trim(c.text) <> '' AND c.text CONTAINS '?'))
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                       'reply' AS kind,
                       coalesce(toString(u.telegram_user_id), 'anonymous') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(c.text, ''), 1200) AS text,
                       toString(c.posted_at) AS timestamp,
                       0 AS reactions,
                       0 AS replies
            }
            WITH evidenceId, kind, author, channel, text, timestamp, reactions, replies
            WHERE text <> ''
            ORDER BY timestamp DESC, evidenceId DESC
            SKIP $skip LIMIT $size
            RETURN collect({
                id: evidenceId,
                type: kind,
                author: author,
                channel: channel,
                text: text,
                timestamp: timestamp,
                reactions: reactions,
                replies: replies
            }) AS items
        }
        CALL {
            CALL {
                WITH t
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                  AND ($questions_only = false OR (p.text IS NOT NULL AND trim(p.text) <> '' AND p.text CONTAINS '?'))
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(p.uuid, 'post:' + elementId(p)) AS evidenceId,
                       'message' AS kind,
                       coalesce(ch.username, ch.title, 'unknown') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(p.text, ''), 1200) AS text,
                       toString(p.posted_at) AS timestamp,
                       coalesce(p.views, 0) AS reactions,
                       coalesce(p.comment_count, 0) AS replies
                UNION ALL
                WITH t
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                  AND ($questions_only = false OR (c.text IS NOT NULL AND trim(c.text) <> '' AND c.text CONTAINS '?'))
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN coalesce(c.uuid, 'comment:' + elementId(c)) AS evidenceId,
                       'reply' AS kind,
                       coalesce(toString(u.telegram_user_id), 'anonymous') AS author,
                       coalesce(ch.title, ch.username, 'unknown') AS channel,
                       left(coalesce(c.text, ''), 1200) AS text,
                       toString(c.posted_at) AS timestamp,
                       0 AS reactions,
                       0 AS replies
            }
            WITH evidenceId, kind, author, channel, text, timestamp, reactions, replies
            WHERE text <> ''
              AND ($focus_id <> '' AND evidenceId = $focus_id)
            RETURN head(collect({
                id: evidenceId,
                type: kind,
                author: author,
                channel: channel,
                text: text,
                timestamp: timestamp,
                reactions: reactions,
                replies: replies
            })) AS focusedItem
        }
        RETURN total, items, focusedItem
        LIMIT 1
    """, {
        **_range_params(resolved_ctx),
        "topic_name": topic_name,
        "category": category or "",
        "questions_only": questions_only,
        "skip": skip,
        "size": safe_size,
        "focus_id": (focus_id or "").strip(),
        "noise": sorted(_NOISY_TOPIC_KEYS),
    })
    if not rows:
        return None
    row = rows[0]
    total = _safe_int(row.get("total"))
    items = list(row.get("items") or [])
    return {
        "items": items,
        "total": total,
        "page": safe_page,
        "size": safe_size,
        "hasMore": end < total,
        "focusedItem": row.get("focusedItem"),
    }


def get_topic_detail_v2(topic_name: str, category: str | None = None, ctx: DashboardDateContext | None = None) -> dict | None:
    """Single-stream topic detail query that reuses one materialized event set."""
    resolved_ctx = ctx or _default_detail_context()
    rows = run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND t.name = $topic_name
          AND ($category = '' OR cat.name = $category)
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
        CALL (t) {
            CALL (t) {
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                OPTIONAL MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
                WITH p, ch, max(toLower(coalesce(s.label, ''))) AS sentimentLabel
                RETURN {
                    id: coalesce(p.uuid, 'post:' + elementId(p)),
                    type: 'message',
                    contentType: 'post',
                    author: coalesce(ch.username, ch.title, 'unknown'),
                    channel: coalesce(ch.title, ch.username, 'unknown'),
                    text: left(coalesce(p.text, ''), 1200),
                    timestamp: toString(p.posted_at),
                    occurredAt: p.posted_at,
                    actorKey: coalesce(ch.username, ch.title, 'unknown'),
                    reactions: coalesce(p.views, 0),
                    replies: coalesce(p.comment_count, 0),
                    sentimentLabel: coalesce(sentimentLabel, ''),
                    isQuestion: p.text IS NOT NULL AND trim(p.text) <> '' AND p.text CONTAINS '?'
                } AS event
                UNION ALL
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                OPTIONAL MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
                WITH c, u, ch, max(toLower(coalesce(s.label, ''))) AS sentimentLabel
                RETURN {
                    id: coalesce(c.uuid, 'comment:' + elementId(c)),
                    type: 'reply',
                    contentType: 'comment',
                    author: coalesce(toString(u.telegram_user_id), 'anonymous'),
                    channel: coalesce(ch.title, ch.username, 'unknown'),
                    text: left(coalesce(c.text, ''), 1200),
                    timestamp: toString(c.posted_at),
                    occurredAt: c.posted_at,
                    actorKey: coalesce(toString(u.telegram_user_id), coalesce(ch.username, ch.title, 'anonymous')),
                    reactions: 0,
                    replies: 0,
                    sentimentLabel: coalesce(sentimentLabel, ''),
                    isQuestion: c.text IS NOT NULL AND trim(c.text) <> '' AND c.text CONTAINS '?'
                } AS event
            }
            WITH event
            ORDER BY event.occurredAt DESC, event.id DESC
            RETURN collect(event) AS currentRows
        }
        CALL (t) {
            CALL (t) {
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($previous_start)
                  AND p.posted_at < datetime($previous_end)
                RETURN 1 AS hit
                UNION ALL
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($previous_start)
                  AND c.posted_at < datetime($previous_end)
                RETURN 1 AS hit
            }
            RETURN count(hit) AS prevMentions
        }
        WITH t, cat, currentRows, prevMentions,
             [row IN currentRows WHERE row.text <> ''] AS displayRows
        WHERE size(displayRows) > 0
        CALL {
            WITH currentRows
            UNWIND currentRows AS row
            WITH toString(date(row.occurredAt)) AS day, count(*) AS count
            ORDER BY day
            RETURN collect({day: day, count: count}) AS dailyRows
        }
        CALL {
            WITH currentRows
            UNWIND currentRows AS row
            WITH date(row.occurredAt).year AS year, date(row.occurredAt).week AS week, count(*) AS count
            ORDER BY year, week
            RETURN collect({year: year, week: week, count: count}) AS weeklyRows
        }
        CALL {
            WITH currentRows
            UNWIND currentRows AS row
            WITH row.channel AS channel, count(*) AS mentions
            WHERE channel <> '' AND channel <> 'unknown'
            ORDER BY mentions DESC, channel ASC
            RETURN collect(channel)[..3] AS topChannels
        }
        WITH t, cat, currentRows, displayRows, prevMentions, dailyRows, weeklyRows, topChannels,
             size([row IN currentRows WHERE row.contentType = 'post' | 1]) AS postCount,
             size([row IN currentRows WHERE row.contentType = 'comment' | 1]) AS commentCount,
             size([row IN currentRows WHERE row.sentimentLabel = 'positive' | 1]) AS positiveScore,
             size([row IN currentRows WHERE row.sentimentLabel = 'neutral' | 1]) AS neutralScore,
             size([row IN currentRows WHERE row.sentimentLabel IN ['negative', 'urgent', 'sarcastic'] | 1]) AS negativeScore,
             size(reduce(acc = [], row IN displayRows | CASE WHEN row.actorKey <> '' AND NOT (row.actorKey IN acc) THEN acc + row.actorKey ELSE acc END)) AS distinctUsers,
             size(reduce(acc = [], row IN displayRows | CASE WHEN row.channel <> '' AND row.channel <> 'unknown' AND NOT (row.channel IN acc) THEN acc + row.channel ELSE acc END)) AS distinctChannels,
             [row IN displayRows[..6] | {
                 id: row.id,
                 type: row.type,
                 author: row.author,
                 channel: row.channel,
                 text: row.text,
                 timestamp: row.timestamp,
                 reactions: row.reactions,
                 replies: row.replies
             }] AS evidence,
             head([row IN displayRows | {
                 id: row.id,
                 type: row.type,
                 author: row.author,
                 channel: row.channel,
                 text: row.text,
                 timestamp: row.timestamp,
                 reactions: row.reactions,
                 replies: row.replies
             }]) AS sampleEvidence,
             [row IN displayRows WHERE row.isQuestion | {
                 id: row.id,
                 type: row.type,
                 author: row.author,
                 channel: row.channel,
                 text: row.text,
                 timestamp: row.timestamp,
                 reactions: row.reactions,
                 replies: row.replies
             }][..12] AS questionEvidence
        WITH t, cat, postCount, commentCount, prevMentions, dailyRows, weeklyRows, topChannels,
             sampleEvidence, distinctUsers, distinctChannels, evidence, questionEvidence,
             size(currentRows) AS mentionCount,
             size(displayRows) AS evidenceCount,
             positiveScore, neutralScore, negativeScore,
             positiveScore + neutralScore + negativeScore AS sentimentTotal
        RETURN t.name AS name,
               cat.name AS category,
               postCount,
               commentCount,
               mentionCount,
               distinctUsers AS userCount,
               mentionCount AS totalInteractions,
               mentionCount AS last7Mentions,
               prevMentions AS prev7Mentions,
               dailyRows,
               weeklyRows,
               topChannels,
               sampleEvidence,
               evidenceCount,
               distinctUsers,
               distinctChannels,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * positiveScore / sentimentTotal)) ELSE 0 END AS sentimentPositive,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * neutralScore / sentimentTotal)) ELSE 0 END AS sentimentNeutral,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * negativeScore / sentimentTotal)) ELSE 0 END AS sentimentNegative,
               CASE
                   WHEN prevMentions > 0 THEN round(100.0 * (mentionCount - prevMentions) / prevMentions, 1)
                   WHEN mentionCount > 0 THEN 100.0
                   ELSE 0.0
               END AS growth7dPct,
               evidence,
               questionEvidence
        LIMIT 1
    """, {
        **_range_params(resolved_ctx),
        "topic_name": topic_name,
        "category": category or "",
        "noise": sorted(_NOISY_TOPIC_KEYS),
    })
    if not rows:
        return None
    decorated = _decorate_topics_page_row(rows[0])
    if not _is_topics_page_row_allowed(decorated):
        return None
    return decorated


def get_topic_evidence_page_v2(
    topic_name: str,
    category: str | None = None,
    view: str = "all",
    page: int = 0,
    size: int = 20,
    focus_id: str | None = None,
    ctx: DashboardDateContext | None = None,
) -> dict | None:
    """Paginated topic evidence feed backed by a single materialized event set."""
    resolved_ctx = ctx or _default_detail_context()
    safe_page = max(0, int(page))
    safe_size = max(1, min(int(size), 50))
    skip = safe_page * safe_size
    page_end = skip + safe_size
    questions_only = (view or "all").strip().lower() == "questions"
    rows = run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND t.name = $topic_name
          AND ($category = '' OR cat.name = $category)
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
        CALL (t) {
            CALL (t) {
                MATCH (p:Post)-[:TAGGED]->(t)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
                RETURN {
                    id: coalesce(p.uuid, 'post:' + elementId(p)),
                    type: 'message',
                    author: coalesce(ch.username, ch.title, 'unknown'),
                    channel: coalesce(ch.title, ch.username, 'unknown'),
                    text: left(coalesce(p.text, ''), 1200),
                    timestamp: toString(p.posted_at),
                    occurredAt: p.posted_at,
                    reactions: coalesce(p.views, 0),
                    replies: coalesce(p.comment_count, 0),
                    isQuestion: p.text IS NOT NULL AND trim(p.text) <> '' AND p.text CONTAINS '?'
                } AS event
                UNION ALL
                MATCH (c:Comment)-[:TAGGED]->(t)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                OPTIONAL MATCH (u:User)-[:WROTE]->(c)
                OPTIONAL MATCH (c)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
                RETURN {
                    id: coalesce(c.uuid, 'comment:' + elementId(c)),
                    type: 'reply',
                    author: coalesce(toString(u.telegram_user_id), 'anonymous'),
                    channel: coalesce(ch.title, ch.username, 'unknown'),
                    text: left(coalesce(c.text, ''), 1200),
                    timestamp: toString(c.posted_at),
                    occurredAt: c.posted_at,
                    reactions: 0,
                    replies: 0,
                    isQuestion: c.text IS NOT NULL AND trim(c.text) <> '' AND c.text CONTAINS '?'
                } AS event
            }
            WITH event
            ORDER BY event.occurredAt DESC, event.id DESC
            RETURN collect(event) AS currentRows
        }
        WITH t, cat, currentRows,
             [row IN currentRows WHERE row.text <> ''] AS displayRows
        WHERE size(displayRows) > 0
        WITH t, cat, currentRows, displayRows,
             CASE
                 WHEN $questions_only THEN [row IN displayRows WHERE row.isQuestion]
                 ELSE displayRows
             END AS filteredRows
        RETURN t.name AS name,
               cat.name AS category,
               size(currentRows) AS mentionCount,
               size(displayRows) AS evidenceCount,
               head([row IN displayRows | {
                   id: row.id,
                   type: row.type,
                   author: row.author,
                   channel: row.channel,
                   text: row.text,
                   timestamp: row.timestamp,
                   reactions: row.reactions,
                   replies: row.replies
               }]) AS sampleEvidence,
               size(filteredRows) AS total,
               [row IN filteredRows[$skip..$page_end] | {
                   id: row.id,
                   type: row.type,
                   author: row.author,
                   channel: row.channel,
                   text: row.text,
                   timestamp: row.timestamp,
                   reactions: row.reactions,
                   replies: row.replies
               }] AS items,
               head([row IN filteredRows WHERE $focus_id <> '' AND row.id = $focus_id | {
                   id: row.id,
                   type: row.type,
                   author: row.author,
                   channel: row.channel,
                   text: row.text,
                   timestamp: row.timestamp,
                   reactions: row.reactions,
                   replies: row.replies
               }]) AS focusedItem
        LIMIT 1
    """, {
        **_range_params(resolved_ctx),
        "topic_name": topic_name,
        "category": category or "",
        "questions_only": questions_only,
        "skip": skip,
        "page_end": page_end,
        "focus_id": (focus_id or "").strip(),
        "noise": sorted(_NOISY_TOPIC_KEYS),
    })
    if not rows:
        return None
    row = rows[0]
    validation_row = _decorate_topics_page_row({
        "name": row.get("name"),
        "category": row.get("category"),
        "mentionCount": row.get("mentionCount"),
        "evidenceCount": row.get("evidenceCount"),
        "sampleEvidence": row.get("sampleEvidence"),
    })
    if not _is_topics_page_row_allowed(validation_row):
        return None
    total = _safe_int(row.get("total"))
    items = list(row.get("items") or [])
    return {
        "items": items,
        "total": total,
        "page": safe_page,
        "size": safe_size,
        "hasMore": page_end < total,
        "focusedItem": row.get("focusedItem"),
    }


def get_topic_detail(topic_name: str, category: str | None = None, ctx: DashboardDateContext | None = None) -> dict | None:
    if USE_TOPIC_QUERY_V2:
        return get_topic_detail_v2(topic_name, category, ctx)
    return get_topic_detail_v1(topic_name, category, ctx)


def get_topic_evidence_page(
    topic_name: str,
    category: str | None = None,
    view: str = "all",
    page: int = 0,
    size: int = 20,
    focus_id: str | None = None,
    ctx: DashboardDateContext | None = None,
) -> dict | None:
    if USE_TOPIC_QUERY_V2:
        return get_topic_evidence_page_v2(topic_name, category, view, page, size, focus_id, ctx)
    return get_topic_evidence_page_v1(topic_name, category, view, page, size, focus_id, ctx)


def get_all_channels(ctx: DashboardDateContext | None = None) -> list[dict]:
    """Compact channel summaries for the Channels detail page."""
    resolved_ctx = ctx or _default_detail_context()
    return run_query("""
        MATCH (ch:Channel)
        OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(pAll:Post)
        WHERE pAll.posted_at >= datetime($start)
          AND pAll.posted_at < datetime($end)
        WITH ch,
             count(pAll) AS postCount,
             avg(coalesce(pAll.views, 0)) AS avgViews,
             max(pAll.posted_at) AS lastPost
        CALL {
            WITH ch
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at >= datetime($previous_start)
              AND p.posted_at < datetime($previous_end)
            RETURN count(p) AS postsPrev
        }
        CALL {
            WITH ch
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)-[:TAGGED]->(t:Topic)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
            WITH t.name AS topic, count(p) AS mentions
            WHERE topic <> ''
            ORDER BY mentions DESC, topic ASC
            RETURN head(collect(topic)) AS topTopic
        }
        RETURN ch.username AS username,
               ch.title AS title,
               ch.member_count AS memberCount,
               ch.description AS description,
               postCount,
               round(avgViews) AS avgViews,
               toString(lastPost) AS lastPost,
               toInteger(round(postCount / toFloat($window_days))) AS dailyMessages,
               CASE
                   WHEN postsPrev > 0 THEN round(100.0 * (postCount - postsPrev) / postsPrev, 1)
                   WHEN postCount > 0 THEN 100.0
                   ELSE 0.0
               END AS growth7dPct,
               coalesce(topTopic, '') AS topTopic
        ORDER BY postCount DESC, ch.title ASC
    """, {
        **_range_params(resolved_ctx),
        "window_days": max(1, resolved_ctx.days),
        "noise": sorted(_NOISY_TOPIC_KEYS),
    })


def get_channel_detail(channel_key: str, ctx: DashboardDateContext | None = None) -> dict | None:
    """Full channel detail with recent posts and distributions."""
    resolved_ctx = ctx or _default_detail_context()
    rows = run_query("""
        MATCH (ch:Channel)
        WHERE coalesce(ch.username, '') = $channel_key
           OR coalesce(ch.title, '') = $channel_key
        OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(pAll:Post)
        WHERE pAll.posted_at >= datetime($start)
          AND pAll.posted_at < datetime($end)
        WITH ch,
             count(pAll) AS postCount,
             avg(coalesce(pAll.views, 0)) AS avgViews,
             max(pAll.posted_at) AS lastPost
        CALL {
            WITH ch
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at >= datetime($previous_start)
              AND p.posted_at < datetime($previous_end)
            RETURN count(p) AS postsPrev
        }
        CALL {
            WITH ch
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            WITH p.posted_at.dayOfWeek AS dow, count(p) AS c
            WHERE dow IS NOT NULL
            RETURN collect({dow: dow, count: c}) AS weeklyRows
        }
        CALL {
            WITH ch
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            WITH p.posted_at.hour AS hour, count(p) AS c
            WHERE hour IS NOT NULL
            RETURN collect({hour: hour, count: c}) AS hourlyRows
        }
        CALL {
            WITH ch
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)-[:TAGGED]->(t:Topic)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
            WITH t.name AS topic, count(p) AS mentions
            WHERE topic <> ''
            ORDER BY mentions DESC
            WITH collect({name: topic, mentions: mentions})[..6] AS topTopics, sum(mentions) AS totalMentions
            RETURN [tt IN topTopics | {
                name: tt.name,
                mentions: tt.mentions,
                pct: CASE WHEN totalMentions > 0 THEN toInteger(round(100.0 * tt.mentions / totalMentions)) ELSE 0 END
            }] AS topTopics
        }
        CALL {
            WITH ch
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            WITH coalesce(p.media_type, 'text') AS mediaType, count(p) AS count
            WHERE mediaType IS NOT NULL
            ORDER BY count DESC
            RETURN collect({type: mediaType, count: count})[..6] AS messageTypes
        }
        CALL {
            WITH ch
            OPTIONAL MATCH (u:User)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            WITH u, count(c) AS posts
            WHERE u IS NOT NULL
            ORDER BY posts DESC
            RETURN collect({
                name: coalesce(toString(u.telegram_user_id), 'anonymous'),
                posts: posts,
                helpScore: toInteger(CASE WHEN posts * 5 > 100 THEN 100 ELSE posts * 5 END)
            })[..4] AS topVoices
        }
        CALL {
            WITH ch
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
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
        CALL {
            WITH ch
            CALL {
                WITH ch
                OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE p.posted_at >= datetime($start)
                  AND p.posted_at < datetime($end)
                RETURN toLower(coalesce(s.label, '')) AS label, count(s) AS score
                UNION ALL
                WITH ch
                OPTIONAL MATCH (u:User)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch)
                OPTIONAL MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
                WHERE c.posted_at >= datetime($start)
                  AND c.posted_at < datetime($end)
                RETURN toLower(coalesce(s.label, '')) AS label, count(s) AS score
            }
            WITH label, sum(score) AS score
            WHERE label <> ''
            RETURN
                sum(CASE WHEN label = 'positive' THEN score ELSE 0 END) AS positiveScore,
                sum(CASE WHEN label = 'neutral' THEN score ELSE 0 END) AS neutralScore,
                sum(CASE WHEN label IN ['negative', 'urgent', 'sarcastic'] THEN score ELSE 0 END) AS negativeScore
        }
        WITH ch, postCount, avgViews, lastPost, postsPrev,
             weeklyRows, hourlyRows, topTopics, messageTypes, topVoices, recentPosts,
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
               toInteger(round(postCount / toFloat($window_days))) AS dailyMessages,
               CASE
                   WHEN postsPrev > 0 THEN round(100.0 * (postCount - postsPrev) / postsPrev, 1)
                   WHEN postCount > 0 THEN 100.0
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
        LIMIT 1
    """, {
        **_range_params(resolved_ctx),
        "window_days": max(1, resolved_ctx.days),
        "channel_key": channel_key,
        "noise": sorted(_NOISY_TOPIC_KEYS),
    })
    return rows[0] if rows else None


def get_channel_posts_page(
    channel_key: str,
    page: int = 0,
    size: int = 20,
    ctx: DashboardDateContext | None = None,
) -> dict | None:
    """Paginated channel posts feed for the selected timeframe."""
    resolved_ctx = ctx or _default_detail_context()
    safe_page = max(0, int(page))
    safe_size = max(1, min(int(size), 50))
    skip = safe_page * safe_size
    end = skip + safe_size
    rows = run_query("""
        MATCH (ch:Channel)
        WHERE coalesce(ch.username, '') = $channel_key
           OR coalesce(ch.title, '') = $channel_key
        CALL {
            WITH ch
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN count(p) AS total
        }
        CALL {
            WITH ch
            OPTIONAL MATCH (ch)<-[:IN_CHANNEL]-(p:Post)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            WITH ch, p
            WHERE p IS NOT NULL
            ORDER BY p.posted_at DESC, coalesce(p.uuid, elementId(p)) DESC
            SKIP $skip LIMIT $size
            RETURN collect({
                id: coalesce(p.uuid, 'post:' + elementId(p)),
                author: coalesce(ch.username, ch.title, 'unknown'),
                text: left(coalesce(p.text, ''), 220),
                timestamp: toString(p.posted_at),
                reactions: coalesce(p.views, 0),
                replies: coalesce(p.comment_count, 0)
            }) AS items
        }
        RETURN total, items
        LIMIT 1
    """, {
        **_range_params(resolved_ctx),
        "channel_key": channel_key,
        "skip": skip,
        "size": safe_size,
    })
    if not rows:
        return None
    row = rows[0]
    total = _safe_int(row.get("total"))
    return {
        "items": list(row.get("items") or []),
        "total": total,
        "page": safe_page,
        "size": safe_size,
        "hasMore": end < total,
    }


def get_all_audience(page: int = 0, size: int = 50, ctx: DashboardDateContext | None = None) -> list[dict]:
    """Compact audience summaries for the Audience detail page."""
    resolved_ctx = ctx or _default_detail_context()
    return run_query("""
        MATCH (u:User)
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN count(c) AS commentCount,
                   max(c.posted_at) AS lastSeenWindow
        }
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:TAGGED]->(t:Topic)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
            WITH t.name AS topic, count(c) AS mentionCount
            WHERE topic <> ''
            ORDER BY mentionCount DESC, topic ASC
            WITH collect(topic)[..5] AS topics,
                 collect({name: topic, count: mentionCount})[..5] AS topTopics
            RETURN topics, topTopics
        }
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
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
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            WITH toLower(coalesce(s.label, '')) AS label, count(s) AS score
            WHERE label <> ''
            RETURN
                sum(CASE WHEN label = 'positive' THEN score ELSE 0 END) AS positiveScore,
                sum(CASE WHEN label = 'neutral' THEN score ELSE 0 END) AS neutralScore,
                sum(CASE WHEN label IN ['negative', 'urgent', 'sarcastic'] THEN score ELSE 0 END) AS negativeScore
        }
        WITH u, commentCount, lastSeenWindow, topics, topTopics, channels,
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
               toString(coalesce(lastSeenWindow, u.last_seen)) AS lastSeen,
               commentCount,
               topics,
               topTopics,
               channels,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * positiveScore / sentimentTotal)) ELSE 0 END AS sentimentPositive,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * neutralScore / sentimentTotal)) ELSE 0 END AS sentimentNeutral,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * negativeScore / sentimentTotal)) ELSE 0 END AS sentimentNegative
        ORDER BY commentCount DESC, userId ASC
        SKIP $skip LIMIT $size
    """, {
        **_range_params(resolved_ctx),
        "skip": page * size,
        "size": size,
        "noise": sorted(_NOISY_TOPIC_KEYS),
    })


def get_audience_detail(user_id: str, ctx: DashboardDateContext | None = None) -> dict | None:
    """Full audience-member detail payload."""
    resolved_ctx = ctx or _default_detail_context()
    rows = run_query("""
        MATCH (u:User)
        WHERE toString(u.telegram_user_id) = $user_id
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN count(c) AS commentCount,
                   max(c.posted_at) AS lastSeenWindow
        }
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:TAGGED]->(t:Topic)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
            WITH t.name AS topic, count(c) AS mentionCount
            WHERE topic <> ''
            ORDER BY mentionCount DESC, topic ASC
            WITH collect(topic)[..5] AS topics,
                 collect({name: topic, count: mentionCount})[..5] AS topTopics
            RETURN topics, topTopics
        }
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
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
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
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
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            WITH date(c.posted_at) AS day, count(c) AS msgs
            WHERE day IS NOT NULL
            ORDER BY day ASC
            RETURN collect({week: toString(day), msgs: msgs})[..6] AS activityData
        }
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            WITH toLower(coalesce(s.label, '')) AS label, count(s) AS score
            WHERE label <> ''
            RETURN
                sum(CASE WHEN label = 'positive' THEN score ELSE 0 END) AS positiveScore,
                sum(CASE WHEN label = 'neutral' THEN score ELSE 0 END) AS neutralScore,
                sum(CASE WHEN label IN ['negative', 'urgent', 'sarcastic'] THEN score ELSE 0 END) AS negativeScore
        }
        WITH u, commentCount, lastSeenWindow, topics, topTopics, channels, recentMessages, activityData,
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
               toString(coalesce(lastSeenWindow, u.last_seen)) AS lastSeen,
               commentCount,
               topics,
               topTopics,
               channels,
               recentMessages,
               activityData,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * positiveScore / sentimentTotal)) ELSE 0 END AS sentimentPositive,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * neutralScore / sentimentTotal)) ELSE 0 END AS sentimentNeutral,
               CASE WHEN sentimentTotal > 0 THEN toInteger(round(100.0 * negativeScore / sentimentTotal)) ELSE 0 END AS sentimentNegative
        LIMIT 1
    """, {
        **_range_params(resolved_ctx),
        "user_id": user_id,
        "noise": sorted(_NOISY_TOPIC_KEYS),
    })
    return rows[0] if rows else None


def get_audience_messages_page(
    user_id: str,
    page: int = 0,
    size: int = 20,
    ctx: DashboardDateContext | None = None,
) -> dict | None:
    """Paginated audience recent-messages feed for the selected timeframe."""
    resolved_ctx = ctx or _default_detail_context()
    safe_page = max(0, int(page))
    safe_size = max(1, min(int(size), 50))
    skip = safe_page * safe_size
    end = skip + safe_size
    rows = run_query("""
        MATCH (u:User)
        WHERE toString(u.telegram_user_id) = $user_id
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN count(c) AS total
        }
        CALL {
            WITH u
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            WITH c, ch
            WHERE c IS NOT NULL
            ORDER BY c.posted_at DESC, coalesce(c.uuid, elementId(c)) DESC
            SKIP $skip LIMIT $size
            RETURN collect({
                id: coalesce(c.uuid, 'comment:' + elementId(c)),
                text: left(coalesce(c.text, ''), 220),
                channel: coalesce(ch.title, ch.username, 'unknown'),
                timestamp: toString(c.posted_at),
                reactions: 0,
                replies: 0
            }) AS items
        }
        RETURN total, items
        LIMIT 1
    """, {
        **_range_params(resolved_ctx),
        "user_id": user_id,
        "skip": skip,
        "size": safe_size,
    })
    if not rows:
        return None
    row = rows[0]
    total = _safe_int(row.get("total"))
    return {
        "items": list(row.get("items") or []),
        "total": total,
        "page": safe_page,
        "size": safe_size,
        "hasMore": end < total,
    }
