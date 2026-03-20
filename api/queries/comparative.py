"""
comparative.py — Tier 8: Comparative Analytics & Deep Dive

Provides: weeklyShifts, sentimentByTopic, topPosts, contentTypePerformance,
          vitalityIndicators, allTopics, allChannels, allAudience
"""
from __future__ import annotations
from collections import defaultdict
from datetime import timedelta
from typing import Any, Iterable

from api.dashboard_dates import DashboardDateContext
from api.db import run_query, run_single
from api.queries import predictive, pulse
from buffer.supabase_writer import SupabaseWriter
from utils.topic_normalizer import normalize_model_topics


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


def _fetch_window_posts(ctx: DashboardDateContext) -> list[dict]:
    return _paginate(
        lambda from_idx, to_idx: _supabase().client.table("telegram_posts")
        .select("id, posted_at", count="exact")
        .gte("posted_at", ctx.start_at.isoformat())
        .lt("posted_at", ctx.end_at.isoformat())
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


def _safe_int(value, fallback: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return fallback


def _safe_pct(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        return 0
    return int(round((float(numerator) / float(denominator)) * 100.0))


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


def get_sentiment_by_topic(ctx: DashboardDateContext) -> list[dict]:
    """Reliable topic sentiment counts reconstructed from source posts/comments + AI analysis."""
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
