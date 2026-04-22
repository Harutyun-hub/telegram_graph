"""
actionable.py — Tier 7: Business Intelligence & Opportunities

Provides: businessOpportunities, jobSeeking, jobTrends, housingData
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import time

from api.dashboard_dates import DashboardDateContext
from api.db import run_query
from buffer.supabase_writer import SupabaseWriter
from loguru import logger


_supabase_writer: SupabaseWriter | None = None
_WORK_SIGNAL_TYPES = ("Job_Seeking", "Hiring", "Partnership_Request")
_WORK_SIGNAL_PRIORITY = {
    "Job_Seeking": 3,
    "Hiring": 2,
    "Partnership_Request": 1,
}
_WORK_EVIDENCE_CATEGORIES = ("Employment", "Business & Enterprise")
_WORK_EVIDENCE_KEYWORDS = (
    "job",
    "jobs",
    "hiring",
    "vacancy",
    "salary",
    "remote",
    "freelance",
    "career",
    "работ",
    "ваканс",
    "зарплат",
    "удален",
    "удалён",
    "найм",
    "собесед",
    "фриланс",
)
_NOISY_TOPIC_KEYS = ["", "null", "unknown", "none", "n/a", "na"]
_EXCLUDED_CATEGORIES = [
    "Politics",
    "Security",
    "Government & Leadership",
    "Opposition & Protest",
    "Military & Defense",
    "Geopolitical Alignment",
    "Global Conflict",
]
_DEMAND_HINTS = [
    "need help",
    "looking for",
    "where can i",
    "where to find",
    "is there anyone",
    "can anyone recommend",
    "can someone recommend",
    "any recommendations",
    "how do i find",
    "подскажите",
    "помогите",
    "где найти",
    "ищу",
    "нужен",
    "нужна",
    "нужны",
    "есть ли",
    "кто может",
    "можете порекомендовать",
]
_GAP_HINTS = [
    "no good",
    "nobody offers",
    "nothing reliable",
    "hard to find",
    "missing service",
    "market gap",
    "underserved",
    "not enough options",
    "нет нормаль",
    "нет хорош",
    "нет сервиса",
    "сложно найти",
    "не хватает",
    "никто не делает",
]
_EXCLUDED_OPPORTUNITY_TYPES = ["Hiring", "Investment_Interest", "Real_Estate", "Import_Export"]
_SUPABASE_PAGE_SIZE = 200
_work_signal_snapshot_cache: dict[str, dict] = {}


def _get_supabase_writer() -> SupabaseWriter:
    global _supabase_writer
    if _supabase_writer is None:
        _supabase_writer = SupabaseWriter()
    return _supabase_writer


def _parse_iso_datetime(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _paginate(query_factory) -> list[dict]:
    started_at = time.perf_counter()
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
    logger.info(
        "Actionable ai_analysis pagination | rows={} total_count={} page_size={} elapsed_ms={}",
        len(rows),
        total_count,
        _SUPABASE_PAGE_SIZE,
        round((time.perf_counter() - started_at) * 1000, 2),
    )
    return rows


def _fetch_batch_analyses_between(start_iso: str, end_iso: str) -> list[dict]:
    started_at = time.perf_counter()
    try:
        rows = _paginate(
            lambda from_idx, to_idx: _get_supabase_writer().client.table("ai_analysis")
            .select("telegram_user_id, content_id, raw_llm_response, created_at", count="exact")
            .eq("content_type", "batch")
            .not_.is_("telegram_user_id", "null")
            .gte("created_at", start_iso)
            .lt("created_at", end_iso)
            .order("created_at", desc=False)
            .range(from_idx, to_idx)
        )
        logger.info(
            "Actionable batch analysis fetch | start={} end={} rows={} elapsed_ms={}",
            start_iso,
            end_iso,
            len(rows),
            round((time.perf_counter() - started_at) * 1000, 2),
        )
        return rows
    except Exception as exc:
        logger.warning(
            "Actionable batch analysis fetch failed | start={} end={} elapsed_ms={} error={}",
            start_iso,
            end_iso,
            round((time.perf_counter() - started_at) * 1000, 2),
            exc,
        )
        return []


def _fetch_work_signal_rows(ctx: DashboardDateContext) -> list[dict]:
    started_at = time.perf_counter()
    try:
        response = _get_supabase_writer().client.rpc(
            "dashboard_batch_signal_summary",
            {
                "p_previous_start": ctx.previous_start_at.isoformat(),
                "p_previous_end": ctx.previous_end_at.isoformat(),
                "p_start": ctx.start_at.isoformat(),
                "p_end": ctx.end_at.isoformat(),
            },
        ).execute()
        rows = list(response.data or [])
        logger.info(
            "Actionable batch signal rpc | cache_key={} rows={} elapsed_ms={}",
            ctx.cache_key,
            len(rows),
            round((time.perf_counter() - started_at) * 1000, 2),
        )
        return rows
    except Exception as exc:
        logger.warning(
            "Actionable batch signal rpc failed; falling back to row scan | cache_key={} elapsed_ms={} error={}",
            ctx.cache_key,
            round((time.perf_counter() - started_at) * 1000, 2),
            exc,
        )
        return []


def _extract_work_signal_type(row: dict) -> str | None:
    raw = row.get("raw_llm_response")
    if not isinstance(raw, dict):
        return None
    biz = raw.get("business_opportunity")
    if not isinstance(biz, dict):
        return None
    value = str(biz.get("opportunity_type") or "").strip()
    return value if value in _WORK_SIGNAL_TYPES else None


def _dominant_signal_rows(
    counts_by_user: dict[str, Counter],
    latest_by_user: dict[str, dict[str, datetime]],
) -> list[dict]:
    rows: list[dict] = []
    for user_id, signal_counts in counts_by_user.items():
        if not signal_counts:
            continue

        def _score(item: tuple[str, int]) -> tuple[int, float, int]:
            signal_type, count = item
            latest = latest_by_user.get(user_id, {}).get(signal_type)
            latest_ts = latest.timestamp() if latest else 0.0
            return (count, latest_ts, _WORK_SIGNAL_PRIORITY.get(signal_type, 0))

        dominant_signal, dominant_count = max(signal_counts.items(), key=_score)
        rows.append(
            {
                "userId": user_id,
                "signalType": dominant_signal,
                "signalCount": dominant_count,
            }
        )
    rows.sort(key=lambda row: (-int(row.get("signalCount", 0)), str(row.get("signalType") or ""), str(row.get("userId") or "")))
    return rows


def _signal_rows_from_counts(counts_by_user: dict[str, Counter]) -> list[dict]:
    rows: list[dict] = []
    for user_id, signal_counts in counts_by_user.items():
        for signal_type, signal_count in signal_counts.items():
            if signal_type not in _WORK_SIGNAL_TYPES or signal_count <= 0:
                continue
            rows.append(
                {
                    "userId": user_id,
                    "signalType": signal_type,
                    "signalCount": int(signal_count),
                }
            )
    rows.sort(key=lambda row: (-int(row.get("signalCount", 0)), str(row.get("signalType") or ""), str(row.get("userId") or "")))
    return rows


def _window_for_timestamp(ts: datetime, ctx: DashboardDateContext) -> str | None:
    if ctx.start_at <= ts < ctx.end_at:
        return "current"
    if ctx.previous_start_at <= ts < ctx.previous_end_at:
        return "previous"
    return None


def _build_work_signal_rows(ctx: DashboardDateContext) -> dict[str, list[dict]]:
    rpc_rows = _fetch_work_signal_rows(ctx)
    if rpc_rows:
        current_rows = [
            {
                "userId": str(row.get("user_id") or ""),
                "signalType": str(row.get("signal_type") or ""),
                "signalCount": int(row.get("signal_count") or 0),
            }
            for row in rpc_rows
            if str(row.get("window_key") or "") == "current"
            and str(row.get("user_id") or "").strip()
            and str(row.get("signal_type") or "").strip() in _WORK_SIGNAL_TYPES
        ]
        previous_rows = [
            {
                "userId": str(row.get("user_id") or ""),
                "signalType": str(row.get("signal_type") or ""),
                "signalCount": int(row.get("signal_count") or 0),
            }
            for row in rpc_rows
            if str(row.get("window_key") or "") == "previous"
            and str(row.get("user_id") or "").strip()
            and str(row.get("signal_type") or "").strip() in _WORK_SIGNAL_TYPES
        ]
        current_rows.sort(key=lambda row: (-int(row.get("signalCount", 0)), str(row.get("signalType") or ""), str(row.get("userId") or "")))
        previous_rows.sort(key=lambda row: (-int(row.get("signalCount", 0)), str(row.get("signalType") or ""), str(row.get("userId") or "")))
    else:
        start_iso = ctx.previous_start_at.isoformat()
        end_iso = ctx.end_at.isoformat()
        analyses = _fetch_batch_analyses_between(start_iso, end_iso)
        if not analyses:
            return {"current": [], "previous": []}

        current_counts: dict[str, Counter] = defaultdict(Counter)
        current_latest: dict[str, dict[str, datetime]] = defaultdict(dict)
        previous_counts: dict[str, Counter] = defaultdict(Counter)
        previous_latest: dict[str, dict[str, datetime]] = defaultdict(dict)

        for row in analyses:
            user_id = str(row.get("telegram_user_id") or "").strip()
            signal_type = _extract_work_signal_type(row)
            signal_ts = _parse_iso_datetime(row.get("created_at"))
            if not user_id or not signal_type or signal_ts is None:
                continue

            window = _window_for_timestamp(signal_ts, ctx)
            if window == "current":
                bucket_counts = current_counts
                bucket_latest = current_latest
            elif window == "previous":
                bucket_counts = previous_counts
                bucket_latest = previous_latest
            else:
                continue

            bucket_counts[user_id][signal_type] += 1
            prev_latest = bucket_latest[user_id].get(signal_type)
            if prev_latest is None or signal_ts > prev_latest:
                bucket_latest[user_id][signal_type] = signal_ts

        current_rows = _signal_rows_from_counts(current_counts)
        previous_rows = _signal_rows_from_counts(previous_counts)

    return {"current": current_rows, "previous": previous_rows}


def _build_work_signal_snapshot(ctx: DashboardDateContext) -> dict:
    raw_rows = _build_work_signal_rows(ctx)
    current_rows = list(raw_rows.get("current") or [])
    previous_rows = list(raw_rows.get("previous") or [])
    current_by_type = Counter(
        str(row.get("signalType") or "")
        for row in current_rows
        if str(row.get("signalType") or "").strip()
    )
    previous_by_type = Counter(
        str(row.get("signalType") or "")
        for row in previous_rows
        if str(row.get("signalType") or "").strip()
    )

    trend_rows = [
        {
            "topic": signal_type,
            "currentUsers": int(current_by_type.get(signal_type, 0)),
            "previousUsers": int(previous_by_type.get(signal_type, 0)),
        }
        for signal_type in _WORK_SIGNAL_TYPES
        if current_by_type.get(signal_type, 0) > 0 or previous_by_type.get(signal_type, 0) > 0
    ]
    trend_rows.sort(key=lambda row: (-int(row["currentUsers"]), str(row["topic"])))
    return {
        "jobSeeking": current_rows,
        "jobTrends": trend_rows,
        "currentSignalRows": current_rows,
        "previousSignalRows": previous_rows,
    }


def _get_work_signal_snapshot(ctx: DashboardDateContext) -> dict:
    cached = _work_signal_snapshot_cache.get(ctx.cache_key)
    if cached is not None:
        return cached

    snapshot = _build_work_signal_snapshot(ctx)
    if len(_work_signal_snapshot_cache) >= 32:
        _work_signal_snapshot_cache.pop(next(iter(_work_signal_snapshot_cache)))
    _work_signal_snapshot_cache[ctx.cache_key] = snapshot
    return snapshot


def _get_work_signal_evidence(ctx: DashboardDateContext) -> dict[str, list[dict]]:
    rows = run_query("""
        UNWIND $signal_types AS signalType
        CALL {
            WITH signalType
            MATCH (u:User)-[:SIGNALS_OPPORTUNITY]->(:BusinessOpportunity {type: signalType})
            WHERE EXISTS {
                MATCH (u)-[i:INTERESTED_IN]->(:Topic)
                WHERE i.last_seen >= datetime($start)
                  AND i.last_seen < datetime($end)
            }
            OPTIONAL MATCH (u)-[:WROTE]->(c:Comment)-[:REPLIES_TO]->(:Post)-[:IN_CHANNEL]->(ch:Channel)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
            OPTIONAL MATCH (c)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
            WITH c, ch,
                 collect(DISTINCT CASE
                     WHEN t IS NULL THEN NULL
                     ELSE { name: t.name, category: cat.name }
                 END) AS taggedTopics
            WHERE c IS NULL
               OR size([topic IN taggedTopics WHERE topic IS NOT NULL AND topic.category IN $evidence_categories | topic.name]) > 0
               OR any(keyword IN $keywords WHERE toLower(c.text) CONTAINS keyword)
            WITH c, ch,
                 [topic IN taggedTopics WHERE topic IS NOT NULL AND topic.category IN $evidence_categories | topic.name] AS workTopics
            ORDER BY c.posted_at DESC
            RETURN collect(DISTINCT CASE
                WHEN c IS NULL THEN NULL
                ELSE {
                    id: coalesce(c.uuid, 'comment:' + elementId(c)),
                    text: left(replace(replace(trim(c.text), '\n', ' '), '\r', ' '), 220),
                    kind: 'comment',
                    channel: coalesce(ch.title, ch.username, 'Unknown channel'),
                    postedAt: toString(c.posted_at),
                    topic: coalesce(head(workTopics), 'Job Market Condition'),
                    sourceTopic: coalesce(head(workTopics), 'Job Market Condition')
                }
            END)[..3] AS commentEvidence
        }
        CALL {
            WITH signalType
            MATCH (p:Post)-[:TAGGED]->(t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(pch:Channel)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
              AND cat.name IN $evidence_categories
            WITH p, pch, collect(DISTINCT t.name) AS topicNames
            ORDER BY p.posted_at DESC
            RETURN collect(DISTINCT {
                id: coalesce(p.uuid, 'post:' + elementId(p)),
                text: left(replace(replace(trim(p.text), '\n', ' '), '\r', ' '), 220),
                kind: 'post',
                channel: coalesce(pch.title, pch.username, 'Unknown channel'),
                postedAt: toString(p.posted_at),
                topic: coalesce(head(topicNames), 'Job Market Condition'),
                sourceTopic: coalesce(head(topicNames), 'Job Market Condition')
            })[..2] AS postEvidence
        }
        WITH signalType,
             [item IN commentEvidence WHERE item IS NOT NULL] AS commentEvidence,
             [item IN postEvidence WHERE item IS NOT NULL] AS postEvidence
        RETURN signalType,
               CASE
                   WHEN size(commentEvidence) > 0 THEN commentEvidence
                   ELSE postEvidence
               END AS evidence
    """, {
        "signal_types": list(_WORK_SIGNAL_TYPES),
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "evidence_categories": list(_WORK_EVIDENCE_CATEGORIES),
        "keywords": list(_WORK_EVIDENCE_KEYWORDS),
    })
    return {
        str(row.get("signalType") or ""): list(row.get("evidence") or [])
        for row in rows
        if str(row.get("signalType") or "").strip()
    }


def _attach_work_signal_evidence(rows: list[dict], ctx: DashboardDateContext) -> list[dict]:
    if not rows:
        return rows
    try:
        evidence_by_signal = _get_work_signal_evidence(ctx)
    except Exception:
        evidence_by_signal = {}
    if not evidence_by_signal:
        return rows

    enriched: list[dict] = []
    for row in rows:
        signal_type = str(row.get("signalType") or "").strip()
        evidence = evidence_by_signal.get(signal_type, [])
        enriched.append({**row, "evidence": evidence})
    return enriched


def get_business_opportunities(ctx: DashboardDateContext) -> list[dict]:
    """Business opportunity signals among users active in the selected window."""
    return run_query("""
        MATCH (u:User)-[:SIGNALS_OPPORTUNITY]->(b:BusinessOpportunity)
        MATCH (u)-[i:INTERESTED_IN]->(t:Topic)
        WHERE b.type IS NOT NULL
          AND i.last_seen >= datetime($previous_start)
          AND i.last_seen < datetime($end)
        WITH b.type AS type,
             head(collect(DISTINCT b.description)) AS description,
             count(DISTINCT CASE
                 WHEN i.last_seen >= datetime($start)
                  AND i.last_seen < datetime($end)
                 THEN u
             END) AS signals,
             count(DISTINCT CASE
                 WHEN i.last_seen >= datetime($previous_start)
                  AND i.last_seen < datetime($previous_end)
                 THEN u
             END) AS previousSignals,
             collect(DISTINCT CASE
                 WHEN i.last_seen >= datetime($start)
                  AND i.last_seen < datetime($end)
                 THEN t.name
             END) AS relatedTopicPool
        WHERE signals > 0 OR previousSignals > 0
        RETURN type,
               description,
               signals,
               previousSignals,
               [topic IN relatedTopicPool WHERE topic IS NOT NULL][..5] AS relatedTopics
        ORDER BY signals DESC
    """, {
        "start": ctx.start_at.isoformat(),
        "end": ctx.end_at.isoformat(),
        "previous_start": ctx.previous_start_at.isoformat(),
        "previous_end": ctx.previous_end_at.isoformat(),
    })


def _brief_window_params(days: int, ctx: DashboardDateContext | None = None) -> dict[str, object]:
    if ctx is None:
        window_end = datetime.now(timezone.utc)
        window_start = window_end - timedelta(days=max(14, days))
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


def get_business_opportunity_brief_candidates(
    *,
    days: int = 30,
    ctx: DashboardDateContext | None = None,
    limit_topics: int = 16,
    evidence_per_topic: int = 14,
) -> list[dict]:
    """Demand-led opportunity bundles for AI-generated business opportunity briefs."""
    safe_days = max(14, min(int(days), 90))
    safe_limit_topics = max(6, min(int(limit_topics), 32))
    safe_evidence = max(6, min(int(evidence_per_topic), 24))

    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise
          AND NOT cat.name IN $excluded_categories

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
            WITH p, toLower(trim(p.text)) AS textLower
            WITH p,
                 CASE
                    WHEN p.text CONTAINS '?'
                      OR any(h IN $demand_hints WHERE textLower CONTAINS h)
                    THEN 1 ELSE 0
                 END AS askLike,
                 CASE
                    WHEN any(h IN $gap_hints WHERE textLower CONTAINS h)
                    THEN 1 ELSE 0
                 END AS gapHit
            RETURN
                count(CASE WHEN askLike = 1 OR gapHit = 1 THEN 1 END) AS postSignals,
                count(CASE WHEN (askLike = 1 OR gapHit = 1) AND p.posted_at >= datetime($current_start) AND p.posted_at < datetime($end) THEN 1 END) AS postSignals7d,
                count(CASE WHEN (askLike = 1 OR gapHit = 1) AND p.posted_at >= datetime($previous_start)
                            AND p.posted_at < datetime($previous_end) THEN 1 END) AS postSignalsPrev7d,
                max(CASE WHEN askLike = 1 OR gapHit = 1 THEN p.posted_at END) AS latestPostTs
        }

        CALL {
            WITH t
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (u)-[:EXHIBITS]->(intent:Intent)
            OPTIONAL MATCH (u)-[:SIGNALS_OPPORTUNITY]->(bo:BusinessOpportunity)
            WITH c, p, u,
                 max(CASE WHEN intent.name IN ['Support / Help', 'Information Seeking'] THEN 1 ELSE 0 END) AS supportIntent,
                 max(CASE WHEN bo.type IN ['Service_Demand', 'Product_Demand', 'Market_Gap_Observed', 'Business_Idea'] THEN 1 ELSE 0 END) AS opportunityHint,
                 max(CASE WHEN bo.type IN $excluded_opportunity_types THEN 1 ELSE 0 END) AS excludedOpportunityHint
            WITH c, p, u, supportIntent, opportunityHint, excludedOpportunityHint,
                 toLower(trim(c.text)) AS textLower,
                 toLower(trim(coalesce(p.text, ''))) AS contextLower
            WITH c,
                supportIntent,
                CASE
                    WHEN c.text CONTAINS '?'
                      OR any(h IN $demand_hints WHERE textLower CONTAINS h OR contextLower CONTAINS h)
                    THEN 1 ELSE 0
                END AS askLike,
                CASE
                    WHEN any(h IN $gap_hints WHERE textLower CONTAINS h OR contextLower CONTAINS h)
                    THEN 1 ELSE 0
                END AS gapHit,
                CASE
                    WHEN textLower CONTAINS 'recommend' OR textLower CONTAINS 'посовет'
                      OR textLower CONTAINS 'рекоменд'
                    THEN 1 ELSE 0
                END AS recommendationHit,
                CASE
                    WHEN excludedOpportunityHint = 1 THEN 0
                    ELSE opportunityHint
                END AS opportunityHint
            RETURN
                count(CASE
                    WHEN askLike = 1 OR gapHit = 1 OR supportIntent = 1 OR opportunityHint = 1
                    THEN 1 END) AS commentSignals,
                count(CASE
                    WHEN (askLike = 1 OR gapHit = 1 OR supportIntent = 1 OR opportunityHint = 1)
                      AND c.posted_at >= datetime($current_start)
                      AND c.posted_at < datetime($end)
                    THEN 1 END) AS commentSignals7d,
                count(CASE
                    WHEN (askLike = 1 OR gapHit = 1 OR supportIntent = 1 OR opportunityHint = 1)
                      AND c.posted_at >= datetime($previous_start)
                      AND c.posted_at < datetime($previous_end)
                    THEN 1 END) AS commentSignalsPrev7d,
                max(CASE
                    WHEN askLike = 1 OR gapHit = 1 OR supportIntent = 1 OR opportunityHint = 1
                    THEN c.posted_at END) AS latestCommentTs
        }

        WITH t, cat,
             (postSignals + commentSignals) AS demandSignals,
             (postSignals7d + commentSignals7d) AS signals7d,
             (postSignalsPrev7d + commentSignalsPrev7d) AS signalsPrev7d,
             CASE
                 WHEN latestPostTs IS NULL THEN latestCommentTs
                 WHEN latestCommentTs IS NULL THEN latestPostTs
                 WHEN latestPostTs >= latestCommentTs THEN latestPostTs
                 ELSE latestCommentTs
             END AS latestTs
        WHERE demandSignals >= 2
        WITH t, cat, demandSignals, signals7d, signalsPrev7d, latestTs,
             CASE
                WHEN (signals7d + signalsPrev7d) < 8 THEN 0
                ELSE toInteger(round(100.0 * (signals7d - signalsPrev7d) / (signalsPrev7d + 3)))
             END AS trend7dPct
        ORDER BY demandSignals DESC, latestTs DESC
        LIMIT $limit_topics

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND p.text IS NOT NULL
              AND trim(p.text) <> ''
            OPTIONAL MATCH (p)-[:IN_CHANNEL]->(ch:Channel)
            WITH p, ch, toLower(trim(p.text)) AS textLower
            WITH p, ch,
                 CASE
                    WHEN p.text CONTAINS '?'
                      OR any(h IN $demand_hints WHERE textLower CONTAINS h)
                    THEN 1 ELSE 0
                 END AS askLike,
                 CASE
                    WHEN any(h IN $gap_hints WHERE textLower CONTAINS h)
                    THEN 1 ELSE 0
                 END AS gapHit
            RETURN
                coalesce(p.uuid, 'post:' + elementId(p)) AS id,
                'post' AS kind,
                left(trim(p.text), 2600) AS text,
                '' AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                '' AS userId,
                toString(p.posted_at) AS timestamp,
                p.posted_at AS ts,
                askLike,
                gapHit,
                0 AS supportIntent,
                0 AS recommendationHit,
                0 AS opportunityHint
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND c.text IS NOT NULL
              AND trim(c.text) <> ''
            OPTIONAL MATCH (c)-[:REPLIES_TO]->(p:Post)-[:IN_CHANNEL]->(ch:Channel)
            OPTIONAL MATCH (u:User)-[:WROTE]->(c)
            OPTIONAL MATCH (u)-[:EXHIBITS]->(intent:Intent)
            OPTIONAL MATCH (u)-[:SIGNALS_OPPORTUNITY]->(bo:BusinessOpportunity)
            WITH c, p, u, ch,
                 max(CASE WHEN intent.name IN ['Support / Help', 'Information Seeking'] THEN 1 ELSE 0 END) AS supportIntent,
                 max(CASE WHEN bo.type IN ['Service_Demand', 'Product_Demand', 'Market_Gap_Observed', 'Business_Idea'] THEN 1 ELSE 0 END) AS opportunityHint,
                 max(CASE WHEN bo.type IN $excluded_opportunity_types THEN 1 ELSE 0 END) AS excludedOpportunityHint,
                 toLower(trim(c.text)) AS textLower,
                 toLower(trim(coalesce(p.text, ''))) AS contextLower
            WITH c, p, u, ch, supportIntent, opportunityHint, excludedOpportunityHint,
                 CASE
                    WHEN c.text CONTAINS '?'
                      OR any(h IN $demand_hints WHERE textLower CONTAINS h OR contextLower CONTAINS h)
                    THEN 1 ELSE 0
                 END AS askLike,
                 CASE
                    WHEN any(h IN $gap_hints WHERE textLower CONTAINS h OR contextLower CONTAINS h)
                    THEN 1 ELSE 0
                 END AS gapHit,
                 CASE
                    WHEN textLower CONTAINS 'recommend' OR textLower CONTAINS 'посовет'
                      OR textLower CONTAINS 'рекоменд'
                    THEN 1 ELSE 0
                 END AS recommendationHit
            RETURN
                coalesce(c.uuid, 'comment:' + elementId(c)) AS id,
                'comment' AS kind,
                left(trim(c.text), 2600) AS text,
                left(coalesce(p.text, ''), 1200) AS parentText,
                coalesce(ch.title, ch.username, 'unknown') AS channel,
                coalesce(toString(u.telegram_user_id), '') AS userId,
                toString(c.posted_at) AS timestamp,
                c.posted_at AS ts,
                askLike,
                gapHit,
                supportIntent,
                recommendationHit,
                CASE
                    WHEN excludedOpportunityHint = 1 THEN 0
                    ELSE opportunityHint
                END AS opportunityHint
        }
        WITH t, cat, demandSignals, signals7d, signalsPrev7d, trend7dPct, latestTs,
             collect({
                id: id,
                kind: kind,
                text: text,
                parentText: parentText,
                channel: channel,
                userId: userId,
                timestamp: timestamp,
                askLike: askLike,
                gapHit: gapHit,
                supportIntent: supportIntent,
                recommendationHit: recommendationHit,
                opportunityHint: opportunityHint,
                ts: ts
             }) AS rows
        UNWIND rows AS row
        WITH t, cat, demandSignals, signals7d, signalsPrev7d, trend7dPct, latestTs, row
        WHERE row.askLike = 1 OR row.gapHit = 1 OR row.supportIntent = 1 OR row.opportunityHint = 1
        ORDER BY row.ts DESC
        WITH t, cat, demandSignals, signals7d, signalsPrev7d, trend7dPct, latestTs,
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
                askLike: row.askLike,
                gapHit: row.gapHit,
                supportIntent: row.supportIntent,
                recommendationHit: row.recommendationHit,
                opportunityHint: row.opportunityHint
             })[..$evidence_per_topic] AS evidence
        RETURN
            t.name AS topic,
            cat.name AS category,
            demandSignals AS signalCount,
            uniqueUsers,
            channelCount,
            signals7d,
            signalsPrev7d,
            trend7dPct,
            toString(latestTs) AS latestAt,
            evidence
        ORDER BY signalCount DESC, uniqueUsers DESC, latestAt DESC
    """, {
        "limit_topics": safe_limit_topics,
        "evidence_per_topic": safe_evidence,
        "noise": _NOISY_TOPIC_KEYS,
        "excluded_categories": _EXCLUDED_CATEGORIES,
        "demand_hints": _DEMAND_HINTS,
        "gap_hints": _GAP_HINTS,
        "excluded_opportunity_types": _EXCLUDED_OPPORTUNITY_TYPES,
        **_brief_window_params(safe_days, ctx),
    })


def get_job_seeking(ctx: DashboardDateContext) -> list[dict]:
    """Current-window work-intent rows with real graph evidence."""
    base_rows = list(_get_work_signal_snapshot(ctx).get("currentSignalRows") or [])
    if not base_rows:
        return []
    user_ids = sorted({str(row.get("userId") or "").strip() for row in base_rows if str(row.get("userId") or "").strip()})
    user_profiles = {}
    if user_ids:
        profile_rows = run_query("""
            UNWIND $user_ids AS userId
            MATCH (u:User)
            WHERE toString(u.telegram_user_id) = userId
            RETURN userId,
                   u.inferred_age_bracket AS age,
                   u.community_role AS role,
                   u.financial_distress_level AS distress
        """, {"user_ids": user_ids})
        user_profiles = {
            str(row.get("userId") or "").strip(): {
                "age": row.get("age"),
                "role": row.get("role"),
                "distress": row.get("distress"),
            }
            for row in profile_rows
            if str(row.get("userId") or "").strip()
        }
    enriched_rows = []
    for row in base_rows:
        profile = user_profiles.get(str(row.get("userId") or "").strip(), {})
        enriched_rows.append({**row, **profile})
    return _attach_work_signal_evidence(enriched_rows, ctx)


def get_job_trends(ctx: DashboardDateContext) -> list[dict]:
    """Selected-window work-intent trends from the graph."""
    return list(_get_work_signal_snapshot(ctx).get("jobTrends") or [])


def get_housing_data() -> list[dict]:
    """Housing-related topics and user interest."""
    return run_query("""
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(cat:TopicCategory)
        WHERE cat.name = 'Economy' AND t.name IN ['Housing Market', 'Investment Opportunity']
        OPTIONAL MATCH (u:User)-[i:INTERESTED_IN]->(t)
        OPTIONAL MATCH (p:Post)-[:TAGGED]->(t)
        WITH t.name AS topic, count(DISTINCT u) AS interestedUsers,
             count(DISTINCT p) AS posts,
             sum(i.count) AS interactions
        RETURN topic, interestedUsers, posts, interactions
    """)
