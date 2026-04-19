from __future__ import annotations

import time
from datetime import date
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from loguru import logger

import config
from api import aggregator
from api.db import run_query
from api.dashboard_dates import DashboardDateContext, build_dashboard_date_context
from api.dashboard_v2_assembler import DashboardV2FactsNotReadyError, assemble_dashboard_v2_exact
from api.dashboard_v2_registry import (
    ALL_WIDGET_IDS,
    DIRECT_SOURCE_TRUTH_WIDGET_IDS,
    RAW_SNAPSHOT_FIELDS_BY_WIDGET_ID,
)
from api.dashboard_v2_store import DashboardV2Store
from api.queries import comparative, pulse, strategic

SOURCE_TRUTH_VALIDATION_MODE = "source_truth"
FACT_INVARIANT_VALIDATION_MODE = "fact_invariant"

BLOCKING_REASON_SOURCE_TRUTH_ERROR = "source_truth_validator_error"
BLOCKING_REASON_SOURCE_TRUTH_TIMEOUT = "source_truth_validator_timeout"
BLOCKING_REASON_SOURCE_TRUTH_MISMATCH = "source_truth_mismatch"
BLOCKING_REASON_FACT_INVARIANT_FAILED = "fact_invariant_failed"
BLOCKING_REASON_V2_FACTS_NOT_READY = "v2_facts_not_ready"
WARNING_REASON_OLD_PATH_UNAVAILABLE = "old_path_unavailable"
WARNING_REASON_OLD_PATH_PRESENCE_MISMATCH = "old_path_presence_mismatch"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _direct_truth_builders() -> dict[str, Callable[[DashboardDateContext], Any]]:
    return {
        "community_brief": pulse.get_community_brief,
        "community_health_score": pulse.get_community_health,
        "trending_topics_feed": pulse.get_trending_topics,
        "conversation_trends": strategic.get_trend_lines,
        "topic_lifecycle": strategic.get_lifecycle_stages,
        "sentiment_by_topic": comparative.get_sentiment_by_topic,
        "week_over_week_shifts": comparative.get_weekly_shifts,
    }


def _source_truth_summary_builders() -> dict[str, Callable[[DashboardDateContext, dict[str, Any]], dict[str, Any]]]:
    return {
        "community_brief": _community_brief_source_summary,
        "community_health_score": _community_health_source_summary,
        "trending_topics_feed": _trending_topics_source_summary,
        "conversation_trends": _conversation_trends_source_summary,
        "topic_lifecycle": _topic_lifecycle_source_summary,
        "sentiment_by_topic": _sentiment_by_topic_source_summary,
        "week_over_week_shifts": _week_over_week_source_summary,
    }


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _extract_snapshot_widget(snapshot: dict[str, Any], widget_id: str) -> Any:
    if not isinstance(snapshot, dict):
        return None
    field_names = RAW_SNAPSHOT_FIELDS_BY_WIDGET_ID.get(widget_id) or ()
    if not field_names:
        return None
    if len(field_names) == 1:
        return snapshot.get(field_names[0])
    return {
        field_name: snapshot.get(field_name)
        for field_name in field_names
        if field_name in snapshot
    }


def _non_empty_fields(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return []
    fields: list[str] = []
    for key, value in payload.items():
        if value not in (None, "", [], {}):
            fields.append(str(key))
    return sorted(fields)


def _extract_top_items(payload: Any) -> list[str]:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        for candidate_key in (
            "trendingTopics",
            "topics",
            "questionCategories",
            "trendLines",
            "weeklyShifts",
            "sentimentByTopic",
            "lifecycleStages",
        ):
            candidate = payload.get(candidate_key)
            if isinstance(candidate, list):
                items = candidate
                break
        else:
            return []
    else:
        return []
    results: list[str] = []
    for item in items[:5]:
        if isinstance(item, dict):
            for key in ("topic", "name", "metricKey", "label", "stage", "category", "item"):
                text = str(item.get(key) or "").strip()
                if text:
                    results.append(text)
                    break
        elif item not in (None, ""):
            results.append(str(item))
    return results


def _count_payload_items(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        if all(value in (None, "", [], {}) for value in payload.values()):
            return 0
        if any(isinstance(value, list) for value in payload.values()):
            return sum(len(value) for value in payload.values() if isinstance(value, list))
        return len(payload)
    return 1 if payload not in (None, "", [], {}) else 0


def _summarize_widget(widget_id: str, payload: Any) -> dict[str, Any]:
    if payload in (None, "", [], {}):
        return {
            "present": False,
            "itemCount": 0,
            "nonEmptyFields": [],
            "topItems": [],
        }
    summary = {
        "present": True,
        "itemCount": _count_payload_items(payload),
        "nonEmptyFields": _non_empty_fields(payload),
        "topItems": _extract_top_items(payload),
    }
    if widget_id == "community_brief" and isinstance(payload, dict):
        summary.update(
            {
                "messagesAnalyzed": payload.get("messagesAnalyzed"),
                "postsAnalyzedInWindow": payload.get("postsAnalyzedInWindow", payload.get("postsAnalyzed24h")),
                "commentScopesAnalyzedInWindow": payload.get(
                    "commentScopesAnalyzedInWindow",
                    payload.get("commentScopesAnalyzed24h"),
                ),
                "totalAnalysesInWindow": payload.get("totalAnalysesInWindow", payload.get("totalAnalyses24h")),
            }
        )
    elif widget_id == "community_health_score" and isinstance(payload, dict):
        components = payload.get("components")
        summary.update(
            {
                "currentScore": payload.get("currentScore", payload.get("score")),
                "componentCount": len(components or [])
                if isinstance(components, list)
                else len((components or {}).get("en", []))
                if isinstance(components, dict)
                else 0,
            }
        )
    return summary


def _summary_from_topics(*, field_name: str, topic_names: list[str]) -> dict[str, Any]:
    cleaned = [str(name).strip() for name in topic_names if str(name).strip()]
    if not cleaned:
        return _summarize_widget(field_name, None)
    return {
        "present": True,
        "itemCount": len(cleaned),
        "nonEmptyFields": [field_name],
        "topItems": cleaned[:5],
    }


def _trend_line_topic_rows(ctx: DashboardDateContext, cache: dict[str, Any]) -> list[dict[str, Any]]:
    cached = cache.get("trend_line_topic_rows")
    if isinstance(cached, list):
        return cached
    rows = strategic.get_trend_lines(ctx)
    topic_rows: dict[str, dict[str, Any]] = {}
    for row in rows:
        topic = str(row.get("topic") or "").strip()
        bucket_text = str(row.get("bucket") or "").strip()
        if not topic or not bucket_text:
            continue
        try:
            bucket_day = date.fromisoformat(bucket_text)
        except Exception:
            continue
        mentions = int(row.get("posts") or row.get("mentions") or 0)
        current = topic_rows.setdefault(
            topic,
            {
                "topic": topic,
                "totalMentions": 0,
                "points": [],
                "firstSeen": bucket_day,
                "lastSeen": bucket_day,
            },
        )
        current["totalMentions"] += mentions
        current["points"].append({"bucket": bucket_day, "mentions": mentions})
        if bucket_day < current["firstSeen"]:
            current["firstSeen"] = bucket_day
        if bucket_day > current["lastSeen"]:
            current["lastSeen"] = bucket_day
    ordered = [topic_rows[key] for key in sorted(topic_rows)]
    cache["trend_line_topic_rows"] = ordered
    return ordered


def _simple_topic_rank_rows(ctx: DashboardDateContext, cache: dict[str, Any]) -> list[dict[str, Any]]:
    cached = cache.get("simple_topic_rank_rows")
    if isinstance(cached, list):
        return cached
    rows = run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND t.name IN $canonical_topics
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN count(DISTINCT coalesce(p.uuid, 'post:' + elementId(p))) AS hits
            UNION ALL
            WITH t
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN count(DISTINCT coalesce(c.uuid, 'comment:' + elementId(c))) AS hits
        }
        WITH t.name AS topic, sum(hits) AS mentions
        WHERE mentions > 0
        RETURN topic, toInteger(mentions) AS mentions
        ORDER BY mentions DESC, topic ASC
        LIMIT 25
        """,
        {
            "canonical_topics": list(pulse._CANONICAL_TOPIC_NAMES),
            "noise": list(pulse._NOISY_TOPIC_NAMES),
            "start": ctx.start_at.isoformat(),
            "end": ctx.end_at.isoformat(),
        },
    )
    normalized = [
        {
            "topic": str(row.get("topic") or "").strip(),
            "mentions": int(row.get("mentions") or 0),
        }
        for row in rows
        if str(row.get("topic") or "").strip()
    ]
    cache["simple_topic_rank_rows"] = normalized
    return normalized


def _topic_lifecycle_rank_rows(ctx: DashboardDateContext, cache: dict[str, Any]) -> list[dict[str, Any]]:
    cached = cache.get("topic_lifecycle_rank_rows")
    if isinstance(cached, list):
        return cached
    rows = run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(:TopicCategory)
        WHERE coalesce(t.proposed, false) = false
          AND t.name IN $canonical_topics
          AND NOT toLower(trim(coalesce(t.name, ''))) IN $noise

        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
            RETURN count(DISTINCT coalesce(p.uuid, 'post:' + elementId(p))) AS currentHits
            UNION ALL
            WITH t
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
            RETURN count(DISTINCT coalesce(c.uuid, 'comment:' + elementId(c))) AS currentHits
        }
        CALL {
            WITH t
            MATCH (p:Post)-[:TAGGED]->(t)
            WHERE p.posted_at >= datetime($previous_start)
              AND p.posted_at < datetime($previous_end)
            RETURN count(DISTINCT coalesce(p.uuid, 'post:' + elementId(p))) AS previousHits
            UNION ALL
            WITH t
            MATCH (c:Comment)-[:TAGGED]->(t)
            WHERE c.posted_at >= datetime($previous_start)
              AND c.posted_at < datetime($previous_end)
            RETURN count(DISTINCT coalesce(c.uuid, 'comment:' + elementId(c))) AS previousHits
        }
        WITH t.name AS topic, sum(currentHits) AS currentMentions, sum(previousHits) AS previousMentions
        RETURN topic, toInteger(currentMentions) AS currentMentions, toInteger(previousMentions) AS previousMentions
        """,
        {
            "canonical_topics": list(pulse._CANONICAL_TOPIC_NAMES),
            "noise": list(pulse._NOISY_TOPIC_NAMES),
            "start": ctx.start_at.isoformat(),
            "end": ctx.end_at.isoformat(),
            "previous_start": ctx.previous_start_at.isoformat(),
            "previous_end": ctx.previous_end_at.isoformat(),
        },
    )
    ranked: list[dict[str, Any]] = []
    for row in rows:
        topic = str(row.get("topic") or "").strip()
        if not topic:
            continue
        current_mentions = int(row.get("currentMentions") or 0)
        previous_mentions = int(row.get("previousMentions") or 0)
        if current_mentions <= 0 and previous_mentions <= 0:
            continue
        stage_confidence = round(
            100.0
            * (
                0.7 * (1.0 if current_mentions >= 40 else float(current_mentions) / 40.0)
                + 0.3
                * (
                    1.0
                    if abs(current_mentions - previous_mentions) >= 12
                    else abs(float(current_mentions - previous_mentions)) / 12.0
                )
            ),
            1,
        )
        stage = (
            "declining"
            if current_mentions < 4
            else "growing"
            if (
                current_mentions >= previous_mentions
                and (
                    round(100.0 * (float(current_mentions - previous_mentions) / float(previous_mentions + 3)), 1) >= 10
                    or (current_mentions - previous_mentions) >= 3
                    or (previous_mentions == 0 and current_mentions >= 4)
                )
            )
            else "declining"
        )
        ranked.append(
            {
                "topic": topic,
                "stage": stage,
                "weeklyCurrent": current_mentions,
                "stageConfidence": stage_confidence,
            }
        )
    ranked.sort(
        key=lambda item: (
            1 if item["stage"] == "growing" else 2,
            -int(item["weeklyCurrent"]),
            -float(item["stageConfidence"]),
            str(item["topic"]),
        )
    )
    cache["topic_lifecycle_rank_rows"] = ranked
    return ranked


def _topic_window_rows(ctx: DashboardDateContext, cache: dict[str, Any]) -> list[dict[str, Any]]:
    cached = cache.get("topic_window_rows")
    if isinstance(cached, list):
        return cached
    rows = pulse._query_topic_widget_rows(ctx, evidence_limit=1)
    cache["topic_window_rows"] = rows
    return rows


def _community_brief_source_summary(ctx: DashboardDateContext, cache: dict[str, Any]) -> dict[str, Any]:
    payload = pulse._community_brief_from_source_scope(
        ctx,
        top_topic_rows=[
            {"sourceTopic": row.get("topic"), "name": row.get("topic")}
            for row in _simple_topic_rank_rows(ctx, cache)[:5]
        ],
    )
    return _summarize_widget("community_brief", payload)


def _community_health_source_summary(ctx: DashboardDateContext, cache: dict[str, Any]) -> dict[str, Any]:
    health_inputs = cache.get("community_health_inputs")
    if not isinstance(health_inputs, dict):
        health_inputs = pulse._health_inputs(ctx)
        cache["community_health_inputs"] = health_inputs
    payload = pulse._community_health_from_inputs(
        ctx,
        current_rows=list(health_inputs.get("current_rows") or []),
        previous_rows=list(health_inputs.get("previous_rows") or []),
        current_diversity=dict(health_inputs.get("current_diversity") or {}),
        previous_diversity=dict(health_inputs.get("previous_diversity") or {}),
    )
    return _summarize_widget("community_health_score", payload)


def _trending_topics_source_summary(ctx: DashboardDateContext, cache: dict[str, Any]) -> dict[str, Any]:
    rows = _topic_window_rows(ctx, cache)
    eligible = [
        row
        for row in rows
        if int(row.get("currentMentions") or 0) >= pulse._TRENDING_MIN_MENTIONS
        and int(row.get("evidenceCount") or 0) >= pulse._TRENDING_MIN_EVIDENCE
        and int(row.get("distinctChannels") or 0) >= pulse._TRENDING_MIN_CHANNELS
        and int(row.get("distinctUsers") or 0) >= pulse._TRENDING_MIN_USERS
    ]
    eligible.sort(
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
    topic_names = [str(row.get("sourceTopic") or row.get("name") or "").strip() for row in eligible]
    return _summary_from_topics(field_name="trendingTopics", topic_names=topic_names)


def _conversation_trends_source_summary(ctx: DashboardDateContext, cache: dict[str, Any]) -> dict[str, Any]:
    topic_rows = _simple_topic_rank_rows(ctx, cache)
    return _summary_from_topics(
        field_name="trendLines",
        topic_names=[str(row.get("topic") or "").strip() for row in topic_rows],
    )


def _topic_lifecycle_source_summary(ctx: DashboardDateContext, cache: dict[str, Any]) -> dict[str, Any]:
    ranked = _topic_lifecycle_rank_rows(ctx, cache)
    return _summary_from_topics(
        field_name="lifecycleStages",
        topic_names=[str(item.get("topic") or "").strip() for item in ranked],
    )


def _sentiment_by_topic_source_summary(ctx: DashboardDateContext, cache: dict[str, Any]) -> dict[str, Any]:
    cached = cache.get("sentiment_topic_summary")
    if isinstance(cached, dict):
        return cached
    rows = run_query(
        """
        CALL () {
            MATCH (p:Post)-[:TAGGED]->(t:Topic)
            MATCH (p)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE p.posted_at >= datetime($start)
              AND p.posted_at < datetime($end)
              AND t.name IN $canonical_topics
            RETURN trim(coalesce(t.name, '')) AS topic,
                   count(DISTINCT p) AS score
            UNION ALL
            MATCH (c:Comment)-[:TAGGED]->(t:Topic)
            MATCH (c)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE c.posted_at >= datetime($start)
              AND c.posted_at < datetime($end)
              AND t.name IN $canonical_topics
            RETURN trim(coalesce(t.name, '')) AS topic,
                   count(DISTINCT c) AS score
        }
        WITH topic, sum(score) AS score
        WHERE topic <> ''
        RETURN topic, toInteger(sum(score)) AS count
        ORDER BY count DESC, topic ASC
        LIMIT 15
        """,
        {
            "start": ctx.start_at.isoformat(),
            "end": ctx.end_at.isoformat(),
            "canonical_topics": list(pulse._CANONICAL_TOPIC_NAMES),
        },
    )
    summary = _summary_from_topics(
        field_name="sentimentByTopic",
        topic_names=[str(row.get("topic") or "").strip() for row in rows],
    )
    cache["sentiment_topic_summary"] = summary
    return summary


def _week_over_week_source_summary(ctx: DashboardDateContext, cache: dict[str, Any]) -> dict[str, Any]:
    cached = cache.get("week_over_week_summary")
    if isinstance(cached, dict):
        return cached
    metric_keys = [
        str(item.get("metricKey") or "").strip()
        for item in comparative.get_weekly_shifts(ctx)
        if str(item.get("metricKey") or "").strip()
    ]
    summary = {
        "present": bool(metric_keys),
        "itemCount": len(metric_keys),
        "nonEmptyFields": ["weeklyShifts"] if metric_keys else [],
        "topItems": metric_keys[:5],
    }
    cache["week_over_week_summary"] = summary
    return summary


def _present_widget_count(widget_diffs: dict[str, Any], key: str) -> int:
    return sum(1 for diff in widget_diffs.values() if bool(_as_dict(diff.get(key)).get("present")))


def build_direct_truth_snapshot(ctx: DashboardDateContext) -> dict[str, Any]:
    builders = _direct_truth_builders()
    return {widget_id: builders[widget_id](ctx) for widget_id in DIRECT_SOURCE_TRUTH_WIDGET_IDS}


def _error_text(exc: Exception) -> str:
    text = str(exc).strip() or exc.__class__.__name__
    return text[:240]


def _execute_with_timeout(
    *,
    label: str,
    timeout_seconds: float,
    fn: Callable[[], Any],
) -> dict[str, Any]:
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"dashboard-v2-compare-{label}")
    future = executor.submit(fn)
    try:
        return {"status": "ok", "value": future.result(timeout=max(0.1, float(timeout_seconds)))}
    except FutureTimeoutError:
        future.cancel()
        logger.warning("Dashboard V2 compare timeout | label={} timeout={}s", label, timeout_seconds)
        return {
            "status": "timeout",
            "error": f"{label} timed out after {timeout_seconds:.1f}s",
        }
    except Exception as exc:
        logger.warning("Dashboard V2 compare execution failed | label={} error={}", label, exc)
        return {"status": "error", "error": _error_text(exc)}
    finally:
        executor.shutdown(wait=False)


def _new_validation(mode: str) -> dict[str, Any]:
    return {
        "mode": mode,
        "semanticStatus": "skipped",
        "regressionStatus": "skipped",
        "blockingReasons": [],
        "warnings": [],
    }


def _summaries_match_for_source_truth(widget_id: str, v2_summary: dict[str, Any], direct_summary: dict[str, Any]) -> bool:
    if bool(v2_summary.get("present")) != bool(direct_summary.get("present")):
        return False
    if widget_id == "community_brief":
        for key in (
            "messagesAnalyzed",
            "postsAnalyzedInWindow",
            "commentScopesAnalyzedInWindow",
            "totalAnalysesInWindow",
        ):
            if v2_summary.get(key) != direct_summary.get(key):
                return False
        return True
    if widget_id == "community_health_score":
        for key in ("currentScore", "componentCount"):
            if v2_summary.get(key) != direct_summary.get(key):
                return False
        return True
    if widget_id in {"trending_topics_feed", "conversation_trends", "topic_lifecycle", "sentiment_by_topic", "week_over_week_shifts"}:
        direct_top_items = list(direct_summary.get("topItems") or [])
        if not direct_top_items:
            return bool(v2_summary.get("present")) == bool(direct_summary.get("present"))
        return list(v2_summary.get("topItems") or []) == direct_top_items
    if v2_summary.get("itemCount") != direct_summary.get("itemCount"):
        return False
    direct_top_items = list(direct_summary.get("topItems") or [])
    if direct_top_items and list(v2_summary.get("topItems") or []) != direct_top_items:
        return False
    return True


def _validate_source_truth_widget(
    *,
    widget_id: str,
    ctx: DashboardDateContext,
    v2_summary: dict[str, Any],
    timeout_seconds: float,
    shared_cache: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    validation = _new_validation(SOURCE_TRUTH_VALIDATION_MODE)
    if timeout_seconds <= 0:
        validation["semanticStatus"] = "fail"
        validation["blockingReasons"] = [BLOCKING_REASON_SOURCE_TRUTH_TIMEOUT]
        validation["error"] = "source-truth validation budget exhausted before execution"
        return _summarize_widget(widget_id, None), validation
    builder = _source_truth_summary_builders()[widget_id]
    result = _execute_with_timeout(
        label=f"source-truth-{widget_id}",
        timeout_seconds=float(timeout_seconds),
        fn=lambda: builder(ctx, shared_cache),
    )
    if result["status"] == "timeout":
        validation["semanticStatus"] = "fail"
        validation["blockingReasons"] = [BLOCKING_REASON_SOURCE_TRUTH_TIMEOUT]
        validation["error"] = result["error"]
        return _summarize_widget(widget_id, None), validation
    if result["status"] == "error":
        validation["semanticStatus"] = "fail"
        validation["blockingReasons"] = [BLOCKING_REASON_SOURCE_TRUTH_ERROR]
        validation["error"] = result["error"]
        return _summarize_widget(widget_id, None), validation

    summary = _as_dict(result["value"])
    validation["semanticStatus"] = "pass"
    if not _summaries_match_for_source_truth(widget_id, v2_summary, summary):
        validation["semanticStatus"] = "fail"
        validation["blockingReasons"] = [BLOCKING_REASON_SOURCE_TRUTH_MISMATCH]
    return summary, validation


def _validate_fact_invariant_widget(
    *,
    widget_id: str,
    raw_payload: Any,
    v2_summary: dict[str, Any],
    exact_readiness: dict[str, Any],
) -> dict[str, Any]:
    validation = _new_validation(FACT_INVARIANT_VALIDATION_MODE)
    blocking_reasons: list[str] = []
    if not bool(exact_readiness.get("v2RouteReady")):
        blocking_reasons.append(BLOCKING_REASON_FACT_INVARIANT_FAILED)
        validation["readiness"] = {
            "missingFamilies": list(exact_readiness.get("missingFamilies") or []),
            "missingDates": list(exact_readiness.get("missingDates") or []),
            "degradedFamilies": list(exact_readiness.get("degradedFamilies") or []),
            "degradedDates": list(exact_readiness.get("degradedDates") or []),
        }
    if raw_payload is None:
        blocking_reasons.append(BLOCKING_REASON_FACT_INVARIANT_FAILED)
    if int(v2_summary.get("itemCount") or 0) < len(v2_summary.get("topItems") or []):
        blocking_reasons.append(BLOCKING_REASON_FACT_INVARIANT_FAILED)

    validation["blockingReasons"] = sorted(set(blocking_reasons))
    validation["semanticStatus"] = "pass" if not validation["blockingReasons"] else "fail"
    return validation


def _build_regression_validation(
    *,
    old_path_available: bool,
    old_payload: Any,
    old_summary: dict[str, Any],
    v2_summary: dict[str, Any],
) -> tuple[str, list[str]]:
    if not old_path_available:
        return "unavailable", [WARNING_REASON_OLD_PATH_UNAVAILABLE]
    warnings: list[str] = []
    if bool(old_summary.get("present")) != bool(v2_summary.get("present")):
        warnings.append(WARNING_REASON_OLD_PATH_PRESENCE_MISMATCH)
    return ("warning" if warnings else "pass"), warnings


def _build_validation_summary(
    *,
    widget_diffs: dict[str, Any],
    old_path_status: str,
    v2_status: str,
) -> dict[str, Any]:
    blocking_widgets: list[dict[str, Any]] = []
    regression_warning_widgets: list[dict[str, Any]] = []
    source_truth_count = 0
    fact_invariant_count = 0
    semantic_pass_count = 0
    semantic_fail_count = 0

    for widget_id, diff in widget_diffs.items():
        validation = _as_dict(diff.get("validation"))
        mode = str(validation.get("mode") or "")
        if mode == SOURCE_TRUTH_VALIDATION_MODE:
            source_truth_count += 1
        elif mode == FACT_INVARIANT_VALIDATION_MODE:
            fact_invariant_count += 1
        semantic_status = str(validation.get("semanticStatus") or "")
        if semantic_status == "pass":
            semantic_pass_count += 1
        elif semantic_status == "fail":
            semantic_fail_count += 1
            blocking_widgets.append(
                {
                    "widgetId": widget_id,
                    "mode": mode,
                    "reasons": list(validation.get("blockingReasons") or []),
                    "error": validation.get("error"),
                }
            )
        regression_status = str(validation.get("regressionStatus") or "")
        warnings = list(validation.get("warnings") or [])
        if regression_status in {"warning", "unavailable"} or warnings:
            regression_warning_widgets.append(
                {
                    "widgetId": widget_id,
                    "status": regression_status,
                    "warnings": warnings,
                }
            )

    top_level_blocking_reasons: list[str] = []
    if v2_status != "ready":
        top_level_blocking_reasons.append(BLOCKING_REASON_V2_FACTS_NOT_READY)
    semantic_gate_ready = v2_status == "ready" and not blocking_widgets and not top_level_blocking_reasons
    regression_status = "unavailable" if old_path_status == "unavailable" else "warning" if regression_warning_widgets else "ready"
    return {
        "validationModel": "source_truth_primary",
        "semanticStatus": "ready" if semantic_gate_ready else "failed",
        "semanticGateReady": bool(semantic_gate_ready),
        "regressionStatus": regression_status,
        "blockingReasons": top_level_blocking_reasons,
        "blockingWidgets": blocking_widgets,
        "regressionWarnings": regression_warning_widgets,
        "sourceTruthWidgetCount": source_truth_count,
        "factInvariantWidgetCount": fact_invariant_count,
        "semanticPassCount": semantic_pass_count,
        "semanticFailCount": semantic_fail_count,
    }


def run_dashboard_v2_compare(
    store: DashboardV2Store,
    *,
    from_value: str,
    to_value: str,
) -> dict[str, Any]:
    ctx = build_dashboard_date_context(from_value, to_value)

    v2_status = "ready"
    v2_meta: dict[str, Any]
    v2_snapshot: dict[str, Any] = {}
    exact_readiness = store.summarize_v2_route_readiness(
        min_fact_version=2,
        from_date=ctx.from_date,
        to_date=ctx.to_date,
    )
    try:
        v2_result = assemble_dashboard_v2_exact(
            store,
            ctx=ctx,
            allow_stale_exact_last_known_good=False,
            # Compare should reuse a fresh exact artifact when one already exists.
            # That keeps larger-window validation operationally bounded without
            # falling back to stale same-key last-known-good artifacts.
            prefer_cached_exact_artifacts=True,
        )
        v2_snapshot = dict(v2_result.snapshot or {})
        v2_meta = {
            "status": "ready",
            "cacheKey": ctx.cache_key,
            "cacheStatus": v2_result.cache_status,
            "cacheSource": v2_result.cache_source,
            "rangeResolutionPath": v2_result.range_resolution_path,
            "factVersion": v2_result.fact_version,
            "artifactVersion": v2_result.artifact_version,
            "factWatermark": v2_result.fact_watermark,
            "materializedAt": v2_result.materialized_at,
            "staleFactFamilies": list(v2_result.stale_fact_families or []),
            "readiness": exact_readiness,
        }
    except DashboardV2FactsNotReadyError as exc:
        v2_status = "facts_not_ready"
        v2_meta = {
            "status": v2_status,
            "cacheKey": ctx.cache_key,
            "detail": exc.detail,
            "readiness": exact_readiness,
        }

    old_snapshot: dict[str, Any] = {}
    old_path_status = "skipped"
    old_path_meta = {
        "generatedAt": _utc_now_iso(),
        "from": ctx.from_date.isoformat(),
        "to": ctx.to_date.isoformat(),
        "status": old_path_status,
        "widgetCount": 0,
    }
    if v2_status == "ready":
        old_result = _execute_with_timeout(
            label="old-path",
            timeout_seconds=float(config.DASH_V2_COMPARE_OLD_PATH_TIMEOUT_SECONDS),
            fn=lambda: aggregator.get_dashboard_data(ctx),
        )
        if old_result["status"] == "ok":
            old_snapshot = dict(old_result["value"] or {})
            old_path_status = "ready"
            old_path_meta["status"] = old_path_status
        else:
            old_path_status = "unavailable"
            old_path_meta.update(
                {
                    "status": old_path_status,
                    "warning": WARNING_REASON_OLD_PATH_UNAVAILABLE,
                    "error": old_result.get("error"),
                }
            )

    widget_diffs: dict[str, Any] = {}
    direct_truth_failures = 0
    source_truth_budget_seconds = float(config.DASH_V2_COMPARE_SOURCE_TRUTH_TOTAL_TIMEOUT_SECONDS)
    source_truth_deadline = time.monotonic() + max(0.0, source_truth_budget_seconds)
    source_truth_cache: dict[str, Any] = {}
    for widget_id in ALL_WIDGET_IDS:
        raw_v2_payload = _extract_snapshot_widget(v2_snapshot, widget_id)
        v2_payload_summary = _summarize_widget(widget_id, raw_v2_payload)
        old_payload = _extract_snapshot_widget(old_snapshot, widget_id)
        old_payload_summary = _summarize_widget(widget_id, old_payload)
        widget_diff: dict[str, Any] = {
            "oldPath": old_payload_summary,
            "dashboardV2": v2_payload_summary,
        }

        if v2_status != "ready":
            mode = SOURCE_TRUTH_VALIDATION_MODE if widget_id in DIRECT_SOURCE_TRUTH_WIDGET_IDS else FACT_INVARIANT_VALIDATION_MODE
            validation = _new_validation(mode)
            validation["semanticStatus"] = "skipped"
            validation["regressionStatus"] = "skipped"
            widget_diff["validation"] = validation
            if widget_id in DIRECT_SOURCE_TRUTH_WIDGET_IDS:
                widget_diff["directTruth"] = _summarize_widget(widget_id, None)
            widget_diffs[widget_id] = widget_diff
            continue

        if widget_id in DIRECT_SOURCE_TRUTH_WIDGET_IDS:
            remaining_budget = max(0.0, source_truth_deadline - time.monotonic())
            direct_summary, validation = _validate_source_truth_widget(
                widget_id=widget_id,
                ctx=ctx,
                v2_summary=v2_payload_summary,
                timeout_seconds=min(
                    float(config.DASH_V2_COMPARE_SOURCE_TRUTH_TIMEOUT_SECONDS),
                    remaining_budget,
                ),
                shared_cache=source_truth_cache,
            )
            if validation["semanticStatus"] == "fail":
                direct_truth_failures += 1
            widget_diff["directTruth"] = direct_summary
        else:
            validation = _validate_fact_invariant_widget(
                widget_id=widget_id,
                raw_payload=raw_v2_payload,
                v2_summary=v2_payload_summary,
                exact_readiness=exact_readiness,
            )

        regression_status, warnings = _build_regression_validation(
            old_path_available=old_path_status == "ready",
            old_payload=old_payload,
            old_summary=old_payload_summary,
            v2_summary=v2_payload_summary,
        )
        validation["regressionStatus"] = regression_status
        validation["warnings"] = warnings
        widget_diff["validation"] = validation
        widget_diffs[widget_id] = widget_diff

    old_path_meta["widgetCount"] = _present_widget_count(widget_diffs, "oldPath")
    direct_truth_meta = {
        "generatedAt": _utc_now_iso(),
        "widgetCount": len(DIRECT_SOURCE_TRUTH_WIDGET_IDS),
        "sourceDateSemantics": True,
        "widgetIds": list(DIRECT_SOURCE_TRUTH_WIDGET_IDS),
        "timeoutSecondsPerWidget": float(config.DASH_V2_COMPARE_SOURCE_TRUTH_TIMEOUT_SECONDS),
        "timeoutSecondsTotal": float(config.DASH_V2_COMPARE_SOURCE_TRUTH_TOTAL_TIMEOUT_SECONDS),
        "status": "failed" if direct_truth_failures else "ready" if v2_status == "ready" else "skipped",
    }
    validation_summary = _build_validation_summary(
        widget_diffs=widget_diffs,
        old_path_status=old_path_status,
        v2_status=v2_status,
    )
    old_path_meta["regressionStatus"] = validation_summary["regressionStatus"]
    v2_meta["validationSummary"] = validation_summary
    compare_id = store.create_compare_run(
        cache_key=ctx.cache_key,
        from_date=ctx.from_date,
        to_date=ctx.to_date,
        old_path_meta=old_path_meta,
        v2_meta=v2_meta,
        direct_truth_meta=direct_truth_meta,
        diff_json=widget_diffs,
    )
    return {
        "compareId": compare_id,
        "cacheKey": ctx.cache_key,
        "from": ctx.from_date.isoformat(),
        "to": ctx.to_date.isoformat(),
        "widgetDiffs": widget_diffs,
        "widgetPresence": {
            "oldPath": _present_widget_count(widget_diffs, "oldPath"),
            "dashboardV2": _present_widget_count(widget_diffs, "dashboardV2"),
            "directTruth": _present_widget_count(widget_diffs, "directTruth"),
        },
        "v2Status": v2_status,
        "v2Meta": v2_meta,
        "validationSummary": validation_summary,
        "semanticStatus": validation_summary["semanticStatus"],
        "semanticGateReady": validation_summary["semanticGateReady"],
        "regressionStatus": validation_summary["regressionStatus"],
        "validationModel": validation_summary["validationModel"],
    }
