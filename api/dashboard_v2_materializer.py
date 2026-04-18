from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable

from loguru import logger

import config
from api import behavioral_briefs
from api import opportunity_briefs
from api import question_briefs
from api import recommendation_briefs
from api import topic_overviews
from api.dashboard_dates import DashboardDateContext, build_dashboard_date_context
from api.dashboard_v2_keys import build_dashboard_v2_coverage_row_key, build_dashboard_v2_row_key
from api.dashboard_v2_registry import FACT_FAMILIES, WIDGET_COVERAGE_BY_ID
from api.dashboard_v2_store import DashboardV2FactRow, DashboardV2Store
from api.queries import actionable, behavioral, comparative, network, predictive, psychographic, pulse, strategic
from buffer.supabase_writer import SupabaseWriter


DASHBOARD_V2_FACT_VERSION = 2
_INCREMENTAL_LOOKBACK_DAYS = 2
_COVERAGE_ROW_KEY = build_dashboard_v2_coverage_row_key()
_EXACT_FACT_WIDGET_IDS_BY_FAMILY: dict[str, tuple[str, ...]] = {
    family: tuple(
        widget_id
        for widget_id, coverage in WIDGET_COVERAGE_BY_ID.items()
        if coverage.exact_fact_backed and family in coverage.fact_families
    )
    for family in FACT_FAMILIES
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_day_start(value: date) -> datetime:
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _default_end_date() -> date:
    return (_utc_now() - timedelta(days=1)).date()


def _iter_days(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_str(value: Any, default: str = "") -> str:
    text = str(value).strip() if value is not None else ""
    return text or default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _identity_text(item: Any, *keys: str, default: str = "_summary") -> str:
    if isinstance(item, dict):
        for key in keys:
            text = _as_str(item.get(key))
            if text:
                return text
    return default


def _evidence_refs(item: Any) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    if not isinstance(item, dict):
        return refs
    if _as_str(item.get("sampleEvidenceId")):
        refs.append({"id": _as_str(item.get("sampleEvidenceId")), "kind": "sample"})
    for evidence in _as_list(item.get("evidence")):
        if isinstance(evidence, dict) and _as_str(evidence.get("id")):
            refs.append({"id": _as_str(evidence.get("id")), "kind": _as_str(evidence.get("kind"), "evidence")})
    return refs


def _source_refs(item: Any, *keys: str) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    if not isinstance(item, dict):
        return refs
    for key in keys:
        if _as_str(item.get(key)):
            refs.append({"key": key, "id": _as_str(item.get(key))})
    return refs


def _merge_json_values(existing: Any, incoming: Any) -> Any:
    if existing is None:
        return incoming
    if incoming is None:
        return existing
    if isinstance(existing, dict) and isinstance(incoming, dict):
        merged = dict(existing)
        for key, value in incoming.items():
            merged[key] = _merge_json_values(merged.get(key), value)
        return merged
    if isinstance(existing, list) and isinstance(incoming, list):
        seen: set[str] = set()
        merged: list[Any] = []
        for item in existing + incoming:
            try:
                marker = json.dumps(item, sort_keys=True, ensure_ascii=True, default=str)
            except Exception:
                marker = str(item)
            if marker in seen:
                continue
            seen.add(marker)
            merged.append(item)
        return merged
    if isinstance(existing, (int, float)) and isinstance(incoming, (int, float)):
        return incoming
    return incoming


def _make_fact_payload(
    *,
    kind: str,
    dimensions: dict[str, Any],
    metrics: dict[str, Any] | None = None,
    evidence_refs: list[dict[str, Any]] | None = None,
    source_refs: list[dict[str, Any]] | None = None,
    widget_payloads: dict[str, Any] | None = None,
    widget_ids: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "dimensions": {str(key): value for key, value in dimensions.items()},
        "metrics": metrics or {},
        "evidenceRefs": list(evidence_refs or []),
        "sourceRefs": list(source_refs or []),
        "factHints": {
            "widgetIds": list(widget_ids or []),
            "widgetPayloads": widget_payloads or {},
            "materializationStage": "pr2b_transitional_assembler_ready",
            "sourceEngine": "legacy_query_modules",
        },
    }


def _merge_fact_rows(existing: DashboardV2FactRow, incoming: DashboardV2FactRow) -> DashboardV2FactRow:
    return DashboardV2FactRow(
        row_key=existing.row_key,
        payload_json=_merge_json_values(existing.payload_json, incoming.payload_json),
        source_event_at=existing.source_event_at or incoming.source_event_at,
        topic_key=existing.topic_key or incoming.topic_key,
        channel_id=existing.channel_id or incoming.channel_id,
        user_id=existing.user_id or incoming.user_id,
        content_type=existing.content_type or incoming.content_type,
        cohort_key=existing.cohort_key or incoming.cohort_key,
    )


def _append_row(
    row_map: dict[str, DashboardV2FactRow],
    row: DashboardV2FactRow,
) -> None:
    existing = row_map.get(row.row_key)
    row_map[row.row_key] = _merge_fact_rows(existing, row) if existing is not None else row


def _build_row(
    *,
    kind: str,
    dimensions: dict[str, Any],
    payload_json: dict[str, Any],
    source_event_at: datetime | None = None,
    topic_key: str | None = None,
    channel_id: str | None = None,
    user_id: str | None = None,
    content_type: str | None = None,
    cohort_key: str | None = None,
) -> DashboardV2FactRow:
    return DashboardV2FactRow(
        row_key=build_dashboard_v2_row_key(kind, **dimensions),
        payload_json=payload_json,
        source_event_at=source_event_at,
        topic_key=topic_key,
        channel_id=channel_id,
        user_id=user_id,
        content_type=content_type,
        cohort_key=cohort_key,
    )


def _coverage_row(
    *,
    family: str,
    ctx: DashboardDateContext,
    row_count: int,
    materialized_at: datetime | None,
    source_watermark: datetime | None,
    coverage_ready: bool,
    failed_widgets: list[str] | tuple[str, ...] | None = None,
) -> DashboardV2FactRow:
    failed_widget_list = sorted({str(widget_id) for widget_id in (failed_widgets or []) if str(widget_id).strip()})
    return DashboardV2FactRow(
        row_key=_COVERAGE_ROW_KEY,
        payload_json={
            "kind": "coverage_marker",
            "dimensions": {"scope": "all"},
            "metrics": {"rowCount": int(row_count)},
            "evidenceRefs": [],
            "sourceRefs": [],
            "factHints": {
                "widgetIds": [],
                "widgetPayloads": {},
                "materializationStage": "pr2b_transitional_assembler_ready",
                "sourceEngine": "legacy_query_modules",
            },
            "coverageReady": bool(coverage_ready),
            "coverageDegraded": bool(failed_widget_list),
            "coverageState": "degraded" if failed_widget_list else "ready",
            "rowCount": int(row_count),
            "zeroData": int(row_count) == 0 and not failed_widget_list,
            "factVersion": DASHBOARD_V2_FACT_VERSION,
            "materializedAt": (materialized_at or _utc_now()).isoformat(),
            "sourceWatermark": source_watermark.isoformat() if source_watermark else None,
            "failedWidgets": failed_widget_list,
            "requiredWidgets": list(_EXACT_FACT_WIDGET_IDS_BY_FAMILY.get(family, ())),
            "range": {
                "from": ctx.from_date.isoformat(),
                "to": ctx.to_date.isoformat(),
                "days": ctx.days,
                "cacheKey": ctx.cache_key,
            },
        },
        source_event_at=_utc_day_start(ctx.from_date),
    )


def _community_brief_payload(ctx: DashboardDateContext) -> Any:
    return pulse.get_community_brief(ctx)


def _community_health_payload(ctx: DashboardDateContext) -> Any:
    return pulse.get_community_health(ctx)


def _trending_topics_payload(ctx: DashboardDateContext) -> Any:
    return pulse.get_trending_topics(ctx)


def _conversation_trends_payload(ctx: DashboardDateContext) -> Any:
    trend_lines = strategic.get_trend_lines(ctx)
    return {"trendLines": trend_lines, "trendData": trend_lines}


def _retention_risk_payload(ctx: DashboardDateContext) -> Any:
    return {
        "retentionFactors": predictive.get_retention_factors(ctx),
        "churnSignals": predictive.get_churn_signals(ctx),
    }


def _job_market_payload(ctx: DashboardDateContext) -> Any:
    return {
        "jobSeeking": actionable.get_job_seeking(ctx),
        "jobTrends": actionable.get_job_trends(ctx),
    }


def _content_performance_payload(ctx: DashboardDateContext) -> Any:
    return {
        "topPosts": comparative.get_top_posts(ctx),
        "contentTypePerformance": comparative.get_content_type_performance(ctx),
    }


_WIDGET_FACT_BUILDERS: dict[str, Callable[[DashboardDateContext], Any]] = {
    "community_brief": _community_brief_payload,
    "community_health_score": _community_health_payload,
    "trending_topics_feed": _trending_topics_payload,
    "topic_landscape": strategic.get_topic_bubbles,
    "conversation_trends": _conversation_trends_payload,
    "topic_lifecycle": strategic.get_lifecycle_stages,
    "satisfaction_by_area": behavioral.get_satisfaction_areas,
    "mood_over_time": behavioral.get_mood_data,
    "top_channels": network.get_community_channels,
    "key_voices": network.get_key_voices,
    "information_velocity": network.get_information_velocity,
    "interest_radar": psychographic.get_interests,
    "community_growth_funnel": predictive.get_growth_funnel,
    "retention_risk_gauge": _retention_risk_payload,
    "decision_stage_tracker": predictive.get_decision_stages,
    "emerging_interests": predictive.get_emerging_interests,
    "new_vs_returning_voice": predictive.get_new_vs_returning_voice_widget,
    "job_market_pulse": _job_market_payload,
    "week_over_week_shifts": comparative.get_weekly_shifts,
    "sentiment_by_topic": comparative.get_sentiment_by_topic,
    "content_performance": _content_performance_payload,
}

_SECONDARY_BUILDERS: dict[str, tuple[str, Callable[[], Any]]] = {
    "question_cloud": ("ai_question_briefs", lambda: question_briefs.get_question_briefs(force_refresh=False)),
    "problem_tracker": (
        "ai_behavioral_briefs",
        lambda: {"problemBriefs": behavioral_briefs.get_behavioral_briefs(force_refresh=False).get("problemBriefs", [])},
    ),
    "service_gap_detector": (
        "ai_behavioral_briefs",
        lambda: {"serviceGapBriefs": behavioral_briefs.get_behavioral_briefs(force_refresh=False).get("serviceGapBriefs", [])},
    ),
    "emotional_urgency_index": (
        "ai_behavioral_briefs",
        lambda: {"urgencyBriefs": behavioral_briefs.get_behavioral_briefs(force_refresh=False).get("urgencyBriefs", [])},
    ),
    "recommendation_tracker": (
        "ai_recommendation_briefs",
        lambda: {"recommendations": recommendation_briefs.get_recommendation_briefs(force_refresh=False)},
    ),
    "persona_gallery": ("persona_clusters", lambda: {"personas": psychographic.get_personas(_default_window_context())}),
    "business_opportunity_tracker": (
        "ai_opportunity_briefs",
        lambda: {"businessOpportunityBriefs": opportunity_briefs.get_business_opportunity_briefs(force_refresh=False)},
    ),
}


@dataclass
class DashboardV2MaterializeSummary:
    mode: str
    coverage_start: str
    coverage_end: str
    source_watermark: str | None
    family_runs: list[dict[str, Any]]
    secondary_runs: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DashboardV2WidgetBuildResult:
    outputs: dict[str, Any]
    failed_widget_ids: tuple[str, ...]


def _default_window_context(end_date: date | None = None) -> DashboardDateContext:
    resolved_end = end_date or _default_end_date()
    start_date = resolved_end - timedelta(days=max(1, int(config.DASH_DEFAULT_RANGE_DAYS)) - 1)
    return build_dashboard_date_context(start_date.isoformat(), resolved_end.isoformat())


def _build_exact_widget_outputs(ctx: DashboardDateContext) -> DashboardV2WidgetBuildResult:
    outputs: dict[str, Any] = {}
    failed_widget_ids: list[str] = []
    for widget_id, builder in _WIDGET_FACT_BUILDERS.items():
        try:
            outputs[widget_id] = builder(ctx)
        except Exception as exc:
            logger.warning("Dashboard V2 widget fact build failed | widget={} day={} error={}", widget_id, ctx.cache_key, exc)
            outputs[widget_id] = None
            failed_widget_ids.append(widget_id)
    return DashboardV2WidgetBuildResult(
        outputs=outputs,
        failed_widget_ids=tuple(sorted(set(failed_widget_ids))),
    )


def _failed_exact_widgets_for_family(
    family: str,
    failed_widget_ids: list[str] | tuple[str, ...] | None,
) -> list[str]:
    required = set(_EXACT_FACT_WIDGET_IDS_BY_FAMILY.get(family, ()))
    failed = {str(widget_id) for widget_id in (failed_widget_ids or [])}
    return sorted(required & failed)


def _build_content_family_rows(ctx: DashboardDateContext, outputs: dict[str, Any]) -> list[DashboardV2FactRow]:
    row_map: dict[str, DashboardV2FactRow] = {}
    brief = _as_dict(outputs.get("community_brief"))
    health = _as_dict(outputs.get("community_health_score"))
    if brief or health:
        _append_row(
            row_map,
            _build_row(
                kind="day_summary",
                dimensions={"scope": "all"},
                source_event_at=_utc_day_start(ctx.from_date),
                content_type="summary",
                payload_json=_make_fact_payload(
                    kind="day_summary",
                    dimensions={"scope": "all"},
                    metrics={
                        "messagesAnalyzed": _as_float(brief.get("messagesAnalyzed")),
                        "postsAnalyzedInWindow": _as_float(brief.get("postsAnalyzedInWindow", brief.get("postsAnalyzed24h"))),
                        "commentScopesAnalyzedInWindow": _as_float(
                            brief.get("commentScopesAnalyzedInWindow", brief.get("commentScopesAnalyzed24h"))
                        ),
                        "currentScore": _as_float(health.get("currentScore", health.get("score"))),
                    },
                    widget_ids=["community_brief", "community_health_score"],
                    widget_payloads={
                        "communityBrief": brief or {},
                        "communityHealth": health or {},
                    },
                ),
            ),
        )
    for item in _as_list(outputs.get("trending_topics_feed")):
        topic_key = _identity_text(item, "sourceTopic", "topic", "name", default="_topic")
        source_id = _identity_text(item, "id", "sampleEvidenceId", "topic", default=topic_key)
        payloads: dict[str, Any] = {"trendingTopics": [item]}
        if _as_float(item.get("trend")) > 0:
            payloads["trendingNewTopics"] = [item]
        _append_row(
            row_map,
            _build_row(
                kind="topic_evidence",
                dimensions={"content": "topic", "source": source_id, "topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                topic_key=topic_key,
                content_type="topic",
                payload_json=_make_fact_payload(
                    kind="topic_evidence",
                    dimensions={"content": "topic", "source": source_id, "topic": topic_key},
                    metrics={
                        "mentions": _as_float(item.get("mentions")),
                        "trend": _as_float(item.get("trend")),
                        "growthSupport": _as_float(item.get("growthSupport")),
                    },
                    evidence_refs=_evidence_refs(item),
                    source_refs=_source_refs(item, "id", "sampleEvidenceId"),
                    widget_ids=["trending_topics_feed"],
                    widget_payloads=payloads,
                ),
            ),
        )
    for item in _as_list(outputs.get("information_velocity")):
        topic_key = _identity_text(item, "sourceTopic", "topic", default="_topic")
        source_id = _identity_text(item, "id", "sampleEvidenceId", "topic", default=topic_key)
        _append_row(
            row_map,
            _build_row(
                kind="topic_evidence",
                dimensions={"content": "topic", "source": source_id, "topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                topic_key=topic_key,
                content_type="topic",
                payload_json=_make_fact_payload(
                    kind="topic_evidence",
                    dimensions={"content": "topic", "source": source_id, "topic": topic_key},
                    metrics={
                        "mentions": _as_float(item.get("mentions")),
                        "trend": _as_float(item.get("trend")),
                    },
                    evidence_refs=_evidence_refs(item),
                    source_refs=_source_refs(item, "id", "sampleEvidenceId"),
                    widget_ids=["information_velocity"],
                    widget_payloads={"viralTopics": [item]},
                ),
            ),
        )
    for item in _as_list(outputs.get("key_voices")):
        user_key = _identity_text(item, "userId", "id", "name", "username", default="_voice")
        source_id = _identity_text(item, "sampleEvidenceId", "id", "name", default=user_key)
        _append_row(
            row_map,
            _build_row(
                kind="user_activity_ref",
                dimensions={"content": "voice", "source": source_id, "user": user_key},
                source_event_at=_utc_day_start(ctx.from_date),
                user_id=user_key,
                content_type="voice",
                payload_json=_make_fact_payload(
                    kind="user_activity_ref",
                    dimensions={"content": "voice", "source": source_id, "user": user_key},
                    metrics={"messages": _as_float(item.get("messages")), "influence": _as_float(item.get("influence"))},
                    evidence_refs=_evidence_refs(item),
                    source_refs=_source_refs(item, "id", "sampleEvidenceId"),
                    widget_ids=["key_voices"],
                    widget_payloads={"keyVoices": [item]},
                ),
            ),
        )
    content_perf = _as_dict(outputs.get("content_performance"))
    for item in _as_list(content_perf.get("topPosts")):
        source_id = _identity_text(item, "id", "postId", default="_post")
        content_type = _identity_text(item, "contentType", "type", default="post")
        _append_row(
            row_map,
            _build_row(
                kind="content_perf_input",
                dimensions={"content": content_type, "source": source_id},
                source_event_at=_utc_day_start(ctx.from_date),
                content_type=content_type,
                payload_json=_make_fact_payload(
                    kind="content_perf_input",
                    dimensions={"content": content_type, "source": source_id},
                    metrics={"score": _as_float(item.get("engagementScore", item.get("score"))), "engagement": _as_float(item.get("engagement"))},
                    evidence_refs=_evidence_refs(item),
                    source_refs=_source_refs(item, "id", "postId"),
                    widget_ids=["content_performance"],
                    widget_payloads={"topPosts": [item]},
                ),
            ),
        )
    for item in _as_list(content_perf.get("contentTypePerformance")):
        content_type = _identity_text(item, "type", "contentType", default="content")
        _append_row(
            row_map,
            _build_row(
                kind="content_perf_input",
                dimensions={"content": content_type, "source": content_type},
                source_event_at=_utc_day_start(ctx.from_date),
                content_type=content_type,
                payload_json=_make_fact_payload(
                    kind="content_perf_input",
                    dimensions={"content": content_type, "source": content_type},
                    metrics={
                        "posts": _as_float(item.get("posts")),
                        "engagementRate": _as_float(item.get("engagementRate")),
                    },
                    widget_ids=["content_performance"],
                    widget_payloads={"contentTypePerformance": [item]},
                ),
            ),
        )
    job_market = _as_dict(outputs.get("job_market_pulse"))
    for item in _as_list(job_market.get("jobSeeking")) + _as_list(job_market.get("jobTrends")):
        source_id = _identity_text(item, "id", "role", "topic", default="_job")
        content_type = _identity_text(item, "type", "contentType", default="job")
        _append_row(
            row_map,
            _build_row(
                kind="content_perf_input",
                dimensions={"content": content_type, "source": source_id},
                source_event_at=_utc_day_start(ctx.from_date),
                content_type=content_type,
                payload_json=_make_fact_payload(
                    kind="content_perf_input",
                    dimensions={"content": content_type, "source": source_id},
                    metrics={"mentions": _as_float(item.get("mentions")), "trend": _as_float(item.get("trend"))},
                    evidence_refs=_evidence_refs(item),
                    source_refs=_source_refs(item, "id"),
                    widget_ids=["job_market_pulse"],
                    widget_payloads={"jobSeeking": [item]} if item in _as_list(job_market.get("jobSeeking")) else {"jobTrends": [item]},
                ),
            ),
        )
    voice_widget = outputs.get("new_vs_returning_voice")
    if isinstance(voice_widget, dict):
        _append_row(
            row_map,
            _build_row(
                kind="user_activity_ref",
                dimensions={"content": "cohort", "source": "summary", "user": "_summary"},
                source_event_at=_utc_day_start(ctx.from_date),
                user_id="_summary",
                content_type="cohort",
                payload_json=_make_fact_payload(
                    kind="user_activity_ref",
                    dimensions={"content": "cohort", "source": "summary", "user": "_summary"},
                    widget_ids=["new_vs_returning_voice"],
                    widget_payloads={"newVsReturningVoiceWidget": voice_widget},
                ),
            ),
        )
    return list(row_map.values())


def _build_topics_family_rows(ctx: DashboardDateContext, outputs: dict[str, Any]) -> list[DashboardV2FactRow]:
    row_map: dict[str, DashboardV2FactRow] = {}
    brief = _as_dict(outputs.get("community_brief"))
    health = _as_dict(outputs.get("community_health_score"))
    if brief or health:
        _append_row(
            row_map,
            _build_row(
                kind="topic_day",
                dimensions={"topic": "_summary"},
                source_event_at=_utc_day_start(ctx.from_date),
                topic_key="_summary",
                payload_json=_make_fact_payload(
                    kind="topic_day",
                    dimensions={"topic": "_summary"},
                    widget_ids=["community_brief", "community_health_score"],
                    widget_payloads={"communityBrief": brief or {}, "communityHealth": health or {}},
                ),
            ),
        )
    for item in _as_list(outputs.get("topic_landscape")):
        topic_key = _identity_text(item, "sourceTopic", "name", "topic", default="_topic")
        _append_row(
            row_map,
            _build_row(
                kind="topic_day",
                dimensions={"topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                topic_key=topic_key,
                payload_json=_make_fact_payload(
                    kind="topic_day",
                    dimensions={"topic": topic_key},
                    metrics={"value": _as_float(item.get("value")), "growth": _as_float(item.get("growth"))},
                    widget_ids=["topic_landscape"],
                    widget_payloads={"topicBubbles": [item]},
                ),
            ),
        )
    for item in _as_list(outputs.get("trending_topics_feed")):
        topic_key = _identity_text(item, "sourceTopic", "topic", "name", default="_topic")
        payloads: dict[str, Any] = {"trendingTopics": [item]}
        if _as_float(item.get("trend")) > 0:
            payloads["trendingNewTopics"] = [item]
        _append_row(
            row_map,
            _build_row(
                kind="topic_day",
                dimensions={"topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                topic_key=topic_key,
                payload_json=_make_fact_payload(
                    kind="topic_day",
                    dimensions={"topic": topic_key},
                    metrics={"mentions": _as_float(item.get("mentions")), "trend": _as_float(item.get("trend"))},
                    evidence_refs=_evidence_refs(item),
                    widget_ids=["trending_topics_feed"],
                    widget_payloads=payloads,
                ),
            ),
        )
    for item in _as_list(_as_dict(outputs.get("conversation_trends")).get("trendLines")):
        topic_key = _identity_text(item, "key", "label", default="_trend")
        _append_row(
            row_map,
            _build_row(
                kind="topic_trend_day",
                dimensions={"topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                topic_key=topic_key,
                payload_json=_make_fact_payload(
                    kind="topic_trend_day",
                    dimensions={"topic": topic_key},
                    metrics={"current": _as_float(item.get("current")), "change": _as_float(item.get("change"))},
                    widget_ids=["conversation_trends"],
                    widget_payloads={"trendLines": [item], "trendData": [item]},
                ),
            ),
        )
    for stage in _as_list(outputs.get("topic_lifecycle")):
        if not isinstance(stage, dict):
            continue
        for item in _as_list(stage.get("topics")):
            topic_key = _identity_text(item, "sourceTopic", "name", "topic", default="_topic")
            _append_row(
                row_map,
                _build_row(
                    kind="topic_lifecycle_day",
                    dimensions={"topic": topic_key},
                    source_event_at=_utc_day_start(ctx.from_date),
                    topic_key=topic_key,
                    payload_json=_make_fact_payload(
                        kind="topic_lifecycle_day",
                        dimensions={"topic": topic_key},
                        metrics={"momentum": _as_float(item.get("momentum")), "volume": _as_float(item.get("volume"))},
                        widget_ids=["topic_lifecycle"],
                        widget_payloads={"lifecycleStages": [{"stage": stage.get("stage"), "topics": [item], "color": stage.get("color")}]},
                    ),
                ),
            )
    for item in _as_list(outputs.get("information_velocity")):
        topic_key = _identity_text(item, "sourceTopic", "topic", default="_topic")
        _append_row(
            row_map,
            _build_row(
                kind="topic_trend_day",
                dimensions={"topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                topic_key=topic_key,
                payload_json=_make_fact_payload(
                    kind="topic_trend_day",
                    dimensions={"topic": topic_key},
                    metrics={"mentions": _as_float(item.get("mentions")), "trend": _as_float(item.get("trend"))},
                    evidence_refs=_evidence_refs(item),
                    widget_ids=["information_velocity"],
                    widget_payloads={"viralTopics": [item]},
                ),
            ),
        )
    for item in _as_list(outputs.get("interest_radar")):
        topic_key = _identity_text(item, "topic", "name", "interest", default="_interest")
        _append_row(
            row_map,
            _build_row(
                kind="topic_day",
                dimensions={"topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                topic_key=topic_key,
                payload_json=_make_fact_payload(
                    kind="topic_day",
                    dimensions={"topic": topic_key},
                    metrics={"value": _as_float(item.get("value", item.get("mentions"))), "growth": _as_float(item.get("growth"))},
                    widget_ids=["interest_radar"],
                    widget_payloads={"interests": [item]},
                ),
            ),
        )
    for item in _as_list(outputs.get("emerging_interests")):
        topic_key = _identity_text(item, "topic", "name", default="_emerging")
        _append_row(
            row_map,
            _build_row(
                kind="topic_day",
                dimensions={"topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                topic_key=topic_key,
                payload_json=_make_fact_payload(
                    kind="topic_day",
                    dimensions={"topic": topic_key},
                    metrics={"growthRate": _as_float(item.get("growthRate")), "currentVolume": _as_float(item.get("currentVolume"))},
                    widget_ids=["emerging_interests"],
                    widget_payloads={"emergingInterests": [item]},
                ),
            ),
        )
    for item in _as_list(outputs.get("sentiment_by_topic")):
        topic_key = _identity_text(item, "topic", "name", default="_sentiment")
        _append_row(
            row_map,
            _build_row(
                kind="topic_day",
                dimensions={"topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                topic_key=topic_key,
                payload_json=_make_fact_payload(
                    kind="topic_day",
                    dimensions={"topic": topic_key},
                    metrics={"positive": _as_float(item.get("positive")), "negative": _as_float(item.get("negative"))},
                    widget_ids=["sentiment_by_topic"],
                    widget_payloads={"sentimentByTopic": [item]},
                ),
            ),
        )
    return list(row_map.values())


def _build_channels_family_rows(ctx: DashboardDateContext, outputs: dict[str, Any]) -> list[DashboardV2FactRow]:
    row_map: dict[str, DashboardV2FactRow] = {}
    for item in _as_list(outputs.get("top_channels")):
        channel_key = _identity_text(item, "channelId", "id", "name", "channel", default="_channel")
        _append_row(
            row_map,
            _build_row(
                kind="channel_day",
                dimensions={"channel": channel_key},
                source_event_at=_utc_day_start(ctx.from_date),
                channel_id=channel_key,
                payload_json=_make_fact_payload(
                    kind="channel_day",
                    dimensions={"channel": channel_key},
                    metrics={"messages": _as_float(item.get("messages")), "engagement": _as_float(item.get("engagement"))},
                    widget_ids=["top_channels"],
                    widget_payloads={"communityChannels": [item]},
                ),
            ),
        )
    for item in _as_list(outputs.get("key_voices")):
        channel_key = _identity_text(item, "channelId", "channel", "name", default="_channel")
        _append_row(
            row_map,
            _build_row(
                kind="channel_day",
                dimensions={"channel": channel_key},
                source_event_at=_utc_day_start(ctx.from_date),
                channel_id=channel_key,
                payload_json=_make_fact_payload(
                    kind="channel_day",
                    dimensions={"channel": channel_key},
                    metrics={"voiceInfluence": _as_float(item.get("influence"))},
                    widget_ids=["key_voices"],
                    widget_payloads={"keyVoices": [item]},
                ),
            ),
        )
    return list(row_map.values())


def _build_users_family_rows(ctx: DashboardDateContext, outputs: dict[str, Any]) -> list[DashboardV2FactRow]:
    row_map: dict[str, DashboardV2FactRow] = {}
    for item in _as_list(outputs.get("key_voices")):
        user_key = _identity_text(item, "userId", "id", "name", "username", default="_voice")
        _append_row(
            row_map,
            _build_row(
                kind="user_day",
                dimensions={"user": user_key},
                source_event_at=_utc_day_start(ctx.from_date),
                user_id=user_key,
                payload_json=_make_fact_payload(
                    kind="user_day",
                    dimensions={"user": user_key},
                    metrics={"messages": _as_float(item.get("messages")), "influence": _as_float(item.get("influence"))},
                    widget_ids=["key_voices"],
                    widget_payloads={"keyVoices": [item]},
                ),
            ),
        )
    for item in _as_list(outputs.get("interest_radar")):
        cohort_key = _identity_text(item, "category", "topic", default="_interest")
        _append_row(
            row_map,
            _build_row(
                kind="cohort_day",
                dimensions={"cohort": cohort_key},
                source_event_at=_utc_day_start(ctx.from_date),
                cohort_key=cohort_key,
                payload_json=_make_fact_payload(
                    kind="cohort_day",
                    dimensions={"cohort": cohort_key},
                    metrics={"value": _as_float(item.get("value", item.get("mentions")))},
                    widget_ids=["interest_radar"],
                    widget_payloads={"interests": [item]},
                ),
            ),
        )
    for item in _as_list(outputs.get("community_growth_funnel")):
        cohort_key = _identity_text(item, "stage", "label", default="_stage")
        _append_row(
            row_map,
            _build_row(
                kind="cohort_day",
                dimensions={"cohort": cohort_key},
                source_event_at=_utc_day_start(ctx.from_date),
                cohort_key=cohort_key,
                payload_json=_make_fact_payload(
                    kind="cohort_day",
                    dimensions={"cohort": cohort_key},
                    metrics={"count": _as_float(item.get("count", item.get("value")))},
                    widget_ids=["community_growth_funnel"],
                    widget_payloads={"growthFunnel": [item]},
                ),
            ),
        )
    retention = _as_dict(outputs.get("retention_risk_gauge"))
    for item in _as_list(retention.get("retentionFactors")) + _as_list(retention.get("churnSignals")):
        cohort_key = _identity_text(item, "factor", "label", "name", default="_risk")
        payloads = {"retentionFactors": [item]} if item in _as_list(retention.get("retentionFactors")) else {"churnSignals": [item]}
        _append_row(
            row_map,
            _build_row(
                kind="cohort_day",
                dimensions={"cohort": cohort_key},
                source_event_at=_utc_day_start(ctx.from_date),
                cohort_key=cohort_key,
                payload_json=_make_fact_payload(
                    kind="cohort_day",
                    dimensions={"cohort": cohort_key},
                    metrics={"score": _as_float(item.get("score", item.get("risk")))},
                    widget_ids=["retention_risk_gauge"],
                    widget_payloads=payloads,
                ),
            ),
        )
    voice_widget = outputs.get("new_vs_returning_voice")
    if isinstance(voice_widget, dict):
        for bucket_key in ("buckets", "series", "segments"):
            for item in _as_list(voice_widget.get(bucket_key)):
                cohort_key = _identity_text(item, "key", "label", "name", default="_cohort")
                _append_row(
                    row_map,
                    _build_row(
                        kind="cohort_day",
                        dimensions={"cohort": cohort_key},
                        source_event_at=_utc_day_start(ctx.from_date),
                        cohort_key=cohort_key,
                        payload_json=_make_fact_payload(
                            kind="cohort_day",
                            dimensions={"cohort": cohort_key},
                            metrics={"count": _as_float(item.get("count", item.get("value")))},
                            widget_ids=["new_vs_returning_voice"],
                            widget_payloads={"newVsReturningVoiceWidget": voice_widget},
                        ),
                    ),
                )
        if not row_map:
            _append_row(
                row_map,
                _build_row(
                    kind="cohort_day",
                    dimensions={"cohort": "_summary"},
                    source_event_at=_utc_day_start(ctx.from_date),
                    cohort_key="_summary",
                    payload_json=_make_fact_payload(
                        kind="cohort_day",
                        dimensions={"cohort": "_summary"},
                        widget_ids=["new_vs_returning_voice"],
                        widget_payloads={"newVsReturningVoiceWidget": voice_widget},
                    ),
                ),
            )
    return list(row_map.values())


def _build_behavioral_family_rows(ctx: DashboardDateContext, outputs: dict[str, Any]) -> list[DashboardV2FactRow]:
    row_map: dict[str, DashboardV2FactRow] = {}
    for item in _as_list(outputs.get("satisfaction_by_area")):
        area_key = _identity_text(item, "area", "name", default="_area")
        _append_row(
            row_map,
            _build_row(
                kind="satisfaction_area_day",
                dimensions={"area": area_key},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="satisfaction_area_day",
                    dimensions={"area": area_key},
                    metrics={"satisfaction": _as_float(item.get("satisfaction")), "mentions": _as_float(item.get("mentions"))},
                    widget_ids=["satisfaction_by_area"],
                    widget_payloads={"satisfactionAreas": [item]},
                ),
            ),
        )
    mood_rows = _as_list(outputs.get("mood_over_time"))
    if mood_rows:
        sentiments = sorted({_identity_text(item, "sentiment", "label", default="neutral") for item in mood_rows})
    else:
        sentiments = []
    for item in mood_rows:
        sentiment_key = _identity_text(item, "sentiment", "label", default="neutral")
        _append_row(
            row_map,
            _build_row(
                kind="mood_day",
                dimensions={"sentiment": sentiment_key},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="mood_day",
                    dimensions={"sentiment": sentiment_key},
                    metrics={"value": _as_float(item.get("value", item.get("score"))), "count": _as_float(item.get("count"))},
                    widget_ids=["mood_over_time"],
                    widget_payloads={
                        "moodData": [item],
                        "moodConfig": {"sentiments": sentiments},
                    },
                ),
            ),
        )
    return list(row_map.values())


def _build_predictive_family_rows(ctx: DashboardDateContext, outputs: dict[str, Any]) -> list[DashboardV2FactRow]:
    row_map: dict[str, DashboardV2FactRow] = {}
    for item in _as_list(outputs.get("community_growth_funnel")):
        stage_key = _identity_text(item, "stage", "label", default="_stage")
        _append_row(
            row_map,
            _build_row(
                kind="funnel_stage_day",
                dimensions={"stage": stage_key},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="funnel_stage_day",
                    dimensions={"stage": stage_key},
                    metrics={"count": _as_float(item.get("count", item.get("value")))},
                    widget_ids=["community_growth_funnel"],
                    widget_payloads={"growthFunnel": [item]},
                ),
            ),
        )
    retention = _as_dict(outputs.get("retention_risk_gauge"))
    for item in _as_list(retention.get("retentionFactors")) + _as_list(retention.get("churnSignals")):
        factor_key = _identity_text(item, "factor", "label", "name", default="_factor")
        payloads = {"retentionFactors": [item]} if item in _as_list(retention.get("retentionFactors")) else {"churnSignals": [item]}
        _append_row(
            row_map,
            _build_row(
                kind="retention_factor_day",
                dimensions={"factor": factor_key},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="retention_factor_day",
                    dimensions={"factor": factor_key},
                    metrics={"score": _as_float(item.get("score", item.get("risk")))},
                    widget_ids=["retention_risk_gauge"],
                    widget_payloads=payloads,
                ),
            ),
        )
    for item in _as_list(outputs.get("decision_stage_tracker")):
        stage_key = _identity_text(item, "stage", "label", default="_decision")
        _append_row(
            row_map,
            _build_row(
                kind="decision_stage_day",
                dimensions={"stage": stage_key},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="decision_stage_day",
                    dimensions={"stage": stage_key},
                    metrics={"count": _as_float(item.get("count", item.get("value")))},
                    widget_ids=["decision_stage_tracker"],
                    widget_payloads={"decisionStages": [item]},
                ),
            ),
        )
    for item in _as_list(outputs.get("emerging_interests")):
        topic_key = _identity_text(item, "topic", "name", default="_emerging")
        _append_row(
            row_map,
            _build_row(
                kind="emerging_interest_day",
                dimensions={"topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="emerging_interest_day",
                    dimensions={"topic": topic_key},
                    metrics={"growthRate": _as_float(item.get("growthRate")), "currentVolume": _as_float(item.get("currentVolume"))},
                    widget_ids=["emerging_interests"],
                    widget_payloads={"emergingInterests": [item]},
                ),
            ),
        )
    voice_widget = outputs.get("new_vs_returning_voice")
    if isinstance(voice_widget, dict):
        _append_row(
            row_map,
            _build_row(
                kind="retention_factor_day",
                dimensions={"factor": "_voice_balance"},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="retention_factor_day",
                    dimensions={"factor": "_voice_balance"},
                    widget_ids=["new_vs_returning_voice"],
                    widget_payloads={"newVsReturningVoiceWidget": voice_widget},
                ),
            ),
        )
    return list(row_map.values())


def _build_actionable_family_rows(ctx: DashboardDateContext, outputs: dict[str, Any]) -> list[DashboardV2FactRow]:
    row_map: dict[str, DashboardV2FactRow] = {}
    payload = _as_dict(outputs.get("job_market_pulse"))
    for item in _as_list(payload.get("jobSeeking")) + _as_list(payload.get("jobTrends")):
        role_key = _identity_text(item, "role", "topic", "title", default="_job")
        payloads = {"jobSeeking": [item]} if item in _as_list(payload.get("jobSeeking")) else {"jobTrends": [item]}
        _append_row(
            row_map,
            _build_row(
                kind="job_market_day",
                dimensions={"role": role_key},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="job_market_day",
                    dimensions={"role": role_key},
                    metrics={"mentions": _as_float(item.get("mentions")), "trend": _as_float(item.get("trend"))},
                    evidence_refs=_evidence_refs(item),
                    source_refs=_source_refs(item, "id"),
                    widget_ids=["job_market_pulse"],
                    widget_payloads=payloads,
                ),
            ),
        )
    return list(row_map.values())


def _build_comparative_family_rows(ctx: DashboardDateContext, outputs: dict[str, Any]) -> list[DashboardV2FactRow]:
    row_map: dict[str, DashboardV2FactRow] = {}
    for item in _as_list(outputs.get("week_over_week_shifts")):
        metric_key = _identity_text(item, "metric", "topic", "name", default="_shift")
        _append_row(
            row_map,
            _build_row(
                kind="weekly_shift_input_day",
                dimensions={"metric": metric_key},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="weekly_shift_input_day",
                    dimensions={"metric": metric_key},
                    metrics={"delta": _as_float(item.get("delta", item.get("change"))), "mentions": _as_float(item.get("mentions"))},
                    widget_ids=["week_over_week_shifts"],
                    widget_payloads={"weeklyShifts": [item]},
                ),
            ),
        )
    for item in _as_list(outputs.get("sentiment_by_topic")):
        topic_key = _identity_text(item, "topic", "name", default="_sentiment")
        _append_row(
            row_map,
            _build_row(
                kind="topic_sentiment_day",
                dimensions={"topic": topic_key},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="topic_sentiment_day",
                    dimensions={"topic": topic_key},
                    metrics={
                        "positive": _as_float(item.get("positive")),
                        "neutral": _as_float(item.get("neutral")),
                        "negative": _as_float(item.get("negative")),
                    },
                    widget_ids=["sentiment_by_topic"],
                    widget_payloads={"sentimentByTopic": [item]},
                ),
            ),
        )
    content_perf = _as_dict(outputs.get("content_performance"))
    for item in _as_list(content_perf.get("contentTypePerformance")) + _as_list(content_perf.get("topPosts")):
        content_type = _identity_text(item, "type", "contentType", default="content")
        payloads = {"contentTypePerformance": [item]} if item in _as_list(content_perf.get("contentTypePerformance")) else {"topPosts": [item]}
        _append_row(
            row_map,
            _build_row(
                kind="content_performance_day",
                dimensions={"content": content_type},
                source_event_at=_utc_day_start(ctx.from_date),
                payload_json=_make_fact_payload(
                    kind="content_performance_day",
                    dimensions={"content": content_type},
                    metrics={"score": _as_float(item.get("engagementScore", item.get("score"))), "engagement": _as_float(item.get("engagement"))},
                    widget_ids=["content_performance"],
                    widget_payloads=payloads,
                ),
            ),
        )
    return list(row_map.values())


_FAMILY_ROW_BUILDERS: dict[str, Callable[[DashboardDateContext, dict[str, Any]], list[DashboardV2FactRow]]] = {
    "content": _build_content_family_rows,
    "topics": _build_topics_family_rows,
    "channels": _build_channels_family_rows,
    "users": _build_users_family_rows,
    "behavioral": _build_behavioral_family_rows,
    "predictive": _build_predictive_family_rows,
    "actionable": _build_actionable_family_rows,
    "comparative": _build_comparative_family_rows,
}


def _materialize_family_rows(
    family: str,
    ctx: DashboardDateContext,
    widget_outputs: dict[str, Any] | None = None,
    *,
    failed_widget_ids: list[str] | tuple[str, ...] | None = None,
    source_watermark: datetime | None = None,
    materialized_at: datetime | None = None,
) -> list[DashboardV2FactRow]:
    build_result: DashboardV2WidgetBuildResult | None = None
    if widget_outputs is None:
        build_result = _build_exact_widget_outputs(ctx)
        outputs = build_result.outputs
    else:
        outputs = widget_outputs
    family_failed_widgets = _failed_exact_widgets_for_family(
        family,
        failed_widget_ids if failed_widget_ids is not None else (build_result.failed_widget_ids if build_result else ()),
    )
    builder = _FAMILY_ROW_BUILDERS[family]
    rows = builder(ctx, outputs)
    rows_with_coverage = list(rows)
    rows_with_coverage.append(
        _coverage_row(
            family=family,
            ctx=ctx,
            row_count=len(rows),
            materialized_at=materialized_at or _utc_now(),
            source_watermark=source_watermark,
            coverage_ready=not bool(family_failed_widgets),
            failed_widgets=family_failed_widgets,
        )
    )
    return rows_with_coverage


def _materialize_secondary_rows(store: DashboardV2Store, *, end_date: date, source_watermark: datetime | None) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    ctx = _default_window_context(end_date)
    for widget_id, (storage_key, builder) in _SECONDARY_BUILDERS.items():
        try:
            payload = builder()
            store.upsert_secondary_materialization(
                storage_key=storage_key,
                widget_id=widget_id,
                window_start=ctx.from_date,
                window_end=ctx.to_date,
                payload_json={
                    "widgetId": widget_id,
                    "range": {
                        "from": ctx.from_date.isoformat(),
                        "to": ctx.to_date.isoformat(),
                        "days": ctx.days,
                        "rangeAware": False,
                    },
                    "data": payload,
                },
                meta_json={
                    "source": "legacy_materialized_snapshot",
                    "rangeAware": False,
                    "plannedUpgrade": "PR2B",
                    "materializationStage": "pr2b_background_secondary_transition",
                },
                source_watermark=source_watermark,
            )
            results.append({"widgetId": widget_id, "storageKey": storage_key, "status": "ready"})
        except Exception as exc:
            logger.warning("Dashboard V2 secondary materialization failed | widget={} error={}", widget_id, exc)
            results.append({"widgetId": widget_id, "storageKey": storage_key, "status": "failed", "error": str(exc)})
    try:
        payload = topic_overviews.get_topic_overviews_snapshot(force_refresh=False)
        store.upsert_secondary_materialization(
            storage_key="topic_overviews_v2",
            widget_id="topic_overviews_v2",
            window_start=ctx.from_date,
            window_end=ctx.to_date,
            payload_json=payload,
            meta_json={
                "source": "legacy_materialized_snapshot",
                "rangeAware": False,
                "plannedUpgrade": "PR2B",
                "materializationStage": "pr2b_background_secondary_transition",
            },
            source_watermark=source_watermark,
        )
        results.append({"widgetId": "topic_overviews_v2", "storageKey": "topic_overviews_v2", "status": "ready"})
    except Exception as exc:
        logger.warning("Dashboard V2 topic overview materialization failed: {}", exc)
        results.append({"widgetId": "topic_overviews_v2", "storageKey": "topic_overviews_v2", "status": "failed", "error": str(exc)})
    return results


def materialize_dashboard_v2_foundation(
    store: DashboardV2Store,
    *,
    mode: str = "incremental",
    end_date: date | None = None,
    lookback_days: int | None = None,
) -> dict[str, Any]:
    resolved_end = end_date or _default_end_date()
    default_lookback = _INCREMENTAL_LOOKBACK_DAYS if mode == "incremental" else int(config.DASH_V2_RECONCILE_LOOKBACK_DAYS)
    resolved_lookback = max(1, int(lookback_days or default_lookback))
    coverage_start = resolved_end - timedelta(days=resolved_lookback - 1)
    source_watermark = _utc_now()

    family_runs: list[dict[str, Any]] = []
    for family in FACT_FAMILIES:
        run_id = store.create_fact_run(
            fact_family=family,
            fact_version=DASHBOARD_V2_FACT_VERSION,
            coverage_start=coverage_start,
            coverage_end=resolved_end,
            meta_json={"mode": mode, "materializationStage": "pr2b_transitional_assembler_ready"},
        )
        rows_inserted = 0
        days_processed = 0
        degraded_days: list[str] = []
        failed_widgets: set[str] = set()
        try:
            for fact_day in _iter_days(coverage_start, resolved_end):
                ctx = build_dashboard_date_context(fact_day.isoformat(), fact_day.isoformat())
                build_result = _build_exact_widget_outputs(ctx)
                family_failed_widgets = _failed_exact_widgets_for_family(family, build_result.failed_widget_ids)
                rows = _materialize_family_rows(
                    family,
                    ctx,
                    build_result.outputs,
                    failed_widget_ids=build_result.failed_widget_ids,
                    source_watermark=source_watermark,
                    materialized_at=source_watermark,
                )
                if family_failed_widgets:
                    degraded_days.append(fact_day.isoformat())
                    failed_widgets.update(family_failed_widgets)
                rows_inserted += store.replace_daily_fact_rows(
                    fact_family=family,
                    fact_date=fact_day,
                    run_id=run_id,
                    fact_version=DASHBOARD_V2_FACT_VERSION,
                    rows=rows,
                    source_watermark=source_watermark,
                )
                store.mark_overlapping_artifacts_stale(
                    fact_family=family,
                    changed_date=fact_day,
                    new_watermark=source_watermark,
                    reason=f"{mode}:{family}:{fact_day.isoformat()}:fact_version_{DASHBOARD_V2_FACT_VERSION}",
                )
                days_processed += 1
            store.complete_fact_run(
                run_id,
                status="completed",
                source_watermark=source_watermark,
                meta_json={
                    "rowsInserted": rows_inserted,
                    "daysProcessed": days_processed,
                    "degradedDays": degraded_days,
                    "failedWidgets": sorted(failed_widgets),
                    "mode": mode,
                    "factVersion": DASHBOARD_V2_FACT_VERSION,
                },
            )
            family_runs.append(
                {
                    "factFamily": family,
                    "runId": run_id,
                    "status": "completed",
                    "rowsInserted": rows_inserted,
                    "daysProcessed": days_processed,
                    "degradedDays": degraded_days,
                    "failedWidgets": sorted(failed_widgets),
                    "factVersion": DASHBOARD_V2_FACT_VERSION,
                }
            )
        except Exception as exc:
            store.complete_fact_run(
                run_id,
                status="failed",
                source_watermark=source_watermark,
                error=str(exc),
                meta_json={
                    "rowsInserted": rows_inserted,
                    "daysProcessed": days_processed,
                    "degradedDays": degraded_days,
                    "failedWidgets": sorted(failed_widgets),
                    "mode": mode,
                    "factVersion": DASHBOARD_V2_FACT_VERSION,
                },
            )
            logger.exception("Dashboard V2 foundation materialization failed | family={} error={}", family, exc)
            family_runs.append(
                {
                    "factFamily": family,
                    "runId": run_id,
                    "status": "failed",
                    "rowsInserted": rows_inserted,
                    "daysProcessed": days_processed,
                    "degradedDays": degraded_days,
                    "failedWidgets": sorted(failed_widgets),
                    "error": str(exc),
                    "factVersion": DASHBOARD_V2_FACT_VERSION,
                }
            )

    secondary_runs = _materialize_secondary_rows(store, end_date=resolved_end, source_watermark=source_watermark)
    summary = DashboardV2MaterializeSummary(
        mode=mode,
        coverage_start=coverage_start.isoformat(),
        coverage_end=resolved_end.isoformat(),
        source_watermark=source_watermark.isoformat(),
        family_runs=family_runs,
        secondary_runs=secondary_runs,
    )
    return summary.to_dict()


def materialize_dashboard_v2_incremental(store: DashboardV2Store, *, end_date: date | None = None) -> dict[str, Any]:
    return materialize_dashboard_v2_foundation(store, mode="incremental", end_date=end_date, lookback_days=_INCREMENTAL_LOOKBACK_DAYS)


def materialize_dashboard_v2_reconciliation(store: DashboardV2Store, *, end_date: date | None = None) -> dict[str, Any]:
    return materialize_dashboard_v2_foundation(
        store,
        mode="reconciliation",
        end_date=end_date,
        lookback_days=int(config.DASH_V2_RECONCILE_LOOKBACK_DAYS),
    )


def materialize_dashboard_v2_backfill(store: DashboardV2Store, *, end_date: date | None = None) -> dict[str, Any]:
    return materialize_dashboard_v2_foundation(
        store,
        mode="backfill",
        end_date=end_date,
        lookback_days=int(config.DASH_V2_FACT_LOOKBACK_DAYS),
    )


def make_dashboard_v2_store(writer: SupabaseWriter) -> DashboardV2Store:
    return DashboardV2Store(writer)
