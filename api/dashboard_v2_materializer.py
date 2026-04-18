from __future__ import annotations

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
from api.dashboard_v2_registry import (
    EXACT_FACT_BACKED_WIDGET_IDS,
    FACT_FAMILIES,
    WIDGET_COVERAGE_BY_ID,
)
from api.dashboard_v2_store import DashboardV2FactRow, DashboardV2Store
from api.queries import actionable, behavioral, comparative, network, predictive, psychographic, pulse, strategic
from buffer.supabase_writer import SupabaseWriter


DASHBOARD_V2_FACT_VERSION = 1
_INCREMENTAL_LOOKBACK_DAYS = 2


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


def _row_payload(widget_id: str, ctx: DashboardDateContext, data: Any) -> dict[str, Any]:
    return {
        "widgetId": widget_id,
        "materializationStage": "pr2a_transitional",
        "sourceEngine": "legacy_query_modules",
        "range": {
            "from": ctx.from_date.isoformat(),
            "to": ctx.to_date.isoformat(),
            "days": ctx.days,
            "cacheKey": ctx.cache_key,
        },
        "data": data,
    }


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


def _default_window_context(end_date: date | None = None) -> DashboardDateContext:
    resolved_end = end_date or _default_end_date()
    start_date = resolved_end - timedelta(days=max(1, int(config.DASH_DEFAULT_RANGE_DAYS)) - 1)
    return build_dashboard_date_context(start_date.isoformat(), resolved_end.isoformat())


def _materialize_family_rows(family: str, ctx: DashboardDateContext) -> list[DashboardV2FactRow]:
    rows: list[DashboardV2FactRow] = []
    for widget_id in EXACT_FACT_BACKED_WIDGET_IDS:
        coverage = WIDGET_COVERAGE_BY_ID[widget_id]
        if family not in coverage.fact_families:
            continue
        builder = _WIDGET_FACT_BUILDERS.get(widget_id)
        if builder is None:
            continue
        payload = builder(ctx)
        rows.append(
            DashboardV2FactRow(
                row_key=widget_id,
                payload_json={
                    **_row_payload(widget_id, ctx, payload),
                    "factFamily": family,
                    "declaredFactFamilies": list(coverage.fact_families),
                },
                source_event_at=_utc_day_start(ctx.from_date),
                content_type="widget_snapshot" if family == "content" else None,
            )
        )
    return rows


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
                },
                source_watermark=source_watermark,
            )
            results.append({"widgetId": widget_id, "storageKey": storage_key, "status": "ready"})
        except Exception as exc:
            logger.warning(f"Dashboard V2 secondary materialization failed | widget={widget_id} error={exc}")
            results.append({"widgetId": widget_id, "storageKey": storage_key, "status": "failed", "error": str(exc)})
    try:
        payload = topic_overviews.get_topic_overviews_snapshot(force_refresh=False)
        store.upsert_secondary_materialization(
            storage_key="topic_overviews_v2",
            widget_id="topic_overviews_v2",
            window_start=ctx.from_date,
            window_end=ctx.to_date,
            payload_json=payload,
            meta_json={"source": "legacy_materialized_snapshot", "rangeAware": False, "plannedUpgrade": "PR2B"},
            source_watermark=source_watermark,
        )
        results.append({"widgetId": "topic_overviews_v2", "storageKey": "topic_overviews_v2", "status": "ready"})
    except Exception as exc:
        logger.warning(f"Dashboard V2 topic overview materialization failed: {exc}")
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
            meta_json={"mode": mode},
        )
        rows_inserted = 0
        days_processed = 0
        try:
            for fact_day in _iter_days(coverage_start, resolved_end):
                ctx = build_dashboard_date_context(fact_day.isoformat(), fact_day.isoformat())
                rows = _materialize_family_rows(family, ctx)
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
                    reason=f"{mode}:{family}:{fact_day.isoformat()}",
                )
                days_processed += 1
            store.complete_fact_run(
                run_id,
                status="completed",
                source_watermark=source_watermark,
                meta_json={"rowsInserted": rows_inserted, "daysProcessed": days_processed, "mode": mode},
            )
            family_runs.append(
                {
                    "factFamily": family,
                    "runId": run_id,
                    "status": "completed",
                    "rowsInserted": rows_inserted,
                    "daysProcessed": days_processed,
                }
            )
        except Exception as exc:
            store.complete_fact_run(
                run_id,
                status="failed",
                source_watermark=source_watermark,
                error=str(exc),
                meta_json={"rowsInserted": rows_inserted, "daysProcessed": days_processed, "mode": mode},
            )
            logger.exception("Dashboard V2 foundation materialization failed | family={} error={}", family, exc)
            family_runs.append(
                {
                    "factFamily": family,
                    "runId": run_id,
                    "status": "failed",
                    "rowsInserted": rows_inserted,
                    "daysProcessed": days_processed,
                    "error": str(exc),
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
