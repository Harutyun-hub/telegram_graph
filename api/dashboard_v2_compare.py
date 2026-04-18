from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from api import aggregator
from api.dashboard_dates import DashboardDateContext, build_dashboard_date_context
from api.dashboard_v2_registry import DIRECT_SOURCE_TRUTH_WIDGET_IDS
from api.dashboard_v2_store import DashboardV2Store
from api.queries import comparative, pulse, strategic


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


def _extract_old_path_widget(snapshot: dict[str, Any], widget_id: str) -> Any:
    mapping = {
        "community_brief": snapshot.get("communityBrief"),
        "community_health_score": snapshot.get("communityHealth"),
        "trending_topics_feed": snapshot.get("trendingTopics"),
        "conversation_trends": snapshot.get("trendLines"),
        "topic_lifecycle": snapshot.get("lifecycleStages"),
        "sentiment_by_topic": snapshot.get("sentimentByTopic"),
        "week_over_week_shifts": snapshot.get("weeklyShifts"),
    }
    return mapping.get(widget_id)


def _extract_v2_widget(payload: dict[str, Any], widget_id: str) -> Any:
    if not isinstance(payload, dict):
        return None
    widgets = payload.get("widgets")
    if isinstance(widgets, dict):
        return widgets.get(widget_id)
    return payload.get(widget_id)


def _summarize_widget(widget_id: str, payload: Any) -> dict[str, Any]:
    if payload is None:
        return {"present": False}
    if widget_id == "community_brief" and isinstance(payload, dict):
        return {
            "present": True,
            "messagesAnalyzed": payload.get("messagesAnalyzed"),
            "postsAnalyzedInWindow": payload.get("postsAnalyzedInWindow", payload.get("postsAnalyzed24h")),
            "commentScopesAnalyzedInWindow": payload.get(
                "commentScopesAnalyzedInWindow",
                payload.get("commentScopesAnalyzed24h"),
            ),
            "totalAnalysesInWindow": payload.get("totalAnalysesInWindow", payload.get("totalAnalyses24h")),
        }
    if widget_id == "community_health_score" and isinstance(payload, dict):
        components = payload.get("components")
        return {
            "present": True,
            "currentScore": payload.get("currentScore", payload.get("score")),
            "componentCount": len(components or []) if isinstance(components, list) else len((components or {}).get("en", [])) if isinstance(components, dict) else 0,
        }
    if widget_id == "trending_topics_feed" and isinstance(payload, list):
        return {
            "present": True,
            "count": len(payload),
            "topItems": [str((item or {}).get("topic") or "") for item in payload[:5] if isinstance(item, dict)],
        }
    if widget_id == "conversation_trends" and isinstance(payload, list):
        return {"present": True, "count": len(payload)}
    if widget_id == "topic_lifecycle" and isinstance(payload, list):
        return {
            "present": True,
            "stageCount": len(payload),
            "topicCount": sum(len((stage or {}).get("topics") or []) for stage in payload if isinstance(stage, dict)),
        }
    if widget_id in {"sentiment_by_topic", "week_over_week_shifts"} and isinstance(payload, list):
        return {"present": True, "count": len(payload)}
    if isinstance(payload, dict):
        return {"present": True, "keyCount": len(payload)}
    if isinstance(payload, list):
        return {"present": True, "count": len(payload)}
    return {"present": True, "value": payload}


def build_direct_truth_snapshot(ctx: DashboardDateContext) -> dict[str, Any]:
    builders = _direct_truth_builders()
    return {widget_id: builders[widget_id](ctx) for widget_id in DIRECT_SOURCE_TRUTH_WIDGET_IDS}


def run_dashboard_v2_compare(
    store: DashboardV2Store,
    *,
    from_value: str,
    to_value: str,
) -> dict[str, Any]:
    ctx = build_dashboard_date_context(from_value, to_value)
    old_snapshot = aggregator.get_dashboard_data(ctx)
    artifact = store.get_range_artifact(ctx.cache_key)
    v2_payload = (artifact or {}).get("payload_json") if isinstance(artifact, dict) else None
    direct_truth = build_direct_truth_snapshot(ctx)

    widget_diffs: dict[str, Any] = {}
    for widget_id in DIRECT_SOURCE_TRUTH_WIDGET_IDS:
        old_payload = _extract_old_path_widget(old_snapshot, widget_id)
        v2_widget_payload = _extract_v2_widget(v2_payload or {}, widget_id)
        truth_payload = direct_truth.get(widget_id)
        widget_diffs[widget_id] = {
            "oldPath": _summarize_widget(widget_id, old_payload),
            "v2Artifact": _summarize_widget(widget_id, v2_widget_payload),
            "directTruth": _summarize_widget(widget_id, truth_payload),
        }

    compare_id = store.create_compare_run(
        cache_key=ctx.cache_key,
        from_date=ctx.from_date,
        to_date=ctx.to_date,
        old_path_meta={
            "generatedAt": _utc_now_iso(),
            "from": ctx.from_date.isoformat(),
            "to": ctx.to_date.isoformat(),
        },
        v2_meta={
            "status": "artifact_found" if artifact else "artifact_missing",
            "cacheKey": ctx.cache_key,
            "factWatermark": (artifact or {}).get("fact_watermark") if isinstance(artifact, dict) else None,
            "materializedAt": (artifact or {}).get("materialized_at") if isinstance(artifact, dict) else None,
        },
        direct_truth_meta={
            "generatedAt": _utc_now_iso(),
            "widgetCount": len(direct_truth),
            "sourceDateSemantics": True,
        },
        diff_json=widget_diffs,
    )
    return {
        "compareId": compare_id,
        "cacheKey": ctx.cache_key,
        "from": ctx.from_date.isoformat(),
        "to": ctx.to_date.isoformat(),
        "widgetDiffs": widget_diffs,
        "artifactFound": bool(artifact),
    }
