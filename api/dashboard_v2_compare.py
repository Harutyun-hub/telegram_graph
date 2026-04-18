from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from api import aggregator
from api.dashboard_dates import DashboardDateContext, build_dashboard_date_context
from api.dashboard_v2_assembler import DashboardV2FactsNotReadyError, assemble_dashboard_v2_exact
from api.dashboard_v2_registry import ALL_WIDGET_IDS, DIRECT_SOURCE_TRUTH_WIDGET_IDS, RAW_SNAPSHOT_FIELDS_BY_WIDGET_ID
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


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


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
        for candidate_key in ("trendingTopics", "topics", "questionCategories"):
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
            for key in ("topic", "name", "label", "stage", "category", "item"):
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


def _present_widget_count(widget_diffs: dict[str, Any], key: str) -> int:
    return sum(1 for diff in widget_diffs.values() if bool(_as_dict(diff.get(key)).get("present")))


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
    direct_truth = build_direct_truth_snapshot(ctx)

    v2_status = "ready"
    v2_meta: dict[str, Any]
    v2_snapshot: dict[str, Any] = {}
    try:
        v2_result = assemble_dashboard_v2_exact(
            store,
            ctx=ctx,
            allow_stale_exact_last_known_good=False,
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
        }
    except DashboardV2FactsNotReadyError as exc:
        v2_status = "facts_not_ready"
        v2_meta = {
            "status": v2_status,
            "cacheKey": ctx.cache_key,
            "detail": exc.detail,
        }

    widget_diffs: dict[str, Any] = {}
    for widget_id in ALL_WIDGET_IDS:
        old_payload = _extract_snapshot_widget(old_snapshot, widget_id)
        v2_payload = _extract_snapshot_widget(v2_snapshot, widget_id)
        widget_diff = {
            "oldPath": _summarize_widget(widget_id, old_payload),
            "dashboardV2": _summarize_widget(widget_id, v2_payload),
        }
        if widget_id in DIRECT_SOURCE_TRUTH_WIDGET_IDS:
            widget_diff["directTruth"] = _summarize_widget(widget_id, direct_truth.get(widget_id))
        widget_diffs[widget_id] = widget_diff

    old_path_meta = {
        "generatedAt": _utc_now_iso(),
        "from": ctx.from_date.isoformat(),
        "to": ctx.to_date.isoformat(),
        "widgetCount": _present_widget_count(widget_diffs, "oldPath"),
    }
    direct_truth_meta = {
        "generatedAt": _utc_now_iso(),
        "widgetCount": len(direct_truth),
        "sourceDateSemantics": True,
        "widgetIds": list(DIRECT_SOURCE_TRUTH_WIDGET_IDS),
    }
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
            "directTruth": len(direct_truth),
        },
        "v2Status": v2_status,
        "v2Meta": v2_meta,
    }
