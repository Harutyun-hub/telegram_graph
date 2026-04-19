from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from typing import Any, Callable

from loguru import logger

import config
from api import aggregator
from api.dashboard_dates import DashboardDateContext, build_dashboard_date_context
from api.dashboard_v2_assembler import DashboardV2FactsNotReadyError, assemble_dashboard_v2_exact
from api.dashboard_v2_registry import (
    ALL_WIDGET_IDS,
    DIRECT_SOURCE_TRUTH_WIDGET_IDS,
    RAW_SNAPSHOT_FIELDS_BY_WIDGET_ID,
    get_widget_coverage,
    required_fact_families_for_widgets,
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
) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    validation = _new_validation(SOURCE_TRUTH_VALIDATION_MODE)
    builder = _direct_truth_builders()[widget_id]
    result = _execute_with_timeout(
        label=f"source-truth-{widget_id}",
        timeout_seconds=float(config.DASH_V2_COMPARE_SOURCE_TRUTH_TIMEOUT_SECONDS),
        fn=lambda: builder(ctx),
    )
    if result["status"] == "timeout":
        validation["semanticStatus"] = "fail"
        validation["blockingReasons"] = [BLOCKING_REASON_SOURCE_TRUTH_TIMEOUT]
        validation["error"] = result["error"]
        return None, _summarize_widget(widget_id, None), validation
    if result["status"] == "error":
        validation["semanticStatus"] = "fail"
        validation["blockingReasons"] = [BLOCKING_REASON_SOURCE_TRUTH_ERROR]
        validation["error"] = result["error"]
        return None, _summarize_widget(widget_id, None), validation

    payload = result["value"]
    summary = _summarize_widget(widget_id, payload)
    validation["semanticStatus"] = "pass"
    if not _summaries_match_for_source_truth(widget_id, v2_summary, summary):
        validation["semanticStatus"] = "fail"
        validation["blockingReasons"] = [BLOCKING_REASON_SOURCE_TRUTH_MISMATCH]
    return payload, summary, validation


def _validate_fact_invariant_widget(
    *,
    store: DashboardV2Store,
    ctx: DashboardDateContext,
    widget_id: str,
    raw_payload: Any,
    v2_summary: dict[str, Any],
    readiness_cache: dict[tuple[str, ...], dict[str, Any]],
    min_fact_version: int,
) -> dict[str, Any]:
    validation = _new_validation(FACT_INVARIANT_VALIDATION_MODE)
    coverage = get_widget_coverage(widget_id)
    fact_families = required_fact_families_for_widgets([widget_id]) or coverage.fact_families
    cache_key = tuple(fact_families)
    if cache_key not in readiness_cache:
        readiness_cache[cache_key] = store.get_range_readiness(
            from_date=ctx.from_date,
            to_date=ctx.to_date,
            fact_families=fact_families,
            min_fact_version=min_fact_version,
        )
    readiness = readiness_cache[cache_key]

    blocking_reasons: list[str] = []
    if not readiness.get("ready"):
        blocking_reasons.append(BLOCKING_REASON_FACT_INVARIANT_FAILED)
        validation["readiness"] = {
            "missingFamilies": list(readiness.get("missingFactFamilies") or []),
            "missingDates": list(readiness.get("missingDates") or []),
            "degradedFamilies": list(readiness.get("degradedFactFamilies") or []),
            "degradedDates": list(readiness.get("degradedDates") or []),
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
            prefer_cached_exact_artifacts=False,
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
    direct_truth_payloads: dict[str, Any] = {}
    direct_truth_failures = 0
    readiness_cache: dict[tuple[str, ...], dict[str, Any]] = {}
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
            direct_payload, direct_summary, validation = _validate_source_truth_widget(
                widget_id=widget_id,
                ctx=ctx,
                v2_summary=v2_payload_summary,
            )
            if validation["semanticStatus"] == "fail":
                direct_truth_failures += 1
            direct_truth_payloads[widget_id] = direct_payload
            widget_diff["directTruth"] = direct_summary
        else:
            validation = _validate_fact_invariant_widget(
                store=store,
                ctx=ctx,
                widget_id=widget_id,
                raw_payload=raw_v2_payload,
                v2_summary=v2_payload_summary,
                readiness_cache=readiness_cache,
                min_fact_version=2,
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
