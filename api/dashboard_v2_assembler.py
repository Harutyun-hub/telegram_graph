from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from api.dashboard_dates import DashboardDateContext
from api.dashboard_v2_registry import (
    FACT_FAMILIES,
    FULL_DASHBOARD_REQUIRED_FACT_FAMILIES,
    RAW_SNAPSHOT_FIELDS_BY_WIDGET_ID,
    SECONDARY_STORAGE_KEY_BY_WIDGET_ID,
)
from api.dashboard_v2_secondary import NON_LLM_REQUEST_TIME_SECONDARY_WIDGET_IDS, build_request_time_secondary_snapshot
from api.dashboard_v2_store import (
    DashboardV2Store,
    compute_max_fact_watermark,
    compute_stale_fact_families,
    same_key_last_known_good_allowed,
)


DASHBOARD_V2_ARTIFACT_VERSION = 1
DASHBOARD_V2_FACT_VERSION = 2
_MEMORY_EXACT_CACHE: dict[str, dict[str, Any]] = {}


class DashboardV2FactsNotReadyError(RuntimeError):
    def __init__(self, detail: dict[str, Any]):
        super().__init__("dashboard_v2_facts_not_ready")
        self.detail = detail


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


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


def _empty_snapshot() -> dict[str, Any]:
    return {
        "communityHealth": {"currentScore": 0, "weekAgoScore": 0, "history": [], "components": {}},
        "trendingTopics": [],
        "trendingNewTopics": [],
        "communityBrief": {},
        "topicBubbles": [],
        "trendLines": [],
        "trendData": [],
        "questionCategories": [],
        "questionBriefs": [],
        "lifecycleStages": [],
        "problemBriefs": [],
        "serviceGapBriefs": [],
        "problems": [],
        "serviceGaps": [],
        "satisfactionAreas": [],
        "moodData": [],
        "moodConfig": {},
        "urgencySignals": [],
        "communityChannels": [],
        "keyVoices": [],
        "hourlyActivity": [],
        "weeklyActivity": [],
        "recommendations": [],
        "viralTopics": [],
        "personas": [],
        "interests": [],
        "origins": [],
        "integrationData": [],
        "integrationLevels": [],
        "integrationSeriesConfig": [],
        "emergingInterests": [],
        "retentionFactors": [],
        "churnSignals": [],
        "growthFunnel": [],
        "decisionStages": [],
        "newVsReturningVoiceWidget": {},
        "businessOpportunities": [],
        "businessOpportunityBriefs": [],
        "jobSeeking": [],
        "jobTrends": [],
        "housingData": [],
        "housingHotTopics": [],
        "weeklyShifts": [],
        "sentimentByTopic": [],
        "topPosts": [],
        "contentTypePerformance": [],
        "vitalityIndicators": {},
        "qaGap": {"totalQuestions": 0, "answered": 0},
    }


def _identity_key(item: Any, *, fallback_prefix: str, index: int) -> str:
    if isinstance(item, dict):
        for key in (
            "id",
            "topic",
            "sourceTopic",
            "name",
            "key",
            "label",
            "stage",
            "area",
            "serviceNeed",
            "problem",
            "title",
            "role",
            "factor",
            "type",
            "contentType",
            "category",
        ):
            value = _as_str(item.get(key))
            if value:
                return value.lower()
    return f"{fallback_prefix}:{index}"


def _merge_item_dict(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        current = merged.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = _merge_item_dict(current, value)
        elif isinstance(current, list) and isinstance(value, list):
            seen: set[str] = set()
            combined: list[Any] = []
            for item in current + value:
                marker = json.dumps(item, sort_keys=True, ensure_ascii=True, default=str)
                if marker in seen:
                    continue
                seen.add(marker)
                combined.append(item)
            merged[key] = combined
        elif isinstance(current, (int, float)) and isinstance(value, (int, float)):
            merged[key] = current + value
        elif current in (None, "", [], {}):
            merged[key] = value
        else:
            merged[key] = value
    return merged


def _merge_list_field(values: list[tuple[date, Any]], *, field_name: str) -> list[Any]:
    merged: dict[str, dict[str, Any]] = {}
    scalar_values: list[Any] = []
    for _fact_date, raw_value in values:
        items = raw_value if isinstance(raw_value, list) else [raw_value]
        for index, item in enumerate(items):
            if isinstance(item, dict):
                identity = _identity_key(item, fallback_prefix=field_name, index=index)
                merged[identity] = _merge_item_dict(merged.get(identity, {}), item)
            elif item not in scalar_values:
                scalar_values.append(item)
    if merged:
        return list(merged.values())
    return scalar_values


def _merge_lifecycle(values: list[tuple[date, Any]]) -> list[dict[str, Any]]:
    lifecycle_rows: dict[str, dict[str, Any]] = {}
    for _fact_date, raw_value in values:
        flattened: list[dict[str, Any]] = []
        for index, entry in enumerate(_as_list(raw_value)):
            if not isinstance(entry, dict):
                continue
            if isinstance(entry.get("topics"), list):
                stage_name = _as_str(entry.get("stage"), "_stage")
                for topic_index, topic in enumerate(_as_list(entry.get("topics"))):
                    if not isinstance(topic, dict):
                        continue
                    merged_topic = dict(topic)
                    if stage_name and not _as_str(merged_topic.get("stage")):
                        merged_topic["stage"] = stage_name
                    if _as_str(entry.get("color")) and not _as_str(merged_topic.get("color")):
                        merged_topic["color"] = _as_str(entry.get("color"))
                    flattened.append(merged_topic)
            else:
                flattened.append(dict(entry))
        for index, item in enumerate(flattened):
            stage_name = _as_str(item.get("stage"), "_stage")
            identity = _identity_key(
                {
                    "stage": stage_name,
                    "sourceTopic": item.get("sourceTopic"),
                    "topic": item.get("topic"),
                    "name": item.get("name"),
                },
                fallback_prefix=stage_name,
                index=index,
            )
            lifecycle_rows[identity] = _merge_item_dict(lifecycle_rows.get(identity, {}), item)
    return list(lifecycle_rows.values())


def _merge_community_brief(values: list[tuple[date, Any]], snapshot: dict[str, Any], materialized_at: str | None) -> dict[str, Any]:
    messages = 0.0
    posts = 0.0
    comments = 0.0
    weighted_positive = 0.0
    weighted_negative = 0.0
    weight_total = 0.0
    latest: dict[str, Any] = {}
    for _fact_date, raw_value in values:
        item = _as_dict(raw_value)
        if not item:
            continue
        latest = item
        item_posts = _as_float(item.get("postsAnalyzedInWindow", item.get("postsAnalyzed24h")))
        item_comments = _as_float(item.get("commentScopesAnalyzedInWindow", item.get("commentScopesAnalyzed24h")))
        item_total = _as_float(item.get("totalAnalysesInWindow", item.get("totalAnalyses24h", item_posts + item_comments)))
        messages += _as_float(item.get("messagesAnalyzed"), item_total)
        posts += item_posts
        comments += item_comments
        weight = max(item_total, 1.0)
        weighted_positive += _as_float(item.get("positiveIntentPct24h")) * weight
        weighted_negative += _as_float(item.get("negativeIntentPct24h")) * weight
        weight_total += weight
    top_topics = []
    for topic_item in _as_list(snapshot.get("trendingTopics"))[:5]:
        if isinstance(topic_item, dict):
            top_topics.append(_as_str(topic_item.get("topic") or topic_item.get("name")))
    brief_text = (
        latest.get("mainBrief")
        if isinstance(latest.get("mainBrief"), dict)
        else {
            "en": f"Selected window snapshot ({len(values)}d): {int(posts)} posts and {int(comments)} analyzed comment scopes. Focus areas: {', '.join([item for item in top_topics if item]) or 'core community topics'}.",
            "ru": f"Снимок выбранного окна ({len(values)}д): {int(posts)} постов и {int(comments)} проанализированных комментариев. Основные темы: {', '.join([item for item in top_topics if item]) or 'ключевые темы сообщества'}.",
        }
    )
    expanded = latest.get("expandedBrief")
    if not isinstance(expanded, dict):
        expanded = {
            "en": [brief_text.get("en", "")],
            "ru": [brief_text.get("ru", "")],
        }
    return {
        "messagesAnalyzed": int(messages),
        "updatedMinutesAgo": 0 if materialized_at else int(latest.get("updatedMinutesAgo", 0) or 0),
        "postsAnalyzedInWindow": int(posts),
        "commentScopesAnalyzedInWindow": int(comments),
        "totalAnalysesInWindow": int(posts + comments),
        "postsAnalyzed24h": int(posts),
        "commentScopesAnalyzed24h": int(comments),
        "totalAnalyses24h": int(posts + comments),
        "positiveIntentPct24h": round(weighted_positive / weight_total) if weight_total else 0,
        "negativeIntentPct24h": round(weighted_negative / weight_total) if weight_total else 0,
        "mainBrief": brief_text,
        "expandedBrief": expanded,
    }


def _merge_community_health(values: list[tuple[date, Any]]) -> dict[str, Any]:
    current_scores: list[float] = []
    history: list[dict[str, Any]] = []
    latest_components: Any = {}
    for fact_date, raw_value in values:
        item = _as_dict(raw_value)
        score = _as_float(item.get("currentScore", item.get("score")))
        if score:
            current_scores.append(score)
            history.append({"time": fact_date.isoformat(), "score": round(score)})
        if item.get("components"):
            latest_components = item.get("components")
    current_score = round(sum(current_scores) / len(current_scores)) if current_scores else 0
    week_ago_score = round(current_scores[0]) if current_scores else current_score
    return {
        "currentScore": current_score,
        "weekAgoScore": week_ago_score,
        "history": history,
        "components": latest_components or {},
    }


def _merge_new_vs_returning(values: list[tuple[date, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for _fact_date, raw_value in values:
        merged = _merge_item_dict(merged, _as_dict(raw_value))
    return merged


def _merge_mood_config(values: list[tuple[date, Any]]) -> dict[str, Any]:
    sentiments: list[str] = []
    for _fact_date, raw_value in values:
        for sentiment in _as_list(_as_dict(raw_value).get("sentiments")):
            text = _as_str(sentiment)
            if text and text not in sentiments:
                sentiments.append(text)
    return {"sentiments": sentiments}


def _assemble_field(field_name: str, values: list[tuple[date, Any]], snapshot: dict[str, Any], materialized_at: str | None) -> Any:
    if field_name == "communityBrief":
        return _merge_community_brief(values, snapshot, materialized_at)
    if field_name == "communityHealth":
        return _merge_community_health(values)
    if field_name == "lifecycleStages":
        return _merge_lifecycle(values)
    if field_name == "newVsReturningVoiceWidget":
        return _merge_new_vs_returning(values)
    if field_name == "moodConfig":
        return _merge_mood_config(values)
    if any(isinstance(value, list) for _, value in values):
        return _merge_list_field(values, field_name=field_name)
    if any(isinstance(value, dict) for _, value in values):
        merged: dict[str, Any] = {}
        for _fact_date, value in values:
            merged = _merge_item_dict(merged, _as_dict(value))
        return merged
    return values[-1][1] if values else None


def _collect_field_values(rows_by_family: dict[str, list[dict[str, Any]]]) -> dict[str, list[tuple[date, Any]]]:
    collected: dict[str, list[tuple[date, Any]]] = {}
    seen: set[str] = set()
    for rows in rows_by_family.values():
        for row in rows:
            fact_date = row.get("fact_date")
            payload = _as_dict(row.get("payload_json"))
            hints = _as_dict(payload.get("factHints"))
            widget_payloads = _as_dict(hints.get("widgetPayloads"))
            for field_name, value in widget_payloads.items():
                marker = json.dumps(
                    {
                        "field": field_name,
                        "factDate": str(fact_date),
                        "value": value,
                    },
                    sort_keys=True,
                    ensure_ascii=True,
                    default=str,
                )
                if marker in seen:
                    continue
                seen.add(marker)
                collected.setdefault(field_name, []).append((fact_date, value))
    return collected


def assemble_exact_fact_snapshot(
    *,
    ctx: DashboardDateContext,
    rows_by_family: dict[str, list[dict[str, Any]]],
    materialized_at: str | None,
) -> dict[str, Any]:
    snapshot = _empty_snapshot()
    field_values = _collect_field_values(rows_by_family)
    for field_name, values in field_values.items():
        snapshot[field_name] = _assemble_field(field_name, values, snapshot, materialized_at)
    if not snapshot.get("trendingNewTopics"):
        snapshot["trendingNewTopics"] = [
            item for item in _as_list(snapshot.get("trendingTopics")) if isinstance(item, dict) and _as_float(item.get("trend")) > 0
        ]
    if not snapshot.get("trendData"):
        snapshot["trendData"] = list(_as_list(snapshot.get("trendLines")))
    return snapshot


def _merge_partial_snapshot(snapshot: dict[str, Any], partial: dict[str, Any]) -> None:
    for key, value in partial.items():
        if key not in snapshot:
            snapshot[key] = value
            continue
        if isinstance(snapshot[key], list) and isinstance(value, list):
            snapshot[key] = _merge_list_field([(date.today(), snapshot[key]), (date.today(), value)], field_name=key)
        elif isinstance(snapshot[key], dict) and isinstance(value, dict):
            snapshot[key] = _merge_item_dict(snapshot[key], value)
        elif snapshot[key] in (None, "", [], {}):
            snapshot[key] = value


def _secondary_dependency_names() -> dict[str, tuple[str, str]]:
    return {
        f"secondary:{widget_id}": (SECONDARY_STORAGE_KEY_BY_WIDGET_ID[widget_id], widget_id)
        for widget_id in NON_LLM_REQUEST_TIME_SECONDARY_WIDGET_IDS
        if widget_id in SECONDARY_STORAGE_KEY_BY_WIDGET_ID
    }


def _materialized_at_from_rows(rows_by_family: dict[str, list[dict[str, Any]]]) -> str | None:
    candidates: list[datetime] = []
    for rows in rows_by_family.values():
        for row in rows:
            value = row.get("materialized_at")
            if isinstance(value, datetime):
                candidates.append(value.astimezone(timezone.utc))
            elif value:
                try:
                    candidates.append(datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc))
                except Exception:
                    continue
    return max(candidates).isoformat() if candidates else None


def _request_range_not_ready_detail(
    *,
    ctx: DashboardDateContext,
    readiness: dict[str, Any],
    route_readiness: dict[str, Any],
) -> dict[str, Any]:
    return {
        "code": "v2_facts_not_ready",
        "requestedFrom": ctx.from_date.isoformat(),
        "requestedTo": ctx.to_date.isoformat(),
        "minFactVersion": DASHBOARD_V2_FACT_VERSION,
        "availabilityStart": readiness.get("availabilityStart") or route_readiness.get("coverageStart"),
        "availabilityEnd": readiness.get("availabilityEnd") or route_readiness.get("coverageEnd"),
        "missingFactFamilies": readiness.get("missingFactFamilies") or route_readiness.get("missingFamilies") or [],
        "missingDates": readiness.get("missingDates") or [],
        "degradedFactFamilies": readiness.get("degradedFactFamilies") or route_readiness.get("degradedFamilies") or [],
        "degradedDates": readiness.get("degradedDates") or [],
    }


@dataclass
class DashboardV2AssemblyResult:
    snapshot: dict[str, Any]
    cache_status: str
    cache_source: str
    range_resolution_path: str
    is_stale: bool
    fact_version: int
    artifact_version: int
    fact_watermark: str | None
    materialized_at: str | None
    stale_fact_families: list[str]


def _cache_get(cache_key: str) -> dict[str, Any] | None:
    return _MEMORY_EXACT_CACHE.get(cache_key)


def _cache_put(cache_key: str, payload: dict[str, Any]) -> None:
    _MEMORY_EXACT_CACHE[cache_key] = dict(payload)


def _artifact_matches_exact_context(artifact: dict[str, Any] | None, ctx: DashboardDateContext) -> bool:
    return bool(
        artifact
        and str(artifact.get("cache_key") or "") == ctx.cache_key
        and getattr(artifact.get("from_date"), "isoformat", lambda: str(artifact.get("from_date")))() == ctx.from_date.isoformat()
        and getattr(artifact.get("to_date"), "isoformat", lambda: str(artifact.get("to_date")))() == ctx.to_date.isoformat()
        and str(artifact.get("range_mode") or "") == "exact"
    )


def assemble_dashboard_v2_exact(
    store: DashboardV2Store,
    *,
    ctx: DashboardDateContext,
) -> DashboardV2AssemblyResult:
    route_readiness = store.summarize_v2_route_readiness(
        min_fact_version=DASHBOARD_V2_FACT_VERSION,
        from_date=ctx.from_date,
        to_date=ctx.to_date,
    )
    range_readiness = store.get_range_readiness(
        from_date=ctx.from_date,
        to_date=ctx.to_date,
        fact_families=FULL_DASHBOARD_REQUIRED_FACT_FAMILIES,
        min_fact_version=DASHBOARD_V2_FACT_VERSION,
    )
    if not bool(route_readiness.get("v2RouteReady")) or not bool(range_readiness.get("ready")):
        raise DashboardV2FactsNotReadyError(
            _request_range_not_ready_detail(
                ctx=ctx,
                readiness=range_readiness,
                route_readiness=route_readiness,
            )
        )

    secondary_dependencies = _secondary_dependency_names()
    latest_dependency_watermarks = store.latest_dependency_watermarks_for_range(
        from_date=ctx.from_date,
        to_date=ctx.to_date,
        fact_families=FULL_DASHBOARD_REQUIRED_FACT_FAMILIES,
        secondary_dependencies=secondary_dependencies,
        min_fact_version=DASHBOARD_V2_FACT_VERSION,
    )

    cached = _cache_get(ctx.cache_key)
    if _artifact_matches_exact_context(cached, ctx):
        stale_fact_families = compute_stale_fact_families(cached.get("dependency_watermarks") or {}, latest_dependency_watermarks)
        if not stale_fact_families and not bool(cached.get("is_stale")):
            return DashboardV2AssemblyResult(
                snapshot=dict(cached.get("payload_json") or {}),
                cache_status="memory_exact",
                cache_source="memory",
                range_resolution_path="v2_memory_exact",
                is_stale=False,
                fact_version=DASHBOARD_V2_FACT_VERSION,
                artifact_version=int(cached.get("artifact_version") or DASHBOARD_V2_ARTIFACT_VERSION),
                fact_watermark=_as_str(cached.get("fact_watermark")) or None,
                materialized_at=_as_str(cached.get("materialized_at")) or None,
                stale_fact_families=[],
            )

    artifact = store.get_range_artifact(ctx.cache_key)
    if _artifact_matches_exact_context(artifact, ctx):
        stale_fact_families = sorted(
            set(compute_stale_fact_families(artifact.get("dependency_watermarks") or {}, latest_dependency_watermarks))
            | set(_as_list(artifact.get("stale_fact_families")))
        )
        if not stale_fact_families and not bool(artifact.get("is_stale")):
            payload_json = dict(artifact.get("payload_json") or {})
            _cache_put(
                ctx.cache_key,
                {
                    "cache_key": ctx.cache_key,
                    "from_date": ctx.from_date,
                    "to_date": ctx.to_date,
                    "range_mode": "exact",
                    "payload_json": payload_json,
                    "dependency_watermarks": latest_dependency_watermarks,
                    "fact_watermark": compute_max_fact_watermark(latest_dependency_watermarks).isoformat()
                    if compute_max_fact_watermark(latest_dependency_watermarks)
                    else None,
                    "artifact_version": int(artifact.get("artifact_version") or DASHBOARD_V2_ARTIFACT_VERSION),
                    "materialized_at": _as_str(artifact.get("materialized_at")) or None,
                    "is_stale": False,
                },
            )
            return DashboardV2AssemblyResult(
                snapshot=payload_json,
                cache_status="persisted_exact",
                cache_source="persisted",
                range_resolution_path="v2_persisted_exact",
                is_stale=False,
                fact_version=DASHBOARD_V2_FACT_VERSION,
                artifact_version=int(artifact.get("artifact_version") or DASHBOARD_V2_ARTIFACT_VERSION),
                fact_watermark=_as_str(artifact.get("fact_watermark")) or None,
                materialized_at=_as_str(artifact.get("materialized_at")) or None,
                stale_fact_families=[],
            )
        newer_exact_exists = store.exact_artifact_has_newer_same_key(
            cache_key=ctx.cache_key,
            materialized_at=artifact.get("materialized_at"),
        )
        if same_key_last_known_good_allowed(
            request_from=ctx.from_date,
            request_to=ctx.to_date,
            artifact_from=artifact.get("from_date"),
            artifact_to=artifact.get("to_date"),
            artifact_is_stale=bool(stale_fact_families or artifact.get("is_stale")),
            newer_exact_exists=newer_exact_exists,
        ):
            return DashboardV2AssemblyResult(
                snapshot=dict(artifact.get("payload_json") or {}),
                cache_status="exact_last_known_good",
                cache_source="persisted",
                range_resolution_path="v2_persisted_exact_last_known_good",
                is_stale=True,
                fact_version=DASHBOARD_V2_FACT_VERSION,
                artifact_version=int(artifact.get("artifact_version") or DASHBOARD_V2_ARTIFACT_VERSION),
                fact_watermark=_as_str(artifact.get("fact_watermark")) or None,
                materialized_at=_as_str(artifact.get("materialized_at")) or None,
                stale_fact_families=stale_fact_families,
            )

    build_started_at = time.perf_counter()
    rows_by_family = {
        family: store.fetch_fact_rows_for_range(
            fact_family=family,
            from_date=ctx.from_date,
            to_date=ctx.to_date,
            min_fact_version=DASHBOARD_V2_FACT_VERSION,
        )
        for family in FACT_FAMILIES
    }
    materialized_at = _materialized_at_from_rows(rows_by_family)
    snapshot = assemble_exact_fact_snapshot(ctx=ctx, rows_by_family=rows_by_family, materialized_at=materialized_at)

    for widget_id in NON_LLM_REQUEST_TIME_SECONDARY_WIDGET_IDS:
        storage_key = SECONDARY_STORAGE_KEY_BY_WIDGET_ID.get(widget_id)
        if not storage_key:
            continue
        secondary_row = store.get_exact_secondary_materialization(
            storage_key=storage_key,
            widget_id=widget_id,
            window_start=ctx.from_date,
            window_end=ctx.to_date,
        )
        if secondary_row and str(secondary_row.get("status") or "ready") == "ready":
            _merge_partial_snapshot(snapshot, _as_dict(secondary_row.get("payload_json")))
            continue
        partial = build_request_time_secondary_snapshot(widget_id, snapshot)
        store.upsert_secondary_materialization(
            storage_key=storage_key,
            widget_id=widget_id,
            window_start=ctx.from_date,
            window_end=ctx.to_date,
            payload_json=partial,
            meta_json={
                "source": "facts_only_request_builder",
                "llmUsed": False,
                "networkUsed": False,
                "deterministic": True,
                "materializedAt": _utc_now_iso(),
            },
            source_watermark=compute_max_fact_watermark(latest_dependency_watermarks),
        )
        store.mark_secondary_materialization_stale(
            dependency_name=f"secondary:{widget_id}",
            window_start=ctx.from_date,
            window_end=ctx.to_date,
            new_watermark=compute_max_fact_watermark(latest_dependency_watermarks),
            reason=f"request_build:{widget_id}:{ctx.cache_key}",
        )
        _merge_partial_snapshot(snapshot, partial)

    latest_dependency_watermarks = store.latest_dependency_watermarks_for_range(
        from_date=ctx.from_date,
        to_date=ctx.to_date,
        fact_families=FULL_DASHBOARD_REQUIRED_FACT_FAMILIES,
        secondary_dependencies=secondary_dependencies,
        min_fact_version=DASHBOARD_V2_FACT_VERSION,
    )
    fact_watermark = compute_max_fact_watermark(latest_dependency_watermarks)
    materialized_at = _utc_now_iso()
    store.upsert_range_artifact(
        cache_key=ctx.cache_key,
        from_date=ctx.from_date,
        to_date=ctx.to_date,
        range_mode="exact",
        payload_json=snapshot,
        dependency_watermarks=latest_dependency_watermarks,
        artifact_version=DASHBOARD_V2_ARTIFACT_VERSION,
        is_stale=False,
        stale_fact_families=[],
        stale_reason=None,
    )
    _cache_put(
        ctx.cache_key,
        {
            "cache_key": ctx.cache_key,
            "from_date": ctx.from_date,
            "to_date": ctx.to_date,
            "range_mode": "exact",
            "payload_json": snapshot,
            "dependency_watermarks": latest_dependency_watermarks,
            "fact_watermark": fact_watermark.isoformat() if fact_watermark else None,
            "artifact_version": DASHBOARD_V2_ARTIFACT_VERSION,
            "materialized_at": materialized_at,
            "is_stale": False,
        },
    )
    _ = build_started_at  # retained for future detailed timing without introducing unused warnings in tests
    return DashboardV2AssemblyResult(
        snapshot=snapshot,
        cache_status="assembled_exact_from_facts",
        cache_source="assembled",
        range_resolution_path="v2_assembled_exact_from_facts",
        is_stale=False,
        fact_version=DASHBOARD_V2_FACT_VERSION,
        artifact_version=DASHBOARD_V2_ARTIFACT_VERSION,
        fact_watermark=fact_watermark.isoformat() if fact_watermark else None,
        materialized_at=materialized_at,
        stale_fact_families=[],
    )
