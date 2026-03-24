"""
aggregator.py — Assembles the full AppData response from all query modules.

Professional reliability features:
- cache-first with stale-while-revalidate
- single-flight refresh lock to prevent stampede
- bounded parallel tier execution
- per-tier timeout with safe fallback
- atomic cache swap after full snapshot build
"""
from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from loguru import logger

from api import behavioral_briefs
from api.dashboard_dates import DashboardDateContext, build_dashboard_date_context
from api import opportunity_briefs
from api import question_briefs
from api.queries import actionable, behavioral, comparative, network, predictive, psychographic, pulse, strategic
from buffer.supabase_writer import SupabaseWriter


_supabase_writer = SupabaseWriter()


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


# ── Cache + reliability config ───────────────────────────────────────────────
CACHE_TTL_SECONDS = int(os.getenv("DASH_CACHE_TTL_SECONDS", "300"))
STALE_WHILE_REVALIDATE = _env_flag("DASH_STALE_WHILE_REVALIDATE_ENABLED", True)
PARALLEL_ENABLED = _env_flag("DASH_PARALLEL_ENABLED", True)
PARALLEL_MAX_WORKERS = max(1, min(int(os.getenv("DASH_PARALLEL_MAX_WORKERS", "4")), 8))
TIER_TIMEOUT_SECONDS = max(0.5, float(os.getenv("DASH_TIER_TIMEOUT_SECONDS", "10.0")))
WAIT_FOR_REFRESH_SECONDS = max(1.0, float(os.getenv("DASH_WAIT_FOR_REFRESH_SECONDS", "8.0")))
DETAIL_CACHE_TTL_SECONDS = max(30, int(os.getenv("DETAIL_CACHE_TTL_SECONDS", "180")))

# If one of these tiers falls back during refresh, prefer preserving an
# existing healthy cache instead of replacing it with an empty/degraded view.
CRITICAL_TIERS = {"pulse", "strategic"}


DashboardCacheMeta = Dict[str, object]
DashboardCacheEntry = Tuple[float, dict, DashboardCacheMeta]


_cache_entries: Dict[str, DashboardCacheEntry] = {}

_cache_lock = threading.Lock()

_detail_cache_lock = threading.Lock()
_detail_cache: Dict[str, Tuple[float, Any]] = {}
_tier_executor_lock = threading.Lock()


def _new_tier_executor() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(
        max_workers=PARALLEL_MAX_WORKERS,
        thread_name_prefix="dash-tier",
    )

# Module-level thread pool — reused across all dashboard builds.
# Replaces per-request ThreadPoolExecutor to avoid thread creation overhead
# and thread leak from shutdown(wait=False).
_tier_executor = _new_tier_executor()


def _submit_tier_futures(
    ordered: List[Tuple[str, Callable[[], dict]]],
) -> tuple[ThreadPoolExecutor, Dict[str, Any]]:
    with _tier_executor_lock:
        executor = _tier_executor
        futures = {name: executor.submit(_run_tier_builder, builder) for name, builder in ordered}
    return executor, futures


def _replace_tier_executor(stale_executor: ThreadPoolExecutor) -> None:
    """Rotate the shared executor when a timed-out task is still running."""
    global _tier_executor
    with _tier_executor_lock:
        if stale_executor is not _tier_executor:
            return
        _tier_executor = _new_tier_executor()
    stale_executor.shutdown(wait=False, cancel_futures=True)


def _fallback_tiers(tier_times: Dict[str, Optional[float]]) -> list[str]:
    return [name for name, duration in tier_times.items() if name != "derived" and duration is None]


def _critical_fallback_tiers(tier_names: list[str] | None) -> list[str]:
    if not tier_names:
        return []
    return [name for name in tier_names if name in CRITICAL_TIERS]


def _snapshot_has_core_pulse_data(snapshot: Optional[dict]) -> bool:
    if not isinstance(snapshot, dict) or not snapshot:
        return False
    brief = snapshot.get("communityBrief")
    health = snapshot.get("communityHealth")
    trending = snapshot.get("trendingTopics")

    if not isinstance(brief, dict) or not brief:
        return False
    if not isinstance(health, dict) or not health:
        return False
    if not isinstance(trending, list):
        return False

    has_brief_volume = any(
        key in brief
        for key in (
            "totalAnalyses24h",
            "postsAnalyzed24h",
            "postsLast24h",
            "commentsLast24h",
        )
    )
    has_health_shape = any(key in health for key in ("components", "score", "totalUsers"))
    return has_brief_volume and has_health_shape and len(trending) > 0


def _should_bypass_cached_snapshot(snapshot: Optional[dict], meta: Optional[DashboardCacheMeta]) -> bool:
    if not isinstance(meta, dict):
        return False
    degraded = _critical_fallback_tiers(list(meta.get("degradedTiers") or []))
    if not degraded:
        return False
    return True


def _should_preserve_existing_cache(
    *,
    existing_cache: Optional[dict],
    new_snapshot: dict,
    tier_times: Dict[str, Optional[float]],
) -> tuple[bool, list[str]]:
    fallback = _fallback_tiers(tier_times)
    if not fallback:
        return False, fallback

    critical_fallback = any(name in CRITICAL_TIERS for name in fallback)
    if not critical_fallback:
        return False, fallback

    if not _snapshot_has_core_pulse_data(existing_cache):
        return False, fallback

    if _snapshot_has_core_pulse_data(new_snapshot):
        return False, fallback

    return True, fallback


def _is_cache_valid(cache_key: str, now: Optional[float] = None) -> bool:
    t = now if now is not None else time.time()
    entry = _cache_entries.get(cache_key)
    if entry is None:
        return False
    ts, payload, _meta = entry
    return bool(payload) and (t - ts) < CACHE_TTL_SECONDS


def invalidate_cache() -> None:
    with _cache_lock:
        _cache_entries.clear()
    with _detail_cache_lock:
        _detail_cache.clear()


def _fallback_for_tier(name: str) -> dict:
    if name == "pulse":
        return {
            "communityHealth": {"score": 0, "trend": "neutral"},
            "trendingTopics": [],
            "trendingNewTopics": [],
            "communityBrief": {},
        }
    if name == "strategic":
        return {
            "topicBubbles": [],
            "trendLines": [],
            "trendData": [],
            "heatmap": [],
            "questionCategories": [],
            "questionBriefs": [],
            "lifecycleStages": [],
        }
    if name == "behavioral":
        return {
            "problemBriefs": [],
            "serviceGapBriefs": [],
            "problems": [],
            "serviceGaps": [],
            "satisfactionAreas": [],
            "moodData": [],
            "moodConfig": {},
            "urgencySignals": [],
        }
    if name == "network":
        return {"communityChannels": [], "keyVoices": [], "hourlyActivity": [], "weeklyActivity": [], "recommendations": [], "viralTopics": []}
    if name == "psychographic":
        return {
            "personas": [],
            "interests": [],
            "origins": [],
            "integrationData": [],
            "integrationLevels": [],
            "integrationConfig": {},
            "integrationSeriesConfig": [],
            "newcomerJourney": [],
        }
    if name == "predictive":
        return {"emergingInterests": [], "retentionFactors": [], "churnSignals": [], "growthFunnel": [], "decisionStages": []}
    if name == "actionable":
        return {"businessOpportunities": [], "businessOpportunityBriefs": [], "jobSeeking": [], "jobTrends": [], "housingData": [], "housingHotTopics": []}
    if name == "comparative":
        return {"weeklyShifts": [], "sentimentByTopic": [], "topPosts": [], "contentTypePerformance": [], "vitalityIndicators": {}}
    return {}


# ── Tier builders ────────────────────────────────────────────────────────────

def _tier_pulse(ctx: DashboardDateContext) -> dict:
    def _trending_new_topics() -> list[dict]:
        try:
            rows = _supabase_writer.list_emerging_topic_candidates(status="pending", limit=12)
            result: list[dict] = []
            for row in rows:
                mentions = int(row.get("distinct_content_count") or row.get("proposed_count") or 0)
                trend = min(300, max(0, (int(row.get("proposed_count") or 0) - 1) * 25))
                result.append(
                    {
                        "name": row.get("topic_name"),
                        "category": row.get("closest_category") or "General",
                        "mentions": mentions,
                        "trendPct": trend,
                        "sampleQuote": row.get("latest_evidence"),
                    }
                )
            return result
        except Exception as e:
            logger.warning(f"Tier pulse trending-new failed: {e}")
            return []

    fallback = _fallback_for_tier("pulse")

    def _safe(name: str, fn: Callable[[], object], default):
        try:
            return fn()
        except Exception as e:
            logger.error(f"Pulse widget {name} failed: {e}")
            return default

    return {
        "communityHealth": _safe("communityHealth", lambda: pulse.get_community_health(ctx), fallback["communityHealth"]),
        "trendingTopics": _safe("trendingTopics", lambda: pulse.get_trending_topics(ctx), fallback["trendingTopics"]),
        "trendingNewTopics": _safe("trendingNewTopics", _trending_new_topics, fallback["trendingNewTopics"]),
        "communityBrief": _safe("communityBrief", lambda: pulse.get_community_brief(ctx), fallback["communityBrief"]),
    }


def _tier_strategic(_ctx: DashboardDateContext) -> dict:
    fallback = _fallback_for_tier("strategic")

    def _safe(name: str, fn, default):
        try:
            return fn()
        except Exception as e:
            logger.error(f"Strategic widget {name} failed: {e}")
            return default

    trend_lines = _safe("trendLines", lambda: strategic.get_trend_lines(_ctx), [])
    return {
        "topicBubbles": _safe("topicBubbles", lambda: strategic.get_topic_bubbles(_ctx), fallback["topicBubbles"]),
        "trendLines": trend_lines,
        "trendData": trend_lines,
        "heatmap": _safe("heatmap", strategic.get_heatmap, fallback["heatmap"]),
        "questionCategories": _safe("questionCategories", lambda: strategic.get_question_categories(_ctx), fallback["questionCategories"]),
        "questionBriefs": _safe("questionBriefs", question_briefs.get_question_briefs, fallback["questionBriefs"]),
        "lifecycleStages": _safe("lifecycleStages", lambda: strategic.get_lifecycle_stages(_ctx), fallback["lifecycleStages"]),
    }


def _tier_behavioral(_ctx: DashboardDateContext) -> dict:
    try:
        briefs = behavioral_briefs.get_behavioral_briefs()
        return {
            "problemBriefs": briefs.get("problemBriefs", []),
            "serviceGapBriefs": briefs.get("serviceGapBriefs", []),
            "problems": behavioral.get_problems(_ctx),
            "serviceGaps": behavioral.get_service_gaps(_ctx),
            "satisfactionAreas": behavioral.get_satisfaction_areas(_ctx),
            "moodData": behavioral.get_mood_data(_ctx),
            "moodConfig": {"sentiments": ["Positive", "Negative", "Neutral", "Mixed", "Urgent", "Sarcastic"]},
            "urgencySignals": briefs.get("urgencyBriefs", []),
        }
    except Exception as e:
        logger.error(f"Tier behavioral failed: {e}")
        return _fallback_for_tier("behavioral")


def _tier_network(ctx: DashboardDateContext) -> dict:
    try:
        return {
            "communityChannels": network.get_community_channels(ctx),
            "keyVoices": network.get_key_voices(ctx),
            "hourlyActivity": network.get_hourly_activity(),
            "weeklyActivity": network.get_weekly_activity(),
            "recommendations": network.get_recommendations(),
            "viralTopics": network.get_information_velocity(ctx),  # Use new temporal tracking
        }
    except Exception as e:
        logger.error(f"Tier network failed: {e}")
        return _fallback_for_tier("network")


def _tier_psychographic(_ctx: DashboardDateContext) -> dict:
    try:
        integration_data = psychographic.get_integration_data()
        return {
            "personas": psychographic.get_personas(_ctx),
            "interests": psychographic.get_interests(_ctx),
            "origins": psychographic.get_origins(),
            "integrationData": integration_data,
            "integrationLevels": integration_data,
            "integrationConfig": {"languages": ["ru", "hy", "en", "mixed"]},
            "integrationSeriesConfig": [
                {"key": "learning", "color": "#3b82f6", "label": "Learning & Mixing", "labelRu": "Учится и смешивается", "polarity": "positive"},
                {"key": "bilingual", "color": "#8b5cf6", "label": "Bilingual Bubble", "labelRu": "Двуязычный пузырь", "polarity": "neutral"},
                {"key": "russianOnly", "color": "#f59e0b", "label": "Russian Only", "labelRu": "Только по-русски", "polarity": "negative"},
                {"key": "integrated", "color": "#10b981", "label": "Fully Integrated", "labelRu": "Полностью интегрирован", "polarity": "positive"},
            ],
            "newcomerJourney": psychographic.get_newcomer_journey(),
        }
    except Exception as e:
        logger.error(f"Tier psychographic failed: {e}")
        return _fallback_for_tier("psychographic")


def _tier_predictive(_ctx: DashboardDateContext) -> dict:
    try:
        return {
            "emergingInterests": predictive.get_emerging_interests(_ctx),
            "retentionFactors": predictive.get_retention_factors(_ctx),
            "churnSignals": predictive.get_churn_signals(_ctx),
            "growthFunnel": predictive.get_growth_funnel(_ctx),
            "decisionStages": predictive.get_decision_stages(_ctx),
            "newVsReturningVoiceWidget": predictive.get_new_vs_returning_voice_widget(_ctx),
        }
    except Exception as e:
        logger.error(f"Tier predictive failed: {e}")
        return _fallback_for_tier("predictive")


def _tier_actionable(_ctx: DashboardDateContext) -> dict:
    try:
        housing_data = actionable.get_housing_data()
        return {
            "businessOpportunities": actionable.get_business_opportunities(_ctx),
            "businessOpportunityBriefs": opportunity_briefs.get_business_opportunity_briefs(),
            "jobSeeking": actionable.get_job_seeking(_ctx),
            "jobTrends": actionable.get_job_trends(_ctx),
            "housingData": housing_data,
            "housingHotTopics": housing_data,
        }
    except Exception as e:
        logger.error(f"Tier actionable failed: {e}")
        return _fallback_for_tier("actionable")


def _tier_comparative(ctx: DashboardDateContext) -> dict:
    try:
        return {
            "weeklyShifts": comparative.get_weekly_shifts(ctx),
            "sentimentByTopic": comparative.get_sentiment_by_topic(ctx),
            "topPosts": comparative.get_top_posts(ctx),
            "contentTypePerformance": comparative.get_content_type_performance(ctx),
            "vitalityIndicators": comparative.get_vitality_indicators(),
        }
    except Exception as e:
        logger.error(f"Tier comparative failed: {e}")
        return _fallback_for_tier("comparative")


def _tier_derived(data: dict) -> dict:
    try:
        return {
            "voiceData": data.get("keyVoices", []),
            "topNewTopics": data.get("emergingInterests", []),
            "qaGap": {
                "totalQuestions": len(data.get("questionCategories", [])),
                "answered": 0,
            },
        }
    except Exception:
        return {}


def _ordered_tiers(ctx: DashboardDateContext) -> List[Tuple[str, Callable[[], dict]]]:
    return [
        ("pulse", lambda: _tier_pulse(ctx)),
        ("strategic", lambda: _tier_strategic(ctx)),
        ("behavioral", lambda: _tier_behavioral(ctx)),
        ("network", lambda: _tier_network(ctx)),
        ("psychographic", lambda: _tier_psychographic(ctx)),
        ("predictive", lambda: _tier_predictive(ctx)),
        ("actionable", lambda: _tier_actionable(ctx)),
        ("comparative", lambda: _tier_comparative(ctx)),
    ]


def _run_tier_builder(fn: Callable[[], dict]) -> Tuple[dict, float]:
    t0 = time.time()
    payload = fn()
    return payload, round(time.time() - t0, 3)


def _build_snapshot_sequential(ctx: DashboardDateContext) -> Tuple[dict, Dict[str, Optional[float]]]:
    data: dict = {}
    tier_times: Dict[str, Optional[float]] = {}

    for name, builder in _ordered_tiers(ctx):
        t0 = time.time()
        payload = builder()
        tier_times[name] = round(time.time() - t0, 3)
        data.update(payload)

    t0 = time.time()
    data.update(_tier_derived(data))
    tier_times["derived"] = round(time.time() - t0, 3)
    return data, tier_times


def _build_snapshot_parallel(ctx: DashboardDateContext, use_timeouts: bool = True) -> Tuple[dict, Dict[str, Optional[float]]]:
    ordered = _ordered_tiers(ctx)
    tier_payloads: Dict[str, dict] = {}
    tier_times: Dict[str, Optional[float]] = {}
    executor, futures = _submit_tier_futures(ordered)
    for name, future in futures.items():
        try:
            if use_timeouts:
                payload, duration = future.result(timeout=TIER_TIMEOUT_SECONDS)
            else:
                payload, duration = future.result()
            tier_payloads[name] = payload
            tier_times[name] = duration
        except FuturesTimeout:
            logger.warning(f"Tier {name} timed out after {TIER_TIMEOUT_SECONDS}s — using fallback")
            tier_payloads[name] = _fallback_for_tier(name)
            tier_times[name] = None
            if not future.cancel():
                _replace_tier_executor(executor)
        except Exception as e:
            logger.error(f"Tier {name} crashed — using fallback: {e}")
            tier_payloads[name] = _fallback_for_tier(name)
            tier_times[name] = None

    data: dict = {}
    for name, _builder in ordered:
        data.update(tier_payloads.get(name, _fallback_for_tier(name)))

    t0 = time.time()
    data.update(_tier_derived(data))
    tier_times["derived"] = round(time.time() - t0, 3)
    return data, tier_times


def _build_snapshot(ctx: DashboardDateContext, use_timeouts: bool = True) -> Tuple[dict, Dict[str, Optional[float]], float, str]:
    mode = "parallel" if PARALLEL_ENABLED else "sequential"
    t0 = time.time()

    if PARALLEL_ENABLED:
        data, tier_times = _build_snapshot_parallel(ctx, use_timeouts=use_timeouts)
    else:
        data, tier_times = _build_snapshot_sequential(ctx)

    elapsed = round(time.time() - t0, 3)
    return data, tier_times, elapsed, mode


def _snapshot_meta(
    *,
    tier_times: Dict[str, Optional[float]],
    elapsed: float,
    mode: str,
    cache_status: str,
    is_stale: bool = False,
) -> DashboardCacheMeta:
    return {
        "tierTimes": dict(tier_times),
        "degradedTiers": _fallback_tiers(tier_times),
        "snapshotBuiltAt": datetime.now(timezone.utc).isoformat(),
        "buildElapsedSeconds": elapsed,
        "buildMode": mode,
        "cacheStatus": cache_status,
        "isStale": is_stale,
    }


# ── Main aggregation API ─────────────────────────────────────────────────────

def _default_dashboard_context() -> DashboardDateContext:
    today = time.strftime("%Y-%m-%d", time.gmtime())
    end_date = build_dashboard_date_context(today, today).to_date
    start_date = end_date.fromordinal(end_date.toordinal() - 14)
    return build_dashboard_date_context(start_date.isoformat(), end_date.isoformat())


def get_dashboard_snapshot(
    ctx: Optional[DashboardDateContext] = None,
    force_refresh: bool = False,
) -> tuple[dict, DashboardCacheMeta]:
    """Assemble full AppData snapshot with simple per-range caching."""
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = resolved_ctx.cache_key
    now = time.time()

    with _cache_lock:
        entry = _cache_entries.get(cache_key)
        if entry is not None and not force_refresh and _is_cache_valid(cache_key, now):
            logger.debug(f"Serving dashboard data from range cache | key={cache_key}")
            _ts, snapshot, meta = entry
            if not _should_bypass_cached_snapshot(snapshot, meta):
                return snapshot, dict(meta)
            logger.warning(
                "Bypassing cached dashboard snapshot because a critical tier was degraded "
                f"| key={cache_key} degraded={meta.get('degradedTiers', [])}"
            )
        stale_snapshot = entry[1] if entry is not None else None
        stale_meta = dict(entry[2]) if entry is not None else None

    try:
        data, tier_times, elapsed, mode = _build_snapshot(resolved_ctx, use_timeouts=True)
        build_meta = _snapshot_meta(
            tier_times=tier_times,
            elapsed=elapsed,
            mode=mode,
            cache_status="refresh_success",
            is_stale=False,
        )
        critical_degraded = _critical_fallback_tiers(build_meta.get("degradedTiers", []))

        with _cache_lock:
            preserve_existing = False
            fallback_tiers: list[str] = []
            if stale_snapshot is not None:
                preserve_existing, fallback_tiers = _should_preserve_existing_cache(
                    existing_cache=stale_snapshot,
                    new_snapshot=data,
                    tier_times=tier_times,
                )
            if preserve_existing and stale_snapshot is not None:
                logger.warning(
                    "Dashboard rebuild preserved previous range cache because critical tiers fell back "
                    f"({fallback_tiers}); key={cache_key} elapsed={elapsed}s mode={mode}"
                )
                preserved_meta = dict(stale_meta or {})
                preserved_meta["cacheStatus"] = "preserved_previous_on_fallback"
                preserved_meta["isStale"] = True
                preserved_meta["suppressedDegradedTiers"] = fallback_tiers
                return stale_snapshot, preserved_meta

            if critical_degraded:
                logger.warning(
                    "Dashboard rebuild completed with critical degraded tiers; returning uncached snapshot "
                    f"| key={cache_key} degraded={critical_degraded} elapsed={elapsed}s mode={mode}"
                )
                build_meta["cacheStatus"] = "refresh_success_uncached_degraded"
            else:
                _cache_entries[cache_key] = (time.time(), data, build_meta)

        logger.success(f"Dashboard data assembled in {elapsed}s ({mode}) | key={cache_key} tiers={tier_times}")
        return data, build_meta
    except Exception as e:
        if stale_snapshot is not None:
            logger.warning(f"Dashboard rebuild failed for range {cache_key}; serving stale cache instead: {e}")
            failure_meta = dict(stale_meta or {})
            failure_meta["cacheStatus"] = "stale_on_error"
            failure_meta["isStale"] = True
            failure_meta["refreshError"] = str(e)
            return stale_snapshot, failure_meta
        logger.error(f"Dashboard rebuild failed with no fallback cache for range {cache_key}: {e}")
        raise


def get_dashboard_data(
    ctx: Optional[DashboardDateContext] = None,
    force_refresh: bool = False,
) -> dict:
    snapshot, _meta = get_dashboard_snapshot(ctx, force_refresh=force_refresh)
    return snapshot


# ── Detail page queries (independent cache) ──────────────────────────────────

def _get_cached_detail_value(
    cache_key: str,
    builder: Callable[[], Any],
    ttl_seconds: int = DETAIL_CACHE_TTL_SECONDS,
) -> Any:
    now = time.time()
    stale: Any = None

    with _detail_cache_lock:
        entry = _detail_cache.get(cache_key)
        if entry is not None:
            ts, data = entry
            stale = data
            if (now - ts) < ttl_seconds:
                return data

    try:
        fresh = builder()
        if isinstance(stale, list) and len(stale) > 0 and isinstance(fresh, list) and len(fresh) == 0:
            logger.warning(f"Detail query {cache_key} returned empty payload; serving stale cache")
            return stale
        if stale is not None and fresh is None:
            logger.warning(f"Detail query {cache_key} returned empty detail payload; serving stale cache")
            return stale
        with _detail_cache_lock:
            _detail_cache[cache_key] = (time.time(), fresh)
        return fresh
    except Exception as e:
        if stale is not None:
            logger.warning(f"Detail query {cache_key} failed; serving stale cache instead: {e}")
            return stale
        raise


def get_topics_page(
    page: int = 0,
    size: int = 50,
    ctx: Optional[DashboardDateContext] = None,
) -> list[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"topics:{resolved_ctx.cache_key}:{page}:{size}"
    return _get_cached_detail_value(cache_key, lambda: comparative.get_all_topics(page, size, resolved_ctx))


def get_channels_page(ctx: Optional[DashboardDateContext] = None) -> list[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"channels:{resolved_ctx.cache_key}:all"
    return _get_cached_detail_value(cache_key, lambda: comparative.get_all_channels(resolved_ctx))


def get_audience_page(
    page: int = 0,
    size: int = 50,
    ctx: Optional[DashboardDateContext] = None,
) -> list[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"audience:{resolved_ctx.cache_key}:{page}:{size}"
    return _get_cached_detail_value(cache_key, lambda: comparative.get_all_audience(page, size, resolved_ctx))


def get_topic_detail(
    topic_name: str,
    category: Optional[str] = None,
    ctx: Optional[DashboardDateContext] = None,
) -> Optional[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"topic-detail:{resolved_ctx.cache_key}:{topic_name}:{category or ''}"
    return _get_cached_detail_value(cache_key, lambda: comparative.get_topic_detail(topic_name, category, resolved_ctx))


def get_channel_detail(
    channel_key: str,
    ctx: Optional[DashboardDateContext] = None,
) -> Optional[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"channel-detail:{resolved_ctx.cache_key}:{channel_key}"
    return _get_cached_detail_value(cache_key, lambda: comparative.get_channel_detail(channel_key, resolved_ctx))


def get_audience_detail(
    user_id: str,
    ctx: Optional[DashboardDateContext] = None,
) -> Optional[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"audience-detail:{resolved_ctx.cache_key}:{user_id}"
    return _get_cached_detail_value(cache_key, lambda: comparative.get_audience_detail(user_id, resolved_ctx))
