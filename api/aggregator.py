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
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from loguru import logger

from api import behavioral_briefs
from api.dashboard_dates import DashboardDateContext, build_dashboard_date_context
from api import opportunity_briefs
from api import question_briefs
from api.queries import actionable, behavioral, comparative, network, predictive, psychographic, pulse, strategic
from api.runtime_executors import submit_background


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


# ── Cache + reliability config ───────────────────────────────────────────────
CACHE_TTL_SECONDS = int(os.getenv("DASH_CACHE_TTL_SECONDS", "900"))
STALE_WHILE_REVALIDATE = _env_flag("DASH_STALE_WHILE_REVALIDATE_ENABLED", True)
PARALLEL_ENABLED = _env_flag("DASH_PARALLEL_ENABLED", True)
PARALLEL_MAX_WORKERS = max(1, min(int(os.getenv("DASH_PARALLEL_MAX_WORKERS", "4")), 8))
TIER_TIMEOUT_SECONDS = max(0.5, float(os.getenv("DASH_TIER_TIMEOUT_SECONDS", "10.0")))
REFRESH_TIMEOUT_SECONDS = max(5.0, float(os.getenv("DASH_REFRESH_TIMEOUT_SECONDS", "30.0")))
WAIT_FOR_REFRESH_SECONDS = max(1.0, float(os.getenv("DASH_WAIT_FOR_REFRESH_SECONDS", "8.0")))
WAIT_FOR_EMPTY_REFRESH_SECONDS = max(
    WAIT_FOR_REFRESH_SECONDS,
    float(os.getenv("DASH_WAIT_FOR_EMPTY_REFRESH_SECONDS", str(REFRESH_TIMEOUT_SECONDS + 2.0))),
)
MAX_STALE_SECONDS = max(CACHE_TTL_SECONDS, int(os.getenv("DASH_MAX_STALE_SECONDS", "1800")))
CRITICAL_DEGRADED_STALE_SECONDS = max(
    WAIT_FOR_REFRESH_SECONDS,
    int(os.getenv("DASH_CRITICAL_DEGRADED_STALE_SECONDS", "300")),
)
REFRESH_FAILURE_ALERT_THRESHOLD = max(1, int(os.getenv("DASH_REFRESH_FAILURE_ALERT_THRESHOLD", "3")))
REFRESH_BACKOFF_FAILURE_THRESHOLD = 2
REFRESH_BACKOFF_COOLDOWN_SECONDS = 300.0
DETAIL_CACHE_TTL_SECONDS = max(30, int(os.getenv("DETAIL_CACHE_TTL_SECONDS", "180")))
TOPICS_PAGE_CACHE_TTL_SECONDS = max(
    DETAIL_CACHE_TTL_SECONDS,
    int(os.getenv("TOPICS_PAGE_CACHE_TTL_SECONDS", "300")),
)
DETAIL_MAX_STALE_SECONDS = max(
    DETAIL_CACHE_TTL_SECONDS,
    int(os.getenv("DETAIL_MAX_STALE_SECONDS", "1800")),
)
DETAIL_REFRESH_TIMEOUT_SECONDS = max(
    1.0,
    float(os.getenv("DETAIL_REFRESH_TIMEOUT_SECONDS", "8.0")),
)

# If one of these tiers falls back during refresh, prefer preserving an
# existing healthy cache instead of replacing it with an empty/degraded view.
CRITICAL_TIERS = {"pulse", "strategic"}
ALL_TIER_NAMES = (
    "pulse",
    "strategic",
    "behavioral",
    "network",
    "psychographic",
    "predictive",
    "actionable",
    "comparative",
)


DashboardCacheMeta = Dict[str, object]
DashboardCacheEntry = Tuple[float, dict, DashboardCacheMeta]


_cache_entries: Dict[str, DashboardCacheEntry] = {}

_cache_lock = threading.Lock()

_detail_cache_lock = threading.Lock()
_detail_cache: Dict[str, Tuple[float, Any]] = {}
_detail_refresh_state_lock = threading.Lock()
_tier_executor_lock = threading.Lock()
_refresh_state_lock = threading.Lock()


@dataclass
class DashboardRefreshState:
    inflight: bool = False
    event: threading.Event = field(default_factory=threading.Event)
    failure_count: int = 0
    last_error: str | None = None
    suppressed_until: float = 0.0


_refresh_states: Dict[str, DashboardRefreshState] = {}
_refresh_complete_callback: Optional[Callable[[str, DashboardDateContext, dict, DashboardCacheMeta], None]] = None


def set_dashboard_refresh_complete_callback(
    callback: Optional[Callable[[str, DashboardDateContext, dict, DashboardCacheMeta], None]],
) -> None:
    global _refresh_complete_callback
    _refresh_complete_callback = callback


def _emit_refresh_complete(
    cache_key: str,
    ctx: DashboardDateContext,
    snapshot: dict,
    meta: DashboardCacheMeta,
) -> None:
    callback = _refresh_complete_callback
    if callback is None:
        return
    try:
        callback(cache_key, ctx, snapshot, dict(meta))
    except Exception as exc:
        logger.warning(f"Dashboard refresh completion callback failed | key={cache_key} error={exc}")
_detail_refresh_states: Dict[str, DashboardRefreshState] = {}


class DetailRefreshUnavailableError(RuntimeError):
    """Raised when a detail cache cannot be refreshed and has no serveable stale value."""


def _new_tier_executor() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(
        max_workers=PARALLEL_MAX_WORKERS,
        thread_name_prefix="dash-tier",
    )


def _shutdown_executor(executor: ThreadPoolExecutor, *, wait: bool) -> None:
    try:
        executor.shutdown(wait=wait, cancel_futures=True)
    except TypeError:  # Python 3.8 compatibility
        executor.shutdown(wait=wait)

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
    _shutdown_executor(stale_executor, wait=False)


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


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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
    with _detail_refresh_state_lock:
        _detail_refresh_states.clear()


def _get_refresh_state(cache_key: str) -> DashboardRefreshState:
    with _refresh_state_lock:
        state = _refresh_states.get(cache_key)
        if state is None:
            state = DashboardRefreshState()
            state.event.set()
            _refresh_states[cache_key] = state
        return state


def _acquire_refresh_slot(cache_key: str) -> tuple[DashboardRefreshState, bool]:
    with _refresh_state_lock:
        state = _refresh_states.get(cache_key)
        if state is None:
            state = DashboardRefreshState()
            state.event.set()
            _refresh_states[cache_key] = state
        if state.inflight:
            return state, False
        state.inflight = True
        state.event.clear()
        return state, True


def _release_refresh_slot(cache_key: str) -> None:
    with _refresh_state_lock:
        state = _refresh_states.get(cache_key)
        if state is None:
            return
        state.inflight = False
        state.event.set()


def _refresh_state_snapshot(cache_key: str) -> dict[str, Any]:
    with _refresh_state_lock:
        state = _refresh_states.get(cache_key)
        if state is None:
            return {"refreshFailureCount": 0, "refreshInFlight": False, "refreshSuppressed": False}
        suppressed = state.suppressed_until > time.time()
        return {
            "refreshFailureCount": int(state.failure_count),
            "refreshInFlight": bool(state.inflight),
            "refreshLastError": state.last_error,
            "refreshSuppressed": suppressed,
        }


def _record_refresh_success(cache_key: str) -> None:
    with _refresh_state_lock:
        state = _refresh_states.get(cache_key)
        if state is None:
            return
        state.failure_count = 0
        state.last_error = None
        state.suppressed_until = 0.0


def _error_triggers_refresh_backoff(error: Exception | str) -> bool:
    if isinstance(error, TimeoutError):
        return True
    text = str(error or "").strip().lower()
    if not text:
        return False
    return any(
        token in text
        for token in (
            "memorypooloutofmemoryerror",
            "dbms.memory.transaction.total.max",
            "neo.transienterror.general.memorypooloutofmemoryerror",
            "out of memory",
        )
    )


def _record_refresh_failure(cache_key: str, error: Exception | str) -> int:
    message = str(error)
    relevant_failure = _error_triggers_refresh_backoff(error)
    now = time.time()
    with _refresh_state_lock:
        state = _refresh_states.get(cache_key)
        if state is None:
            state = DashboardRefreshState()
            state.event.set()
            _refresh_states[cache_key] = state
        if relevant_failure:
            state.failure_count += 1
            if state.failure_count >= REFRESH_BACKOFF_FAILURE_THRESHOLD:
                state.suppressed_until = max(
                    state.suppressed_until,
                    now + REFRESH_BACKOFF_COOLDOWN_SECONDS,
                )
        else:
            state.failure_count = 0
        state.last_error = message
        failure_count = state.failure_count
        refresh_suppressed = state.suppressed_until > now
    if failure_count >= REFRESH_FAILURE_ALERT_THRESHOLD:
        logger.error(
            f"Dashboard refresh threshold exceeded | key={cache_key} failures={failure_count} error={message}"
        )
    else:
        logger.warning(f"Dashboard refresh failed | key={cache_key} failures={failure_count} error={message}")
    if refresh_suppressed:
        logger.warning(
            "Dashboard refresh suppressed after repeated failures "
            f"| key={cache_key} cooldown_s={int(REFRESH_BACKOFF_COOLDOWN_SECONDS)}"
        )
    return failure_count


def _cache_entry_age_seconds(entry: DashboardCacheEntry | None, now: float) -> float | None:
    if entry is None:
        return None
    return max(0.0, now - entry[0])


def _entry_max_stale_seconds(entry: DashboardCacheEntry | None) -> float:
    if entry is None:
        return float(MAX_STALE_SECONDS)
    try:
        raw_value = entry[2].get("maxServeAgeSeconds")
        if raw_value is None:
            return float(MAX_STALE_SECONDS)
        value = float(raw_value)
    except Exception:
        return float(MAX_STALE_SECONDS)
    return max(1.0, value)


def _can_serve_stale(entry: DashboardCacheEntry | None, now: float) -> bool:
    if entry is None:
        return False
    ts, snapshot, _meta = entry
    return bool(snapshot) and (now - ts) < _entry_max_stale_seconds(entry)


def _with_refresh_state(cache_key: str, meta: DashboardCacheMeta, *, stale_age_seconds: float | None = None) -> DashboardCacheMeta:
    enriched = dict(meta)
    enriched.update(_refresh_state_snapshot(cache_key))
    if stale_age_seconds is not None:
        enriched["staleAgeSeconds"] = round(stale_age_seconds, 3)
    return enriched


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
        "trendingNewTopics": _safe("trendingNewTopics", lambda: pulse.get_trending_new_topics(ctx), fallback["trendingNewTopics"]),
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
    return _build_snapshot_sequential_with_skips(ctx, skipped_tiers=None)


def _build_snapshot_sequential_with_skips(
    ctx: DashboardDateContext,
    *,
    skipped_tiers: set[str] | None,
) -> Tuple[dict, Dict[str, Optional[float]]]:
    data: dict = {}
    tier_times: Dict[str, Optional[float]] = {}
    skip_set = skipped_tiers or set()

    for name, builder in _ordered_tiers(ctx):
        if name in skip_set:
            tier_times[name] = None
            data.update(_fallback_for_tier(name))
            continue
        t0 = time.time()
        payload = builder()
        tier_times[name] = round(time.time() - t0, 3)
        data.update(payload)

    t0 = time.time()
    data.update(_tier_derived(data))
    tier_times["derived"] = round(time.time() - t0, 3)
    return data, tier_times


def _build_snapshot_parallel(
    ctx: DashboardDateContext,
    use_timeouts: bool = True,
    *,
    skipped_tiers: set[str] | None = None,
) -> Tuple[dict, Dict[str, Optional[float]]]:
    ordered = _ordered_tiers(ctx)
    tier_payloads: Dict[str, dict] = {}
    tier_times: Dict[str, Optional[float]] = {}
    skip_set = skipped_tiers or set()
    runnable = [(name, builder) for name, builder in ordered if name not in skip_set]
    executor, futures = _submit_tier_futures(runnable)

    for name in skip_set:
        tier_payloads[name] = _fallback_for_tier(name)
        tier_times[name] = None

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


def _build_snapshot(
    ctx: DashboardDateContext,
    use_timeouts: bool = True,
    *,
    skipped_tiers: set[str] | None = None,
) -> Tuple[dict, Dict[str, Optional[float]], float, str]:
    mode = "parallel" if PARALLEL_ENABLED else "sequential"
    t0 = time.time()

    if PARALLEL_ENABLED:
        data, tier_times = _build_snapshot_parallel(
            ctx,
            use_timeouts=use_timeouts,
            skipped_tiers=skipped_tiers,
        )
    else:
        data, tier_times = _build_snapshot_sequential_with_skips(
            ctx,
            skipped_tiers=skipped_tiers,
        )

    elapsed = round(time.time() - t0, 3)
    return data, tier_times, elapsed, mode


def _snapshot_meta(
    *,
    tier_times: Dict[str, Optional[float]],
    elapsed: float,
    mode: str,
    cache_status: str,
    is_stale: bool = False,
    refresh_failure_count: int = 0,
) -> DashboardCacheMeta:
    return {
        "tierTimes": dict(tier_times),
        "degradedTiers": _fallback_tiers(tier_times),
        "snapshotBuiltAt": datetime.now(timezone.utc).isoformat(),
        "buildElapsedSeconds": elapsed,
        "buildMode": mode,
        "cacheStatus": cache_status,
        "isStale": is_stale,
        "refreshFailureCount": refresh_failure_count,
    }


def _emergency_degraded_snapshot(
    cache_key: str,
    reason: str,
) -> tuple[dict, DashboardCacheMeta]:
    """Return an all-fallback snapshot when no fresh or stale cache is available."""
    data: dict = {}
    tier_times: Dict[str, Optional[float]] = {}
    for name in ALL_TIER_NAMES:
        data.update(_fallback_for_tier(name))
        tier_times[name] = None
    data.update(_tier_derived(data))
    tier_times["derived"] = 0.0
    failure_count = _record_refresh_failure(cache_key, reason)
    meta = _snapshot_meta(
        tier_times=tier_times,
        elapsed=0.0,
        mode="emergency_fallback",
        cache_status="emergency_degraded",
        is_stale=True,
        refresh_failure_count=failure_count,
    )
    return data, _with_refresh_state(cache_key, meta)


# ── Main aggregation API ─────────────────────────────────────────────────────

def _default_dashboard_context() -> DashboardDateContext:
    today = time.strftime("%Y-%m-%d", time.gmtime())
    end_date = build_dashboard_date_context(today, today).to_date
    start_date = end_date.fromordinal(end_date.toordinal() - 14)
    return build_dashboard_date_context(start_date.isoformat(), end_date.isoformat())


def _build_snapshot_with_timeout(
    ctx: DashboardDateContext,
    *,
    skipped_tiers: set[str] | None = None,
) -> tuple[dict, Dict[str, Optional[float]], float, str]:
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dash-refresh")
    future = executor.submit(_build_snapshot, ctx, True, skipped_tiers=skipped_tiers)
    try:
        return future.result(timeout=REFRESH_TIMEOUT_SECONDS)
    except FuturesTimeout as exc:
        future.cancel()
        raise TimeoutError(
            f"Dashboard rebuild exceeded {REFRESH_TIMEOUT_SECONDS:.1f}s timeout"
        ) from exc
    finally:
        _shutdown_executor(executor, wait=False)


def _refresh_dashboard_snapshot(
    cache_key: str,
    ctx: DashboardDateContext,
    *,
    stale_entry: DashboardCacheEntry | None = None,
) -> tuple[dict, DashboardCacheMeta]:
    stale_snapshot = stale_entry[1] if stale_entry is not None else None
    stale_meta = dict(stale_entry[2]) if stale_entry is not None else None
    stale_ts = stale_entry[0] if stale_entry is not None else None

    try:
        data, tier_times, elapsed, mode = _build_snapshot_with_timeout(ctx)
        build_meta = _snapshot_meta(
            tier_times=tier_times,
            elapsed=elapsed,
            mode=mode,
            cache_status="refresh_success",
            is_stale=False,
            refresh_failure_count=0,
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
            if preserve_existing and stale_snapshot is not None and stale_ts is not None:
                failure_count = _record_refresh_failure(
                    cache_key,
                    f"critical tier fallback preserved previous cache: {fallback_tiers}",
                )
                logger.warning(
                    "Dashboard rebuild preserved previous range cache because critical tiers fell back "
                    f"({fallback_tiers}); key={cache_key} elapsed={elapsed}s mode={mode}"
                )
                preserved_meta = dict(stale_meta or {})
                preserved_meta["cacheStatus"] = "preserved_previous_on_fallback"
                preserved_meta["isStale"] = True
                preserved_meta["suppressedDegradedTiers"] = fallback_tiers
                preserved_meta["refreshFailureCount"] = failure_count
                _cache_entries[cache_key] = (stale_ts, stale_snapshot, preserved_meta)
                return stale_snapshot, _with_refresh_state(
                    cache_key,
                    preserved_meta,
                    stale_age_seconds=max(0.0, time.time() - stale_ts),
                )

            if critical_degraded:
                failure_count = _record_refresh_failure(
                    cache_key,
                    f"critical degraded tiers during refresh: {critical_degraded}",
                )
                logger.warning(
                    "Dashboard rebuild completed with critical degraded tiers; returning uncached snapshot "
                    f"| key={cache_key} degraded={critical_degraded} elapsed={elapsed}s mode={mode}"
                )
                build_meta["cacheStatus"] = "refresh_success_uncached_degraded"
                build_meta["isStale"] = True
                build_meta["maxServeAgeSeconds"] = CRITICAL_DEGRADED_STALE_SECONDS
                build_meta["refreshFailureCount"] = failure_count
                _cache_entries[cache_key] = (time.time(), data, dict(build_meta))
            else:
                _record_refresh_success(cache_key)
                build_meta["refreshFailureCount"] = 0
                _cache_entries[cache_key] = (time.time(), data, build_meta)

        _emit_refresh_complete(cache_key, ctx, data, build_meta)
        logger.success(f"Dashboard data assembled in {elapsed}s ({mode}) | key={cache_key} tiers={tier_times}")
        return data, _with_refresh_state(cache_key, build_meta)
    except Exception as exc:
        failure_count = _record_refresh_failure(cache_key, exc)
        if stale_snapshot is not None and stale_ts is not None:
            logger.warning(f"Dashboard rebuild failed for range {cache_key}; serving stale cache instead: {exc}")
            failure_meta = dict(stale_meta or {})
            failure_meta["cacheStatus"] = "stale_on_error"
            failure_meta["isStale"] = True
            failure_meta["refreshError"] = str(exc)
            failure_meta["refreshFailureCount"] = failure_count
            with _cache_lock:
                _cache_entries[cache_key] = (stale_ts, stale_snapshot, failure_meta)
            return stale_snapshot, _with_refresh_state(
                cache_key,
                failure_meta,
                stale_age_seconds=max(0.0, time.time() - stale_ts),
            )
        logger.error(f"Dashboard rebuild failed with no fallback cache for range {cache_key}: {exc}")
        return _emergency_degraded_snapshot(cache_key, str(exc))


def _background_refresh_dashboard_snapshot(cache_key: str, ctx: DashboardDateContext) -> None:
    with _cache_lock:
        stale_entry = _cache_entries.get(cache_key)
    try:
        _refresh_dashboard_snapshot(cache_key, ctx, stale_entry=stale_entry)
    except Exception as exc:
        logger.error(f"Background dashboard refresh failed | key={cache_key} error={exc}")
    finally:
        _release_refresh_slot(cache_key)


def _ensure_background_refresh(cache_key: str, ctx: DashboardDateContext) -> bool:
    now = time.time()
    with _refresh_state_lock:
        state = _refresh_states.get(cache_key)
        if state is not None and state.suppressed_until > now:
            return False
    _state, leader = _acquire_refresh_slot(cache_key)
    if not leader:
        return False
    thread = threading.Thread(
        target=_background_refresh_dashboard_snapshot,
        args=(cache_key, ctx),
        daemon=True,
        name=f"dash-refresh-{cache_key}",
    )
    thread.start()
    return True


def get_dashboard_snapshot(
    ctx: Optional[DashboardDateContext] = None,
    force_refresh: bool = False,
) -> tuple[dict, DashboardCacheMeta]:
    """Assemble full AppData snapshot with cache, stale fallback, and single-flight refresh."""
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = resolved_ctx.cache_key
    now = time.time()

    with _cache_lock:
        entry = _cache_entries.get(cache_key)
        if entry is not None and not force_refresh and _is_cache_valid(cache_key, now):
            logger.debug(f"Serving dashboard data from range cache | key={cache_key}")
            _ts, snapshot, meta = entry
            if not _should_bypass_cached_snapshot(snapshot, meta):
                return snapshot, _with_refresh_state(cache_key, meta)
            logger.warning(
                "Bypassing cached dashboard snapshot because a critical tier was degraded "
                f"| key={cache_key} degraded={meta.get('degradedTiers', [])}"
            )
        stale_entry = entry if _can_serve_stale(entry, now) else None

    stale_age_seconds = _cache_entry_age_seconds(stale_entry, now)
    if stale_entry is not None and STALE_WHILE_REVALIDATE and not force_refresh:
        refresh_started = _ensure_background_refresh(cache_key, resolved_ctx)
        stale_meta = dict(stale_entry[2])
        stale_meta["cacheStatus"] = (
            "stale_while_revalidate" if refresh_started else "stale_while_revalidate_inflight"
        )
        stale_meta["isStale"] = True
        return stale_entry[1], _with_refresh_state(
            cache_key,
            stale_meta,
            stale_age_seconds=stale_age_seconds,
        )

    state, leader = _acquire_refresh_slot(cache_key)
    if not leader:
        wait_timeout = WAIT_FOR_REFRESH_SECONDS if stale_entry is not None else WAIT_FOR_EMPTY_REFRESH_SECONDS
        if state.event.wait(timeout=wait_timeout):
            with _cache_lock:
                refreshed_entry = _cache_entries.get(cache_key)
            if refreshed_entry is not None:
                refreshed_now = time.time()
                if _is_cache_valid(cache_key, refreshed_now):
                    return refreshed_entry[1], _with_refresh_state(cache_key, refreshed_entry[2])
                if _can_serve_stale(refreshed_entry, refreshed_now):
                    refreshed_meta = dict(refreshed_entry[2])
                    refreshed_meta["cacheStatus"] = "waited_for_refresh_stale"
                    refreshed_meta["isStale"] = True
                    return refreshed_entry[1], _with_refresh_state(
                        cache_key,
                        refreshed_meta,
                        stale_age_seconds=_cache_entry_age_seconds(refreshed_entry, refreshed_now),
                    )
        if stale_entry is not None:
            stale_meta = dict(stale_entry[2])
            stale_meta["cacheStatus"] = "stale_refresh_wait_timeout"
            stale_meta["isStale"] = True
            return stale_entry[1], _with_refresh_state(
                cache_key,
                stale_meta,
                stale_age_seconds=stale_age_seconds,
            )
        reason = (
            f"Dashboard refresh did not complete within {wait_timeout:.1f}s and no stale snapshot is available"
        )
        logger.warning(reason)
        return _emergency_degraded_snapshot(cache_key, reason)

    try:
        return _refresh_dashboard_snapshot(cache_key, resolved_ctx, stale_entry=stale_entry)
    finally:
        _release_refresh_slot(cache_key)


def get_dashboard_data(
    ctx: Optional[DashboardDateContext] = None,
    force_refresh: bool = False,
) -> dict:
    snapshot, _meta = get_dashboard_snapshot(ctx, force_refresh=force_refresh)
    return snapshot


def build_dashboard_snapshot_once(
    ctx: DashboardDateContext,
    *,
    skipped_tiers: set[str] | None = None,
    cache_status: str = "refresh_success_uncached",
) -> tuple[dict, DashboardCacheMeta]:
    """Build a one-off dashboard snapshot without mutating the shared cache."""
    data, tier_times, elapsed, mode = _build_snapshot_with_timeout(
        ctx,
        skipped_tiers=skipped_tiers,
    )
    meta = _snapshot_meta(
        tier_times=tier_times,
        elapsed=elapsed,
        mode=mode,
        cache_status=cache_status,
        is_stale=False,
        refresh_failure_count=0,
    )
    if skipped_tiers:
        meta["skippedTiers"] = sorted(skipped_tiers)
    logger.info(
        "Dashboard one-off snapshot built | key={} elapsed={}s mode={} skipped_tiers={}",
        ctx.cache_key,
        elapsed,
        mode,
        sorted(skipped_tiers or []),
    )
    return data, meta


def peek_dashboard_snapshot(ctx: Optional[DashboardDateContext] = None) -> tuple[dict | None, DashboardCacheMeta | None, str]:
    """Inspect the in-memory dashboard cache without triggering a rebuild."""
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = resolved_ctx.cache_key
    now = time.time()

    with _cache_lock:
        entry = _cache_entries.get(cache_key)

    if entry is None:
        return None, None, "missing"

    snapshot = entry[1]
    meta = dict(entry[2])
    if _is_cache_valid(cache_key, now) and not _should_bypass_cached_snapshot(snapshot, meta):
        return snapshot, _with_refresh_state(cache_key, meta), "fresh"

    if _can_serve_stale(entry, now):
        stale_meta = dict(meta)
        stale_meta["isStale"] = True
        stale_meta.setdefault("cacheStatus", "memory_stale")
        return snapshot, _with_refresh_state(
            cache_key,
            stale_meta,
            stale_age_seconds=_cache_entry_age_seconds(entry, now),
        ), "stale"

    return None, _with_refresh_state(cache_key, meta), "expired"


def prime_dashboard_snapshot(
    ctx: DashboardDateContext,
    snapshot: dict,
    meta: DashboardCacheMeta,
    *,
    cached_at_ts: float | None = None,
) -> None:
    """Seed the in-memory dashboard cache from a trusted external snapshot."""
    timestamp = float(cached_at_ts) if cached_at_ts is not None else time.time()
    with _cache_lock:
        _cache_entries[ctx.cache_key] = (timestamp, dict(snapshot), dict(meta))


def refresh_dashboard_snapshot_async(ctx: DashboardDateContext) -> bool:
    """Trigger a single-flight background refresh for a dashboard cache key."""
    return _ensure_background_refresh(ctx.cache_key, ctx)


def schedule_dashboard_snapshot_refresh(ctx: DashboardDateContext) -> dict[str, Any]:
    cache_key = ctx.cache_key
    now = time.time()
    with _refresh_state_lock:
        state = _refresh_states.get(cache_key)
        if state is None:
            state = DashboardRefreshState()
            state.event.set()
            _refresh_states[cache_key] = state
        if state.suppressed_until > now:
            return {
                "started": False,
                "inflight": bool(state.inflight),
                "suppressed": True,
                "failureCount": int(state.failure_count),
            }
        if state.inflight:
            return {
                "started": False,
                "inflight": True,
                "suppressed": False,
                "failureCount": int(state.failure_count),
            }

    started = _ensure_background_refresh(cache_key, ctx)
    return {
        "started": bool(started),
        "inflight": False if started else True,
        "suppressed": False,
        "failureCount": int(_refresh_state_snapshot(cache_key).get("refreshFailureCount", 0)),
    }


# ── Detail page queries (independent cache) ──────────────────────────────────

def _get_cached_detail_value(
    cache_key: str,
    builder: Callable[[], Any],
    ttl_seconds: int = DETAIL_CACHE_TTL_SECONDS,
) -> Any:
    now = time.time()
    entry: Tuple[float, Any] | None = None

    with _detail_cache_lock:
        entry = _detail_cache.get(cache_key)

    stale = entry[1] if entry is not None else None
    stale_age = max(0.0, now - entry[0]) if entry is not None else None

    if entry is not None and stale_age is not None and stale_age < ttl_seconds:
        logger.debug("Detail cache hit | key={} cacheState=fresh age_s={}", cache_key, round(stale_age, 3))
        return stale

    if entry is not None and stale_age is not None and stale_age < DETAIL_MAX_STALE_SECONDS:
        logger.warning("Detail cache serving stale | key={} cacheState=stale age_s={}", cache_key, round(stale_age, 3))
        _refresh_detail_cache_async(cache_key, builder)
        return stale

    try:
        fresh = _build_detail_with_timeout(cache_key, builder, DETAIL_REFRESH_TIMEOUT_SECONDS)
        if _should_keep_existing_detail_cache(stale, fresh):
            logger.warning("Detail refresh produced empty payload; preserving previous cache | key={}", cache_key)
            if stale is not None:
                return stale
            raise DetailRefreshUnavailableError(f"No usable detail payload for {cache_key}")
        _store_detail_cache_value(cache_key, fresh)
        logger.info("Detail cache refreshed synchronously | key={} cacheState={}", cache_key, "miss" if entry is None else "expired")
        return fresh
    except Exception as exc:
        if entry is not None and stale_age is not None and stale_age < DETAIL_MAX_STALE_SECONDS:
            logger.warning("Detail refresh failed; serving stale cache | key={} error={}", cache_key, exc)
            _refresh_detail_cache_async(cache_key, builder)
            return stale
        raise DetailRefreshUnavailableError(f"Detail payload unavailable for {cache_key}: {exc}") from exc


def _get_detail_refresh_state(cache_key: str) -> DashboardRefreshState:
    with _detail_refresh_state_lock:
        state = _detail_refresh_states.get(cache_key)
        if state is None:
            state = DashboardRefreshState()
            state.event.set()
            _detail_refresh_states[cache_key] = state
        return state


def _acquire_detail_refresh_slot(cache_key: str) -> tuple[DashboardRefreshState, bool]:
    with _detail_refresh_state_lock:
        state = _detail_refresh_states.get(cache_key)
        if state is None:
            state = DashboardRefreshState()
            state.event.set()
            _detail_refresh_states[cache_key] = state
        if state.inflight:
            return state, False
        state.inflight = True
        state.event.clear()
        return state, True


def _release_detail_refresh_slot(cache_key: str) -> None:
    with _detail_refresh_state_lock:
        state = _detail_refresh_states.get(cache_key)
        if state is None:
            return
        state.inflight = False
        state.event.set()


def _should_keep_existing_detail_cache(stale: Any, fresh: Any) -> bool:
    if isinstance(stale, list) and stale and isinstance(fresh, list) and not fresh:
        return True
    if stale is not None and fresh is None:
        return True
    return False


def _store_detail_cache_value(cache_key: str, value: Any) -> None:
    with _detail_cache_lock:
        _detail_cache[cache_key] = (time.time(), value)


def _build_detail_with_timeout(cache_key: str, builder: Callable[[], Any], timeout_seconds: float) -> Any:
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="detail-refresh")
    future = executor.submit(builder)
    try:
        return future.result(timeout=max(0.1, float(timeout_seconds)))
    except FuturesTimeout as exc:
        future.cancel()
        logger.warning("Detail refresh timed out | key={} timeout_s={}", cache_key, timeout_seconds)
        raise TimeoutError(f"refresh timed out after {timeout_seconds}s") from exc
    finally:
        _shutdown_executor(executor, wait=False)


def _refresh_detail_cache_async(cache_key: str, builder: Callable[[], Any]) -> bool:
    _state, acquired = _acquire_detail_refresh_slot(cache_key)
    if not acquired:
        return False

    def _worker() -> None:
        try:
            fresh = builder()
            with _detail_cache_lock:
                existing = _detail_cache.get(cache_key)
                stale = existing[1] if existing is not None else None
            if _should_keep_existing_detail_cache(stale, fresh):
                logger.warning("Detail background refresh skipped empty payload | key={}", cache_key)
                return
            _store_detail_cache_value(cache_key, fresh)
            logger.info("Detail cache refreshed in background | key={}", cache_key)
        except Exception as exc:
            logger.warning("Detail background refresh failed | key={} error={}", cache_key, exc)
        finally:
            _release_detail_refresh_slot(cache_key)

    try:
        submit_background(_worker)
        return True
    except Exception as exc:
        logger.warning("Detail background refresh could not be scheduled | key={} error={}", cache_key, exc)
        _release_detail_refresh_slot(cache_key)
        return False


def get_topics_page(
    page: int = 0,
    size: int = 50,
    ctx: Optional[DashboardDateContext] = None,
) -> list[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"topics:{resolved_ctx.cache_key}:{page}:{size}"
    return _get_cached_detail_value(
        cache_key,
        lambda: comparative.get_all_topics(page, size, resolved_ctx),
        ttl_seconds=TOPICS_PAGE_CACHE_TTL_SECONDS,
    )


def _build_topic_overview_fallback(topic_row: dict, ctx: DashboardDateContext) -> dict:
    topic = str(topic_row.get("name") or topic_row.get("sourceTopic") or "Topic").strip() or "Topic"
    category = str(topic_row.get("category") or "General").strip() or "General"
    mentions = max(0, _to_int(topic_row.get("mentionCount") or topic_row.get("postCount") or 0))
    current_mentions = max(0, _to_int(topic_row.get("currentMentions"), mentions))
    previous_mentions = max(0, _to_int(topic_row.get("previousMentions") or topic_row.get("prev7Mentions") or 0))
    growth = _to_int(topic_row.get("growth7dPct") or 0)
    positive = max(0, _to_int(topic_row.get("sentimentPositive") or 0))
    negative = max(0, _to_int(topic_row.get("sentimentNegative") or 0))
    distinct_users = max(0, _to_int(topic_row.get("distinctUsers") or topic_row.get("userCount") or 0))
    distinct_channels = max(0, _to_int(topic_row.get("distinctChannels") or 0))
    top_channels = [str(ch).strip() for ch in (topic_row.get("topChannels") or []) if str(ch).strip()]
    channel_label = ", ".join(top_channels[:2]) if top_channels else "multiple channels"
    channel_label_ru = ", ".join(top_channels[:2]) if top_channels else "нескольких каналах"
    latest_at = str(topic_row.get("sampleEvidence", {}).get("timestamp") or "")[:10]
    evidence_ids = [
        str(item.get("id")).strip()
        for item in ((topic_row.get("questionEvidence") or []) + (topic_row.get("evidence") or []))
        if str(item.get("id") or "").strip()
    ]

    sentiment_label = "negative" if negative >= positive else "mixed-to-positive"
    sentiment_label_ru = "скорее негативную" if negative >= positive else "смешанную или скорее позитивную"
    summary_en = (
        f'"{topic}" is active in the current selected window with {mentions} mentions '
        f'({growth:+d}% versus the previous comparison window). '
        f'The discussion is concentrated in {channel_label} and currently leans {sentiment_label}.'
    )
    summary_ru = (
        f'Тема "{topic}" заметна в текущем выбранном окне: {mentions} упоминаний '
        f'({growth:+d}% к предыдущему окну сравнения). '
        f'Обсуждение сосредоточено в {channel_label_ru} и сейчас имеет {sentiment_label_ru} тональность.'
    )
    signals_en = [
        f"Volume: {current_mentions} mentions in the selected window versus {previous_mentions} in the previous comparison window.",
        f"Sentiment: {negative}% negative and {positive}% positive across {distinct_users} distinct participants.",
        f"Spread: activity spans {distinct_channels or len(top_channels)} channels; latest evidence date {latest_at or 'recent'}.",
    ]
    signals_ru = [
        f"Объём: {current_mentions} упоминаний в выбранном окне против {previous_mentions} в предыдущем окне сравнения.",
        f"Тональность: {negative}% негатива и {positive}% позитива при {distinct_users} уникальных участниках.",
        f"Ширина сигнала: активность идёт в {distinct_channels or len(top_channels)} каналах; последняя дата доказательства {latest_at or 'недавняя'}.",
    ]
    return {
        "topic": topic,
        "category": category,
        "status": "fallback",
        "summaryEn": summary_en,
        "summaryRu": summary_ru,
        "signalsEn": signals_en,
        "signalsRu": signals_ru,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "windowStart": ctx.from_date.isoformat(),
        "windowEnd": ctx.to_date.isoformat(),
        "windowDays": int(ctx.days),
        "evidenceIds": evidence_ids[:12],
    }


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
    payload = _get_cached_detail_value(cache_key, lambda: comparative.get_topic_detail(topic_name, category, resolved_ctx))
    if payload is None:
        return None
    normalized = dict(payload)
    normalized.setdefault("overview", _build_topic_overview_fallback(normalized, resolved_ctx))
    return normalized


def get_topic_evidence_page(
    topic_name: str,
    category: Optional[str] = None,
    view: str = "all",
    page: int = 0,
    size: int = 20,
    focus_id: Optional[str] = None,
    ctx: Optional[DashboardDateContext] = None,
) -> Optional[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"topic-evidence:{resolved_ctx.cache_key}:{topic_name}:{category or ''}:{view}:{page}:{size}:{focus_id or ''}"
    return _get_cached_detail_value(
        cache_key,
        lambda: comparative.get_topic_evidence_page(topic_name, category, view, page, size, focus_id, resolved_ctx),
    )


def get_channel_detail(
    channel_key: str,
    ctx: Optional[DashboardDateContext] = None,
) -> Optional[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"channel-detail:{resolved_ctx.cache_key}:{channel_key}"
    return _get_cached_detail_value(cache_key, lambda: comparative.get_channel_detail(channel_key, resolved_ctx))


def get_channel_posts_page(
    channel_key: str,
    page: int = 0,
    size: int = 20,
    ctx: Optional[DashboardDateContext] = None,
) -> Optional[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"channel-posts:{resolved_ctx.cache_key}:{channel_key}:{page}:{size}"
    return _get_cached_detail_value(
        cache_key,
        lambda: comparative.get_channel_posts_page(channel_key, page, size, resolved_ctx),
    )


def get_audience_detail(
    user_id: str,
    ctx: Optional[DashboardDateContext] = None,
) -> Optional[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"audience-detail:{resolved_ctx.cache_key}:{user_id}"
    return _get_cached_detail_value(cache_key, lambda: comparative.get_audience_detail(user_id, resolved_ctx))


def get_audience_messages_page(
    user_id: str,
    page: int = 0,
    size: int = 20,
    ctx: Optional[DashboardDateContext] = None,
) -> Optional[dict]:
    resolved_ctx = ctx or _default_dashboard_context()
    cache_key = f"audience-messages:{resolved_ctx.cache_key}:{user_id}:{page}:{size}"
    return _get_cached_detail_value(
        cache_key,
        lambda: comparative.get_audience_messages_page(user_id, page, size, resolved_ctx),
    )
