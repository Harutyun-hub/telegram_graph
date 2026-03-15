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
from typing import Callable, Dict, List, Optional, Tuple

from loguru import logger

from api import behavioral_briefs
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
TIER_TIMEOUT_SECONDS = max(0.5, float(os.getenv("DASH_TIER_TIMEOUT_SECONDS", "8.0")))
WAIT_FOR_REFRESH_SECONDS = max(1.0, float(os.getenv("DASH_WAIT_FOR_REFRESH_SECONDS", "8.0")))
DETAIL_CACHE_TTL_SECONDS = max(30, int(os.getenv("DETAIL_CACHE_TTL_SECONDS", "180")))

# If one of these tiers falls back during refresh, prefer preserving an
# existing healthy cache instead of replacing it with an empty/degraded view.
CRITICAL_TIERS = {"pulse", "strategic"}


_cache: dict = {}
_cache_ts: float = 0.0
_refresh_in_progress: bool = False
_last_refresh_error: Optional[str] = None

_cache_lock = threading.Lock()
_cache_cond = threading.Condition(_cache_lock)

_detail_cache_lock = threading.Lock()
_detail_cache: Dict[str, Tuple[float, list[dict]]] = {}


def _fallback_tiers(tier_times: Dict[str, Optional[float]]) -> list[str]:
    return [name for name, duration in tier_times.items() if name != "derived" and duration is None]


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


def _is_cache_valid(now: Optional[float] = None) -> bool:
    t = now if now is not None else time.time()
    return bool(_cache) and (t - _cache_ts) < CACHE_TTL_SECONDS


def invalidate_cache() -> None:
    global _cache, _cache_ts
    with _cache_cond:
        _cache = {}
        _cache_ts = 0.0
        _cache_cond.notify_all()
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
        return {"businessOpportunities": [], "jobSeeking": [], "jobTrends": [], "housingData": [], "housingHotTopics": []}
    if name == "comparative":
        return {"weeklyShifts": [], "sentimentByTopic": [], "topPosts": [], "contentTypePerformance": [], "vitalityIndicators": {}}
    if name == "details":
        return {"allTopics": [], "allChannels": [], "allAudience": []}
    return {}


# ── Tier builders ────────────────────────────────────────────────────────────

def _tier_pulse() -> dict:
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

    try:
        return {
            "communityHealth": pulse.get_community_health(),
            "trendingTopics": pulse.get_trending_topics(),
            "trendingNewTopics": _trending_new_topics(),
            "communityBrief": pulse.get_community_brief(),
        }
    except Exception as e:
        logger.error(f"Tier pulse failed: {e}")
        return _fallback_for_tier("pulse")


def _tier_strategic() -> dict:
    try:
        trend_lines = strategic.get_trend_lines()
        return {
            "topicBubbles": strategic.get_topic_bubbles(),
            "trendLines": trend_lines,
            "trendData": trend_lines,
            "heatmap": strategic.get_heatmap(),
            "questionCategories": strategic.get_question_categories(),
            "questionBriefs": question_briefs.get_question_briefs(),
            "lifecycleStages": strategic.get_lifecycle_stages(),
        }
    except Exception as e:
        logger.error(f"Tier strategic failed: {e}")
        return _fallback_for_tier("strategic")


def _tier_behavioral() -> dict:
    try:
        briefs = behavioral_briefs.get_behavioral_briefs()
        return {
            "problemBriefs": briefs.get("problemBriefs", []),
            "serviceGapBriefs": briefs.get("serviceGapBriefs", []),
            "problems": behavioral.get_problems(),
            "serviceGaps": behavioral.get_service_gaps(),
            "satisfactionAreas": behavioral.get_satisfaction_areas(),
            "moodData": behavioral.get_mood_data(),
            "moodConfig": {"sentiments": ["Positive", "Negative", "Neutral", "Mixed", "Urgent", "Sarcastic"]},
            "urgencySignals": briefs.get("urgencyBriefs", []),
        }
    except Exception as e:
        logger.error(f"Tier behavioral failed: {e}")
        return _fallback_for_tier("behavioral")


def _tier_network() -> dict:
    try:
        return {
            "communityChannels": network.get_community_channels(),
            "keyVoices": network.get_key_voices(),
            "hourlyActivity": network.get_hourly_activity(),
            "weeklyActivity": network.get_weekly_activity(),
            "recommendations": network.get_recommendations(),
            "viralTopics": network.get_viral_topics(),
        }
    except Exception as e:
        logger.error(f"Tier network failed: {e}")
        return _fallback_for_tier("network")


def _tier_psychographic() -> dict:
    try:
        integration_data = psychographic.get_integration_data()
        return {
            "personas": psychographic.get_personas(),
            "interests": psychographic.get_interests(),
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


def _tier_predictive() -> dict:
    try:
        return {
            "emergingInterests": predictive.get_emerging_interests(),
            "retentionFactors": predictive.get_retention_factors(),
            "churnSignals": predictive.get_churn_signals(),
            "growthFunnel": predictive.get_growth_funnel(),
            "decisionStages": predictive.get_decision_stages(),
        }
    except Exception as e:
        logger.error(f"Tier predictive failed: {e}")
        return _fallback_for_tier("predictive")


def _tier_actionable() -> dict:
    try:
        housing_data = actionable.get_housing_data()
        return {
            "businessOpportunities": actionable.get_business_opportunities(),
            "jobSeeking": actionable.get_job_seeking(),
            "jobTrends": actionable.get_job_trends(),
            "housingData": housing_data,
            "housingHotTopics": housing_data,
        }
    except Exception as e:
        logger.error(f"Tier actionable failed: {e}")
        return _fallback_for_tier("actionable")


def _tier_comparative() -> dict:
    try:
        return {
            "weeklyShifts": comparative.get_weekly_shifts(),
            "sentimentByTopic": comparative.get_sentiment_by_topic(),
            "topPosts": comparative.get_top_posts(),
            "contentTypePerformance": comparative.get_content_type_performance(),
            "vitalityIndicators": comparative.get_vitality_indicators(),
        }
    except Exception as e:
        logger.error(f"Tier comparative failed: {e}")
        return _fallback_for_tier("comparative")


def _tier_details() -> dict:
    try:
        return {
            "allTopics": comparative.get_all_topics(page=0, size=500),
            "allChannels": comparative.get_all_channels(),
            "allAudience": comparative.get_all_audience(page=0, size=500),
        }
    except Exception as e:
        logger.error(f"Tier details failed: {e}")
        return _fallback_for_tier("details")


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


def _ordered_tiers() -> List[Tuple[str, Callable[[], dict]]]:
    return [
        ("pulse", _tier_pulse),
        ("strategic", _tier_strategic),
        ("behavioral", _tier_behavioral),
        ("network", _tier_network),
        ("psychographic", _tier_psychographic),
        ("predictive", _tier_predictive),
        ("actionable", _tier_actionable),
        ("comparative", _tier_comparative),
    ]


def _run_tier_builder(fn: Callable[[], dict]) -> Tuple[dict, float]:
    t0 = time.time()
    payload = fn()
    return payload, round(time.time() - t0, 3)


def _build_snapshot_sequential() -> Tuple[dict, Dict[str, Optional[float]]]:
    data: dict = {}
    tier_times: Dict[str, Optional[float]] = {}

    for name, builder in _ordered_tiers():
        t0 = time.time()
        payload = builder()
        tier_times[name] = round(time.time() - t0, 3)
        data.update(payload)

    t0 = time.time()
    data.update(_tier_derived(data))
    tier_times["derived"] = round(time.time() - t0, 3)
    return data, tier_times


def _build_snapshot_parallel(use_timeouts: bool = True) -> Tuple[dict, Dict[str, Optional[float]]]:
    ordered = _ordered_tiers()
    tier_payloads: Dict[str, dict] = {}
    tier_times: Dict[str, Optional[float]] = {}

    executor = ThreadPoolExecutor(max_workers=PARALLEL_MAX_WORKERS, thread_name_prefix="dash-tier")
    futures = {name: executor.submit(_run_tier_builder, builder) for name, builder in ordered}

    try:
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
            except Exception as e:
                logger.error(f"Tier {name} crashed — using fallback: {e}")
                tier_payloads[name] = _fallback_for_tier(name)
                tier_times[name] = None
    finally:
        # wait=False prevents hanging cold starts when a tier thread stalls.
        executor.shutdown(wait=False)

    data: dict = {}
    for name, _builder in ordered:
        data.update(tier_payloads.get(name, _fallback_for_tier(name)))

    t0 = time.time()
    data.update(_tier_derived(data))
    tier_times["derived"] = round(time.time() - t0, 3)
    return data, tier_times


def _build_snapshot(use_timeouts: bool = True) -> Tuple[dict, Dict[str, Optional[float]], float, str]:
    mode = "parallel" if PARALLEL_ENABLED else "sequential"
    t0 = time.time()

    if PARALLEL_ENABLED:
        data, tier_times = _build_snapshot_parallel(use_timeouts=use_timeouts)
    else:
        data, tier_times = _build_snapshot_sequential()

    elapsed = round(time.time() - t0, 3)
    return data, tier_times, elapsed, mode


def _start_background_refresh_locked() -> None:
    global _refresh_in_progress
    if _refresh_in_progress:
        return
    _refresh_in_progress = True

    def _worker() -> None:
        global _refresh_in_progress, _last_refresh_error, _cache, _cache_ts
        try:
            data, tier_times, elapsed, mode = _build_snapshot(use_timeouts=True)

            preserve_existing = False
            fallback_tiers: list[str] = []
            with _cache_cond:
                preserve_existing, fallback_tiers = _should_preserve_existing_cache(
                    existing_cache=_cache,
                    new_snapshot=data,
                    tier_times=tier_times,
                )
                if preserve_existing:
                    _refresh_in_progress = False
                    _last_refresh_error = (
                        "Refresh produced degraded snapshot; preserved previous cache "
                        f"(fallback_tiers={fallback_tiers})"
                    )
                    _cache_cond.notify_all()
                else:
                    _cache = data
                    _cache_ts = time.time()
                    _refresh_in_progress = False
                    _last_refresh_error = None
                    _cache_cond.notify_all()

            if preserve_existing:
                logger.warning(
                    "Dashboard refresh kept previous cache because critical tiers fell back "
                    f"({fallback_tiers}); elapsed={elapsed}s mode={mode} tiers={tier_times}"
                )
            else:
                logger.success(f"Dashboard refresh completed in {elapsed}s ({mode}) | tiers={tier_times}")
        except Exception as e:
            with _cache_cond:
                _refresh_in_progress = False
                _last_refresh_error = str(e)
                _cache_cond.notify_all()
            logger.error(f"Dashboard refresh failed: {e}")

    threading.Thread(target=_worker, name="dashboard-refresh", daemon=True).start()


# ── Main aggregation API ─────────────────────────────────────────────────────

def get_dashboard_data(force_refresh: bool = False) -> dict:
    """Assemble full AppData snapshot with cache, SWR, and single-flight refresh."""
    global _cache, _cache_ts, _refresh_in_progress, _last_refresh_error

    now = time.time()
    with _cache_cond:
        has_cache = bool(_cache)
        had_cache_before_build = has_cache
        is_fresh = _is_cache_valid(now)
        stale_snapshot = _cache if has_cache else None

        if is_fresh and not force_refresh:
            logger.debug("Serving dashboard data from fresh cache")
            return _cache

        if has_cache and STALE_WHILE_REVALIDATE and not force_refresh:
            _start_background_refresh_locked()
            logger.debug("Serving stale dashboard cache while background refresh runs")
            return _cache

        if _refresh_in_progress:
            deadline = time.time() + WAIT_FOR_REFRESH_SECONDS
            while _refresh_in_progress and time.time() < deadline:
                _cache_cond.wait(timeout=0.1)

            if _cache and not force_refresh:
                logger.debug("Serving dashboard cache after waiting for in-flight refresh")
                return _cache

            # If refresh is still running and no cache available, wait until complete
            while _refresh_in_progress and not _cache:
                _cache_cond.wait(timeout=0.1)

            if _cache and not force_refresh:
                return _cache

        _refresh_in_progress = True

    # Build outside lock
    try:
        data, tier_times, elapsed, mode = _build_snapshot(use_timeouts=had_cache_before_build)

        preserve_existing = False
        fallback_tiers: list[str] = []
        with _cache_cond:
            preserve_existing, fallback_tiers = _should_preserve_existing_cache(
                existing_cache=_cache,
                new_snapshot=data,
                tier_times=tier_times,
            )
            if preserve_existing:
                _refresh_in_progress = False
                _last_refresh_error = (
                    "Synchronous rebuild produced degraded snapshot; preserved previous cache "
                    f"(fallback_tiers={fallback_tiers})"
                )
                preserved = _cache
                _cache_cond.notify_all()
            else:
                _cache = data
                _cache_ts = time.time()
                _refresh_in_progress = False
                _last_refresh_error = None
                preserved = None
                _cache_cond.notify_all()

        if preserve_existing and preserved is not None:
            logger.warning(
                "Dashboard rebuild preserved previous cache because critical tiers fell back "
                f"({fallback_tiers}); elapsed={elapsed}s mode={mode} tiers={tier_times}"
            )
            return preserved

        logger.success(f"Dashboard data assembled in {elapsed}s ({mode}) | tiers={tier_times}")
        return data
    except Exception as e:
        with _cache_cond:
            _refresh_in_progress = False
            _last_refresh_error = str(e)
            fallback: Optional[dict] = _cache if _cache else stale_snapshot
            _cache_cond.notify_all()

        if fallback is not None:
            logger.warning(f"Dashboard rebuild failed; serving stale cache instead: {e}")
            return fallback

        logger.error(f"Dashboard rebuild failed with no fallback cache: {e}")
        raise


# ── Detail page queries (independent cache) ──────────────────────────────────

def _get_cached_detail_list(
    cache_key: str,
    builder: Callable[[], list[dict]],
    ttl_seconds: int = DETAIL_CACHE_TTL_SECONDS,
) -> list[dict]:
    now = time.time()
    stale: Optional[list[dict]] = None

    with _detail_cache_lock:
        entry = _detail_cache.get(cache_key)
        if entry is not None:
            ts, data = entry
            stale = data
            if (now - ts) < ttl_seconds:
                return data

    try:
        fresh = builder() or []
        if stale and len(stale) > 0 and len(fresh) == 0:
            logger.warning(f"Detail query {cache_key} returned empty payload; serving stale cache")
            return stale
        with _detail_cache_lock:
            _detail_cache[cache_key] = (time.time(), fresh)
        return fresh
    except Exception as e:
        if stale is not None:
            logger.warning(f"Detail query {cache_key} failed; serving stale cache instead: {e}")
            return stale
        raise


def get_topics_page(page: int = 0, size: int = 50) -> list[dict]:
    cache_key = f"topics:{page}:{size}"
    return _get_cached_detail_list(cache_key, lambda: comparative.get_all_topics(page, size))


def get_channels_page() -> list[dict]:
    return _get_cached_detail_list("channels:all", comparative.get_all_channels)


def get_audience_page(page: int = 0, size: int = 50) -> list[dict]:
    cache_key = f"audience:{page}:{size}"
    return _get_cached_detail_list(cache_key, lambda: comparative.get_all_audience(page, size))
