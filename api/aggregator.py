"""
aggregator.py — Assembles the full AppData response from all query modules.

This is the single function the API endpoint calls. It runs all queries,
shapes the results to match the frontend's AppData TypeScript interface,
and applies the in-memory cache.
"""
from __future__ import annotations
import time
from loguru import logger

from api.queries import pulse, strategic, behavioral, network
from api.queries import psychographic, predictive, actionable, comparative

# ── In-memory cache ──────────────────────────────────────────────────────────

_cache: dict = {}
_cache_ts: float = 0
CACHE_TTL_SECONDS = 300  # 5 minutes


def _is_cache_valid() -> bool:
    return bool(_cache) and (time.time() - _cache_ts) < CACHE_TTL_SECONDS


def invalidate_cache():
    global _cache, _cache_ts
    _cache = {}
    _cache_ts = 0


# ── Main aggregation ─────────────────────────────────────────────────────────

def get_dashboard_data() -> dict:
    """
    Assemble the full AppData response.
    Returns a dict matching the frontend's AppData interface.
    """
    global _cache, _cache_ts

    if _is_cache_valid():
        logger.debug("Serving dashboard data from cache")
        return _cache

    logger.info("Aggregating dashboard data from Neo4j...")
    t0 = time.time()

    data = {}

    # ── Tier 1: Community Pulse ──
    try:
        data["communityHealth"] = pulse.get_community_health()
        data["trendingTopics"] = pulse.get_trending_topics()
        data["communityBrief"] = pulse.get_community_brief()
    except Exception as e:
        logger.error(f"Tier 1 (pulse) failed: {e}")
        data["communityHealth"] = {"score": 0, "trend": "neutral"}
        data["trendingTopics"] = []
        data["communityBrief"] = {}

    # ── Tier 2: Strategic Topics ──
    try:
        data["topicBubbles"] = strategic.get_topic_bubbles()
        data["trendLines"] = strategic.get_trend_lines()
        data["trendData"] = data["trendLines"]  # alias
        data["heatmap"] = strategic.get_heatmap()
        data["questionCategories"] = strategic.get_question_categories()
        data["lifecycleStages"] = strategic.get_lifecycle_stages()
    except Exception as e:
        logger.error(f"Tier 2 (strategic) failed: {e}")
        data.update({"topicBubbles": [], "trendLines": [], "trendData": [],
                      "heatmap": [], "questionCategories": [], "lifecycleStages": []})

    # ── Tier 3: Behavioral / Pain Points ──
    try:
        data["problems"] = behavioral.get_problems()
        data["serviceGaps"] = behavioral.get_service_gaps()
        data["satisfactionAreas"] = behavioral.get_satisfaction_areas()
        data["moodData"] = behavioral.get_mood_data()
        data["moodConfig"] = {"sentiments": ["Positive", "Negative", "Neutral", "Mixed", "Urgent", "Sarcastic"]}
        data["urgencySignals"] = behavioral.get_urgency_signals()
    except Exception as e:
        logger.error(f"Tier 3 (behavioral) failed: {e}")
        data.update({"problems": [], "serviceGaps": [], "satisfactionAreas": [],
                      "moodData": [], "moodConfig": {}, "urgencySignals": []})

    # ── Tier 4: Network / Channels ──
    try:
        data["communityChannels"] = network.get_community_channels()
        data["keyVoices"] = network.get_key_voices()
        data["hourlyActivity"] = network.get_hourly_activity()
        data["weeklyActivity"] = network.get_weekly_activity()
        data["recommendations"] = network.get_recommendations()
        data["viralTopics"] = network.get_viral_topics()
    except Exception as e:
        logger.error(f"Tier 4 (network) failed: {e}")
        data.update({"communityChannels": [], "keyVoices": [], "hourlyActivity": [],
                      "weeklyActivity": [], "recommendations": [], "viralTopics": []})

    # ── Tier 5: Psychographic / Audience ──
    try:
        data["personas"] = psychographic.get_personas()
        data["interests"] = psychographic.get_interests()
        data["origins"] = psychographic.get_origins()
        data["integrationData"] = psychographic.get_integration_data()
        data["integrationLevels"] = data["integrationData"]  # alias
        data["integrationConfig"] = {"languages": ["ru", "hy", "en", "mixed"]}
        data["integrationSeriesConfig"] = [
            {"key": "learning", "color": "#3b82f6", "label": "Learning & Mixing", "labelRu": "Учится и смешивается", "polarity": "positive"},
            {"key": "bilingual", "color": "#8b5cf6", "label": "Bilingual Bubble", "labelRu": "Двуязычный пузырь", "polarity": "neutral"},
            {"key": "russianOnly", "color": "#f59e0b", "label": "Russian Only", "labelRu": "Только по-русски", "polarity": "negative"},
            {"key": "integrated", "color": "#10b981", "label": "Fully Integrated", "labelRu": "Полностью интегрирован", "polarity": "positive"},
        ]
        data["newcomerJourney"] = psychographic.get_newcomer_journey()
    except Exception as e:
        logger.error(f"Tier 5 (psychographic) failed: {e}")
        data.update({"personas": [], "interests": [], "origins": [],
                      "integrationData": [], "integrationLevels": [],
                      "integrationConfig": {}, "integrationSeriesConfig": [], "newcomerJourney": []})

    # ── Tier 6: Predictive ──
    try:
        data["emergingInterests"] = predictive.get_emerging_interests()
        data["retentionFactors"] = predictive.get_retention_factors()
        data["churnSignals"] = predictive.get_churn_signals()
        data["growthFunnel"] = predictive.get_growth_funnel()
        data["decisionStages"] = predictive.get_decision_stages()
    except Exception as e:
        logger.error(f"Tier 6 (predictive) failed: {e}")
        data.update({"emergingInterests": [], "retentionFactors": [],
                      "churnSignals": [], "growthFunnel": [], "decisionStages": []})

    # ── Tier 7: Actionable / Business ──
    try:
        data["businessOpportunities"] = actionable.get_business_opportunities()
        data["jobSeeking"] = actionable.get_job_seeking()
        data["jobTrends"] = actionable.get_job_trends()
        data["housingData"] = actionable.get_housing_data()
        data["housingHotTopics"] = data["housingData"]  # alias
    except Exception as e:
        logger.error(f"Tier 7 (actionable) failed: {e}")
        data.update({"businessOpportunities": [], "jobSeeking": [],
                      "jobTrends": [], "housingData": [], "housingHotTopics": []})

    # ── Tier 8: Comparative / Deep Dive ──
    try:
        data["weeklyShifts"] = comparative.get_weekly_shifts()
        data["sentimentByTopic"] = comparative.get_sentiment_by_topic()
        data["topPosts"] = comparative.get_top_posts()
        data["contentTypePerformance"] = comparative.get_content_type_performance()
        data["vitalityIndicators"] = comparative.get_vitality_indicators()
    except Exception as e:
        logger.error(f"Tier 8 (comparative) failed: {e}")
        data.update({"weeklyShifts": [], "sentimentByTopic": [],
                      "topPosts": [], "contentTypePerformance": [],
                      "vitalityIndicators": {}})

    # ── Derived / Computed fields ──
    try:
        data["voiceData"] = data.get("keyVoices", [])
        data["topNewTopics"] = data.get("emergingInterests", [])
        data["qaGap"] = {
            "totalQuestions": len(data.get("questionCategories", [])),
            "answered": 0,  # computed from reply chains
        }
    except Exception:
        pass

    # ── Detail pages payload (raw rows, frontend adapter reshapes) ──
    try:
        data["allTopics"] = comparative.get_all_topics(page=0, size=500)
        data["allChannels"] = comparative.get_all_channels()
        data["allAudience"] = comparative.get_all_audience(page=0, size=500)
    except Exception as e:
        logger.error(f"Detail pages payload failed: {e}")
        data.setdefault("allTopics", [])
        data.setdefault("allChannels", [])
        data.setdefault("allAudience", [])

    elapsed = round(time.time() - t0, 2)
    logger.success(f"Dashboard data assembled in {elapsed}s — {len(data)} keys")

    # Cache the result
    _cache = data
    _cache_ts = time.time()

    return data


# ── Detail page queries (not cached) ─────────────────────────────────────────

def get_topics_page(page: int = 0, size: int = 50) -> list[dict]:
    return comparative.get_all_topics(page, size)


def get_channels_page() -> list[dict]:
    return comparative.get_all_channels()


def get_audience_page(page: int = 0, size: int = 50) -> list[dict]:
    return comparative.get_all_audience(page, size)
