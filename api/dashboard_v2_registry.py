from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


FACT_FAMILY_CONTENT = "content"
FACT_FAMILY_TOPICS = "topics"
FACT_FAMILY_CHANNELS = "channels"
FACT_FAMILY_USERS = "users"
FACT_FAMILY_BEHAVIORAL = "behavioral"
FACT_FAMILY_PREDICTIVE = "predictive"
FACT_FAMILY_ACTIONABLE = "actionable"
FACT_FAMILY_COMPARATIVE = "comparative"

FACT_FAMILIES: tuple[str, ...] = (
    FACT_FAMILY_CONTENT,
    FACT_FAMILY_TOPICS,
    FACT_FAMILY_CHANNELS,
    FACT_FAMILY_USERS,
    FACT_FAMILY_BEHAVIORAL,
    FACT_FAMILY_PREDICTIVE,
    FACT_FAMILY_ACTIONABLE,
    FACT_FAMILY_COMPARATIVE,
)

FACT_TABLE_BY_FAMILY = {
    FACT_FAMILY_CONTENT: "dashboard_fact_daily_content",
    FACT_FAMILY_TOPICS: "dashboard_fact_daily_topics",
    FACT_FAMILY_CHANNELS: "dashboard_fact_daily_channels",
    FACT_FAMILY_USERS: "dashboard_fact_daily_users",
    FACT_FAMILY_BEHAVIORAL: "dashboard_fact_daily_behavioral",
    FACT_FAMILY_PREDICTIVE: "dashboard_fact_daily_predictive",
    FACT_FAMILY_ACTIONABLE: "dashboard_fact_daily_actionable",
    FACT_FAMILY_COMPARATIVE: "dashboard_fact_daily_comparative",
}

SECONDARY_MATERIALIZATION_TABLES = {
    "ai_question_briefs": "dashboard_ai_question_briefs",
    "ai_behavioral_briefs": "dashboard_ai_behavioral_briefs",
    "ai_recommendation_briefs": "dashboard_ai_recommendation_briefs",
    "ai_opportunity_briefs": "dashboard_ai_opportunity_briefs",
    "persona_clusters": "dashboard_persona_clusters",
    "topic_overviews_v2": "dashboard_topic_overviews_v2",
}


@dataclass(frozen=True)
class DashboardV2WidgetCoverage:
    widget_id: str
    fact_families: tuple[str, ...]
    exact_fact_backed: bool
    secondary_materialization: str | None = None
    unresolved: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


WIDGET_COVERAGE: tuple[DashboardV2WidgetCoverage, ...] = (
    DashboardV2WidgetCoverage("community_brief", (FACT_FAMILY_CONTENT, FACT_FAMILY_TOPICS), True),
    DashboardV2WidgetCoverage(
        "community_health_score",
        (FACT_FAMILY_CONTENT, FACT_FAMILY_TOPICS),
        True,
        unresolved="Health formula versioning only",
    ),
    DashboardV2WidgetCoverage(
        "trending_topics_feed",
        (FACT_FAMILY_TOPICS, FACT_FAMILY_CONTENT),
        True,
        unresolved="Ranking policy versioning",
    ),
    DashboardV2WidgetCoverage("topic_landscape", (FACT_FAMILY_TOPICS,), True),
    DashboardV2WidgetCoverage("conversation_trends", (FACT_FAMILY_TOPICS,), True),
    DashboardV2WidgetCoverage(
        "question_cloud",
        (FACT_FAMILY_CONTENT,),
        False,
        secondary_materialization="ai_question_briefs",
        unresolved="Final grouped wording",
    ),
    DashboardV2WidgetCoverage("topic_lifecycle", (FACT_FAMILY_TOPICS,), True),
    DashboardV2WidgetCoverage(
        "problem_tracker",
        (FACT_FAMILY_BEHAVIORAL,),
        False,
        secondary_materialization="ai_behavioral_briefs",
        unresolved="Final card synthesis",
    ),
    DashboardV2WidgetCoverage(
        "service_gap_detector",
        (FACT_FAMILY_BEHAVIORAL,),
        False,
        secondary_materialization="ai_behavioral_briefs",
        unresolved="Final card synthesis",
    ),
    DashboardV2WidgetCoverage("satisfaction_by_area", (FACT_FAMILY_BEHAVIORAL,), True),
    DashboardV2WidgetCoverage("mood_over_time", (FACT_FAMILY_BEHAVIORAL,), True),
    DashboardV2WidgetCoverage(
        "emotional_urgency_index",
        (FACT_FAMILY_BEHAVIORAL,),
        False,
        secondary_materialization="ai_behavioral_briefs",
        unresolved="Final clustering",
    ),
    DashboardV2WidgetCoverage("top_channels", (FACT_FAMILY_CHANNELS,), True),
    DashboardV2WidgetCoverage(
        "key_voices",
        (FACT_FAMILY_USERS, FACT_FAMILY_CHANNELS),
        True,
        unresolved="Influence formula versioning",
    ),
    DashboardV2WidgetCoverage(
        "recommendation_tracker",
        (FACT_FAMILY_CONTENT,),
        False,
        secondary_materialization="ai_recommendation_briefs",
        unresolved="Final extraction/grouping",
    ),
    DashboardV2WidgetCoverage("information_velocity", (FACT_FAMILY_TOPICS, FACT_FAMILY_CONTENT), True),
    DashboardV2WidgetCoverage(
        "persona_gallery",
        (FACT_FAMILY_USERS,),
        False,
        secondary_materialization="persona_clusters",
        unresolved="Cluster labels",
    ),
    DashboardV2WidgetCoverage(
        "interest_radar",
        (FACT_FAMILY_USERS, FACT_FAMILY_TOPICS),
        True,
        unresolved="Taxonomy versioning",
    ),
    DashboardV2WidgetCoverage("community_growth_funnel", (FACT_FAMILY_USERS, FACT_FAMILY_PREDICTIVE), True),
    DashboardV2WidgetCoverage(
        "retention_risk_gauge",
        (FACT_FAMILY_USERS, FACT_FAMILY_PREDICTIVE),
        True,
        unresolved="Risk-model versioning",
    ),
    DashboardV2WidgetCoverage(
        "decision_stage_tracker",
        (FACT_FAMILY_USERS, FACT_FAMILY_PREDICTIVE),
        True,
        unresolved="Stage-rule versioning",
    ),
    DashboardV2WidgetCoverage(
        "emerging_interests",
        (FACT_FAMILY_TOPICS, FACT_FAMILY_PREDICTIVE),
        True,
        unresolved="Novelty-threshold versioning",
    ),
    DashboardV2WidgetCoverage("new_vs_returning_voice", (FACT_FAMILY_USERS, FACT_FAMILY_CONTENT), True),
    DashboardV2WidgetCoverage(
        "business_opportunity_tracker",
        (FACT_FAMILY_ACTIONABLE,),
        False,
        secondary_materialization="ai_opportunity_briefs",
        unresolved="Final synthesis",
    ),
    DashboardV2WidgetCoverage("job_market_pulse", (FACT_FAMILY_ACTIONABLE, FACT_FAMILY_CONTENT), True),
    DashboardV2WidgetCoverage("week_over_week_shifts", (FACT_FAMILY_COMPARATIVE,), True),
    DashboardV2WidgetCoverage("sentiment_by_topic", (FACT_FAMILY_TOPICS, FACT_FAMILY_COMPARATIVE), True),
    DashboardV2WidgetCoverage("content_performance", (FACT_FAMILY_CONTENT, FACT_FAMILY_COMPARATIVE), True),
)

WIDGET_COVERAGE_BY_ID = {item.widget_id: item for item in WIDGET_COVERAGE}
ALL_WIDGET_IDS: tuple[str, ...] = tuple(item.widget_id for item in WIDGET_COVERAGE)
EXACT_FACT_BACKED_WIDGET_IDS = tuple(item.widget_id for item in WIDGET_COVERAGE if item.exact_fact_backed)
SECONDARY_MATERIALIZED_WIDGET_IDS = tuple(
    item.widget_id for item in WIDGET_COVERAGE if item.secondary_materialization is not None
)
SECONDARY_STORAGE_KEY_BY_WIDGET_ID = {
    item.widget_id: str(item.secondary_materialization)
    for item in WIDGET_COVERAGE
    if item.secondary_materialization is not None
}
FULL_DASHBOARD_REQUIRED_FACT_FAMILIES: tuple[str, ...] = FACT_FAMILIES
DIRECT_SOURCE_TRUTH_WIDGET_IDS = (
    "community_brief",
    "community_health_score",
    "trending_topics_feed",
    "conversation_trends",
    "topic_lifecycle",
    "sentiment_by_topic",
    "week_over_week_shifts",
)

RAW_SNAPSHOT_FIELDS_BY_WIDGET_ID: dict[str, tuple[str, ...]] = {
    "community_brief": ("communityBrief",),
    "community_health_score": ("communityHealth",),
    "trending_topics_feed": ("trendingTopics", "trendingNewTopics"),
    "topic_landscape": ("topicBubbles",),
    "conversation_trends": ("trendLines", "trendData"),
    "question_cloud": ("questionCategories", "questionBriefs", "qaGap"),
    "topic_lifecycle": ("lifecycleStages",),
    "problem_tracker": ("problemBriefs", "problems"),
    "service_gap_detector": ("serviceGapBriefs", "serviceGaps"),
    "satisfaction_by_area": ("satisfactionAreas",),
    "mood_over_time": ("moodData", "moodConfig"),
    "emotional_urgency_index": ("urgencySignals",),
    "top_channels": ("communityChannels", "hourlyActivity", "weeklyActivity"),
    "key_voices": ("keyVoices",),
    "recommendation_tracker": ("recommendations",),
    "information_velocity": ("viralTopics",),
    "persona_gallery": ("personas", "origins", "integrationData", "integrationLevels", "integrationSeriesConfig"),
    "interest_radar": ("interests",),
    "community_growth_funnel": ("growthFunnel",),
    "retention_risk_gauge": ("retentionFactors", "churnSignals"),
    "decision_stage_tracker": ("decisionStages",),
    "emerging_interests": ("emergingInterests",),
    "new_vs_returning_voice": ("newVsReturningVoiceWidget",),
    "business_opportunity_tracker": ("businessOpportunityBriefs", "businessOpportunities"),
    "job_market_pulse": ("jobSeeking", "jobTrends", "housingData", "housingHotTopics"),
    "week_over_week_shifts": ("weeklyShifts",),
    "sentiment_by_topic": ("sentimentByTopic",),
    "content_performance": ("topPosts", "contentTypePerformance", "vitalityIndicators"),
}


def get_widget_coverage(widget_id: str) -> DashboardV2WidgetCoverage:
    try:
        return WIDGET_COVERAGE_BY_ID[widget_id]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise KeyError(f"Unknown dashboard V2 widget: {widget_id}") from exc


def build_widget_coverage_report() -> list[dict[str, Any]]:
    return [item.to_dict() for item in WIDGET_COVERAGE]


def validate_widget_coverage(expected_widget_ids: tuple[str, ...] | list[str]) -> tuple[list[str], list[str]]:
    expected = {str(item) for item in expected_widget_ids}
    actual = set(ALL_WIDGET_IDS)
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    return missing, unexpected


def required_fact_families_for_widgets(widget_ids: tuple[str, ...] | list[str] | None = None) -> tuple[str, ...]:
    target_ids = tuple(widget_ids or ALL_WIDGET_IDS)
    families: set[str] = set()
    for widget_id in target_ids:
        families.update(get_widget_coverage(widget_id).fact_families)
    return tuple(family for family in FACT_FAMILIES if family in families)
