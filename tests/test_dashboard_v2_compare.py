from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

from api.dashboard_dates import build_dashboard_date_context
from api.dashboard_v2_assembler import DashboardV2AssemblyResult
from api.dashboard_v2_compare import (
    BLOCKING_REASON_SOURCE_TRUTH_ERROR,
    BLOCKING_REASON_SOURCE_TRUTH_TIMEOUT,
    FACT_INVARIANT_VALIDATION_MODE,
    SOURCE_TRUTH_VALIDATION_MODE,
    WARNING_REASON_OLD_PATH_UNAVAILABLE,
    _conversation_trends_source_summary,
    _summarize_widget,
    run_dashboard_v2_compare,
)
from api.dashboard_v2_registry import ALL_WIDGET_IDS


class _CompareStore:
    def __init__(self) -> None:
        self.created_runs: list[dict] = []

    def create_compare_run(self, **kwargs) -> str:
        self.created_runs.append(dict(kwargs))
        return "compare-1"


class _CompareReadyWindowStore(_CompareStore):
    def summarize_v2_route_readiness(
        self,
        *,
        min_fact_version: int = 1,
        lookback_days: int = 400,
        from_date=None,
        to_date=None,
    ):
        del min_fact_version, lookback_days
        if from_date and to_date:
            return {
                "coverageStart": from_date.isoformat(),
                "coverageEnd": to_date.isoformat(),
                "routeReadyWindowStart": from_date.isoformat(),
                "routeReadyWindowEnd": to_date.isoformat(),
                "requestedFrom": from_date.isoformat(),
                "requestedTo": to_date.isoformat(),
                "v2RouteReady": True,
                "missingFamilies": [],
                "missingDates": [],
                "degradedFamilies": [],
                "degradedDates": [],
            }
        return {
            "coverageStart": "2025-03-14",
            "coverageEnd": "2026-04-15",
            "routeReadyWindowStart": "2026-04-15",
            "routeReadyWindowEnd": "2026-04-15",
            "v2RouteReady": False,
            "missingFamilies": ["channels", "users"],
            "degradedFamilies": [],
        }

    def get_range_readiness(self, *, from_date, to_date, fact_families, min_fact_version: int = 1):
        del from_date, to_date, fact_families, min_fact_version
        return {
            "availabilityStart": "2026-04-09",
            "availabilityEnd": "2026-04-15",
            "missingFactFamilies": [],
            "missingDates": [],
            "degradedFactFamilies": [],
            "degradedDates": [],
            "factFamilies": {},
            "ready": True,
        }


def _direct_truth_summaries() -> dict[str, dict[str, object]]:
    return {
        "community_brief": {
            "present": True,
            "itemCount": 12,
            "nonEmptyFields": ["messagesAnalyzed", "postsAnalyzedInWindow", "commentScopesAnalyzedInWindow", "totalAnalysesInWindow"],
            "topItems": [],
            "messagesAnalyzed": 14,
            "postsAnalyzedInWindow": 5,
            "commentScopesAnalyzedInWindow": 9,
            "totalAnalysesInWindow": 14,
        },
        "community_health_score": {
            "present": True,
            "itemCount": 1,
            "nonEmptyFields": ["components", "currentScore"],
            "topItems": [],
            "currentScore": 55,
            "componentCount": 1,
        },
        "trending_topics_feed": {
            "present": True,
            "itemCount": 1,
            "nonEmptyFields": ["trendingTopics"],
            "topItems": ["Road And Transit"],
        },
        "conversation_trends": {
            "present": True,
            "itemCount": 1,
            "nonEmptyFields": ["trendLines"],
            "topItems": ["Road And Transit"],
        },
        "topic_lifecycle": {
            "present": True,
            "itemCount": 1,
            "nonEmptyFields": ["lifecycleStages"],
            "topItems": ["Road And Transit"],
        },
        "sentiment_by_topic": {
            "present": True,
            "itemCount": 1,
            "nonEmptyFields": [],
            "topItems": ["Road And Transit"],
        },
        "week_over_week_shifts": {
            "present": True,
            "itemCount": 1,
            "nonEmptyFields": [],
            "topItems": ["Volume"],
        },
    }


def _old_snapshot() -> dict[str, object]:
    return {
        "communityBrief": {"messagesAnalyzed": 12, "postsAnalyzedInWindow": 4},
        "trendingTopics": [{"topic": "Road And Transit", "mentions": 10}],
        "trendLines": [{"topic": "Road And Transit", "bucket": "2026-04-15", "posts": 3}],
    }


def _v2_result() -> DashboardV2AssemblyResult:
    return DashboardV2AssemblyResult(
        snapshot={
            "communityBrief": {"messagesAnalyzed": 14, "postsAnalyzedInWindow": 5, "commentScopesAnalyzedInWindow": 9, "totalAnalysesInWindow": 14},
            "communityHealth": {"currentScore": 55, "components": [{"name": "trust"}]},
            "trendingTopics": [{"topic": "Road And Transit", "mentions": 11}],
            "trendingNewTopics": [],
            "topicBubbles": [],
            "trendLines": [{"topic": "Road And Transit", "bucket": "2026-04-15", "posts": 4}],
            "trendData": [],
            "questionCategories": [],
            "questionBriefs": [],
            "qaGap": {},
            "lifecycleStages": [{"topic": "Road And Transit", "stage": "growing", "weeklyCurrent": 4, "weeklyDelta": 2}],
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
            "sentimentByTopic": [{"topic": "Road And Transit", "positive": 60}],
            "weeklyShifts": [{"label": "Volume", "delta": 7}],
            "topPosts": [],
            "contentTypePerformance": [],
            "vitalityIndicators": {},
        },
        cache_status="assembled_exact_from_facts",
        cache_source="assembled",
        range_resolution_path="v2_assembled_exact_from_facts",
        is_stale=False,
        fact_version=2,
        artifact_version=1,
        fact_watermark="2026-04-18T11:00:00+00:00",
        materialized_at="2026-04-18T11:00:00+00:00",
        stale_fact_families=[],
    )


def _source_truth_summary_builders(
    summaries: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    summary_map = summaries or _direct_truth_summaries()
    return {widget_id: (lambda ctx, cache, value=value: value) for widget_id, value in summary_map.items()}


class DashboardV2CompareTests(unittest.TestCase):
    def test_compare_uses_source_truth_and_fact_invariant_validation(self) -> None:
        store = _CompareReadyWindowStore()
        ctx = build_dashboard_date_context("2026-04-09", "2026-04-15")

        with patch("api.dashboard_v2_compare.aggregator.get_dashboard_data", return_value=_old_snapshot()), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=_v2_result()) as assemble_mock, \
             patch("api.dashboard_v2_compare._source_truth_summary_builders", return_value=_source_truth_summary_builders()):
            result = run_dashboard_v2_compare(store, from_value=ctx.from_date.isoformat(), to_value=ctx.to_date.isoformat())

        self.assertEqual(result["compareId"], "compare-1")
        self.assertEqual(result["v2Status"], "ready")
        self.assertEqual(result["semanticStatus"], "ready")
        self.assertTrue(result["semanticGateReady"])
        self.assertEqual(result["validationSummary"]["validationModel"], "source_truth_primary")
        self.assertEqual(len(result["widgetDiffs"]), len(ALL_WIDGET_IDS))
        self.assertEqual(result["widgetDiffs"]["community_brief"]["validation"]["mode"], SOURCE_TRUTH_VALIDATION_MODE)
        self.assertEqual(result["widgetDiffs"]["question_cloud"]["validation"]["mode"], FACT_INVARIANT_VALIDATION_MODE)
        self.assertEqual(store.created_runs[0]["v2_meta"]["rangeResolutionPath"], "v2_assembled_exact_from_facts")
        self.assertTrue(assemble_mock.call_args.kwargs["prefer_cached_exact_artifacts"])
        self.assertFalse(assemble_mock.call_args.kwargs["allow_stale_exact_last_known_good"])

    def test_compare_returns_structured_failure_when_source_truth_validator_errors(self) -> None:
        store = _CompareReadyWindowStore()
        ctx = build_dashboard_date_context("2026-04-15", "2026-04-15")
        summaries = _direct_truth_summaries()
        builders = _source_truth_summary_builders(summaries)
        builders["community_brief"] = lambda ctx, cache: (_ for _ in ()).throw(RuntimeError("neo4j overloaded"))

        with patch("api.dashboard_v2_compare.aggregator.get_dashboard_data", return_value=_old_snapshot()), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=_v2_result()), \
             patch("api.dashboard_v2_compare._source_truth_summary_builders", return_value=builders):
            result = run_dashboard_v2_compare(store, from_value=ctx.from_date.isoformat(), to_value=ctx.to_date.isoformat())

        validation = result["widgetDiffs"]["community_brief"]["validation"]
        self.assertEqual(result["semanticStatus"], "failed")
        self.assertFalse(result["semanticGateReady"])
        self.assertEqual(validation["semanticStatus"], "fail")
        self.assertEqual(validation["blockingReasons"], [BLOCKING_REASON_SOURCE_TRUTH_ERROR])
        self.assertEqual(result["validationSummary"]["blockingWidgets"][0]["widgetId"], "community_brief")

    def test_compare_returns_structured_failure_when_source_truth_validator_times_out(self) -> None:
        store = _CompareReadyWindowStore()
        ctx = build_dashboard_date_context("2026-04-15", "2026-04-15")
        summaries = _direct_truth_summaries()
        old_snapshot = _old_snapshot()

        def _fake_execute(*, label: str, timeout_seconds: float, fn):
            del timeout_seconds, fn
            if label == "old-path":
                return {"status": "ok", "value": old_snapshot}
            if label == "source-truth-community_brief":
                return {"status": "timeout", "error": "source-truth-community_brief timed out after 20.0s"}
            widget_id = label.replace("source-truth-", "").replace("-", "_")
            return {"status": "ok", "value": summaries[widget_id]}

        with patch("api.dashboard_v2_compare._execute_with_timeout", side_effect=_fake_execute), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=_v2_result()), \
             patch("api.dashboard_v2_compare._source_truth_summary_builders", return_value=_source_truth_summary_builders(summaries)):
            result = run_dashboard_v2_compare(store, from_value=ctx.from_date.isoformat(), to_value=ctx.to_date.isoformat())

        validation = result["widgetDiffs"]["community_brief"]["validation"]
        self.assertEqual(validation["blockingReasons"], [BLOCKING_REASON_SOURCE_TRUTH_TIMEOUT])
        self.assertEqual(result["semanticStatus"], "failed")
        self.assertFalse(result["semanticGateReady"])

    def test_compare_marks_remaining_source_truth_widgets_timed_out_when_total_budget_is_exhausted(self) -> None:
        store = _CompareReadyWindowStore()
        ctx = build_dashboard_date_context("2026-04-15", "2026-04-15")
        summaries = _direct_truth_summaries()
        call_count = {"source_truth": 0}

        def _fake_execute(*, label: str, timeout_seconds: float, fn):
            del timeout_seconds, fn
            if label == "old-path":
                return {"status": "ok", "value": _old_snapshot()}
            call_count["source_truth"] += 1
            if call_count["source_truth"] == 1:
                return {"status": "ok", "value": summaries["community_brief"]}
            raise AssertionError("source-truth execution should stop once the total budget is exhausted")

        monotonic_values = iter([0.0, 0.0, 61.0, 61.0, 61.0, 61.0, 61.0, 61.0])

        with patch("api.dashboard_v2_compare._execute_with_timeout", side_effect=_fake_execute), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=_v2_result()), \
             patch("api.dashboard_v2_compare._source_truth_summary_builders", return_value=_source_truth_summary_builders(summaries)), \
             patch("api.dashboard_v2_compare.time.monotonic", side_effect=lambda: next(monotonic_values)):
            result = run_dashboard_v2_compare(store, from_value=ctx.from_date.isoformat(), to_value=ctx.to_date.isoformat())

        self.assertEqual(result["widgetDiffs"]["community_health_score"]["validation"]["blockingReasons"], [BLOCKING_REASON_SOURCE_TRUTH_TIMEOUT])
        self.assertEqual(result["widgetDiffs"]["week_over_week_shifts"]["validation"]["blockingReasons"], [BLOCKING_REASON_SOURCE_TRUTH_TIMEOUT])
        self.assertEqual(result["semanticStatus"], "failed")
        self.assertFalse(result["semanticGateReady"])

    def test_compare_keeps_old_path_failure_non_blocking(self) -> None:
        store = _CompareReadyWindowStore()
        ctx = build_dashboard_date_context("2026-04-15", "2026-04-15")
        summaries = _direct_truth_summaries()

        def _fake_execute(*, label: str, timeout_seconds: float, fn):
            del timeout_seconds, fn
            if label == "old-path":
                return {"status": "error", "error": "legacy dashboard unavailable"}
            widget_id = label.replace("source-truth-", "").replace("-", "_")
            return {"status": "ok", "value": summaries[widget_id]}

        with patch("api.dashboard_v2_compare._execute_with_timeout", side_effect=_fake_execute), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=_v2_result()), \
             patch("api.dashboard_v2_compare._source_truth_summary_builders", return_value=_source_truth_summary_builders(summaries)):
            result = run_dashboard_v2_compare(store, from_value=ctx.from_date.isoformat(), to_value=ctx.to_date.isoformat())

        self.assertEqual(result["semanticStatus"], "ready")
        self.assertTrue(result["semanticGateReady"])
        self.assertEqual(result["regressionStatus"], "unavailable")
        self.assertEqual(
            result["widgetDiffs"]["community_brief"]["validation"]["warnings"],
            [WARNING_REASON_OLD_PATH_UNAVAILABLE],
        )
        self.assertEqual(store.created_runs[0]["old_path_meta"]["status"], "unavailable")

    def test_conversation_trends_source_summary_normalizes_raw_rows(self) -> None:
        ctx = build_dashboard_date_context("2026-03-17", "2026-04-15")
        raw_rows = [
            {"topic": "Road And Transit", "bucket": "2026-04-01", "posts": 3},
            {"topic": "Road And Transit", "bucket": "2026-04-02", "posts": 5},
            {"topic": "Water Security", "bucket": "2026-04-01", "posts": 2},
        ]

        with patch("api.dashboard_v2_compare.strategic.get_trend_lines", return_value=raw_rows):
            summary = _conversation_trends_source_summary(ctx, {})

        self.assertTrue(summary["present"])
        self.assertEqual(summary["itemCount"], 2)
        self.assertEqual(summary["topItems"], ["Road And Transit", "Water Security"])

    def test_summarize_widget_trending_topics_matches_widget_subset_contract(self) -> None:
        rows = [
            {"topic": f"Topic {i}", "currentMentions": 20 - i, "distinctChannels": 3, "distinctUsers": 4}
            for i in range(15)
        ]
        summary = _summarize_widget("trending_topics_feed", {"trendingTopics": rows})

        self.assertEqual(summary["itemCount"], 12)
        self.assertEqual(summary["topItems"], ["Topic 0", "Topic 1", "Topic 2", "Topic 3", "Topic 4"])

    def test_summarize_widget_aggregates_conversation_trend_topics(self) -> None:
        summary = _summarize_widget(
            "conversation_trends",
            {
                "trendLines": [
                    {"topic": "Topic A", "bucket": "2026-04-01", "posts": 1},
                    {"topic": "Topic B", "bucket": "2026-04-01", "posts": 2},
                    {"topic": "Topic A", "bucket": "2026-04-02", "posts": 5},
                    {"topic": "Topic C", "bucket": "2026-04-02", "posts": 4},
                ]
            },
        )
        self.assertEqual(summary["itemCount"], 3)
        self.assertEqual(summary["topItems"], ["Topic A", "Topic C", "Topic B"])

    def test_summarize_widget_lifecycle_matches_stage_subset_contract(self) -> None:
        summary = _summarize_widget(
            "topic_lifecycle",
            [
                {"topic": "Growth 1", "stage": "growing", "weeklyCurrent": 10, "weeklyDelta": 4},
                {"topic": "Growth 2", "stage": "emerging", "weeklyCurrent": 8, "weeklyDelta": 3},
                {"topic": "Decline 1", "stage": "declining", "weeklyCurrent": 9, "weeklyDelta": -1},
            ],
        )
        self.assertEqual(summary["itemCount"], 3)
        self.assertEqual(summary["topItems"], ["Growth 1", "Growth 2", "Decline 1"])

    def test_summarize_widget_aggregates_sentiment_topic_totals(self) -> None:
        summary = _summarize_widget(
            "sentiment_by_topic",
            [
                {"topic": "Road And Transit", "sentiment": "Positive", "count": 3},
                {"topic": "Water Security", "sentiment": "Positive", "count": 2},
                {"topic": "Road And Transit", "sentiment": "Negative", "count": 4},
            ],
        )
        self.assertEqual(summary["itemCount"], 2)
        self.assertEqual(summary["topItems"], ["Road And Transit", "Water Security"])


if __name__ == "__main__":
    unittest.main()
