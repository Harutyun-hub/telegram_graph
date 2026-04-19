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


def _direct_truth_payloads() -> dict[str, object]:
    return {
        "community_brief": {"messagesAnalyzed": 14, "postsAnalyzedInWindow": 5, "commentScopesAnalyzedInWindow": 9, "totalAnalysesInWindow": 14},
        "community_health_score": {"currentScore": 55, "components": [{"name": "trust"}]},
        "trending_topics_feed": [{"topic": "Road And Transit", "mentions": 11}],
        "conversation_trends": [{"topic": "Road And Transit"}],
        "topic_lifecycle": [{"topic": "Road And Transit", "stage": "growing"}],
        "sentiment_by_topic": [{"topic": "Road And Transit", "positive": 60}],
        "week_over_week_shifts": [{"label": "Volume", "delta": 7}],
    }


def _old_snapshot() -> dict[str, object]:
    return {
        "communityBrief": {"messagesAnalyzed": 12, "postsAnalyzedInWindow": 4},
        "trendingTopics": [{"topic": "Road And Transit", "mentions": 10}],
        "trendLines": [{"topic": "Road And Transit", "points": [1, 2, 3]}],
    }


def _v2_result() -> DashboardV2AssemblyResult:
    return DashboardV2AssemblyResult(
        snapshot={
            "communityBrief": {"messagesAnalyzed": 14, "postsAnalyzedInWindow": 5, "commentScopesAnalyzedInWindow": 9, "totalAnalysesInWindow": 14},
            "communityHealth": {"currentScore": 55, "components": [{"name": "trust"}]},
            "trendingTopics": [{"topic": "Road And Transit", "mentions": 11}],
            "trendingNewTopics": [],
            "topicBubbles": [],
            "trendLines": [{"topic": "Road And Transit", "points": [2, 3, 4]}],
            "trendData": [],
            "questionCategories": [],
            "questionBriefs": [],
            "qaGap": {},
            "lifecycleStages": [{"topic": "Road And Transit", "stage": "growing"}],
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


def _direct_truth_builders(payloads: dict[str, object] | None = None) -> dict[str, object]:
    payload_map = payloads or _direct_truth_payloads()
    return {widget_id: (lambda ctx, value=value: value) for widget_id, value in payload_map.items()}


class DashboardV2CompareTests(unittest.TestCase):
    def test_compare_uses_source_truth_and_fact_invariant_validation(self) -> None:
        store = _CompareReadyWindowStore()
        ctx = build_dashboard_date_context("2026-04-09", "2026-04-15")

        with patch("api.dashboard_v2_compare.aggregator.get_dashboard_data", return_value=_old_snapshot()), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=_v2_result()), \
             patch("api.dashboard_v2_compare._direct_truth_builders", return_value=_direct_truth_builders()):
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

    def test_compare_returns_structured_failure_when_source_truth_validator_errors(self) -> None:
        store = _CompareReadyWindowStore()
        ctx = build_dashboard_date_context("2026-04-15", "2026-04-15")
        payloads = _direct_truth_payloads()
        builders = _direct_truth_builders(payloads)
        builders["community_brief"] = lambda ctx: (_ for _ in ()).throw(RuntimeError("neo4j overloaded"))

        with patch("api.dashboard_v2_compare.aggregator.get_dashboard_data", return_value=_old_snapshot()), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=_v2_result()), \
             patch("api.dashboard_v2_compare._direct_truth_builders", return_value=builders):
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
        payloads = _direct_truth_payloads()
        old_snapshot = _old_snapshot()

        def _fake_execute(*, label: str, timeout_seconds: float, fn):
            del timeout_seconds, fn
            if label == "old-path":
                return {"status": "ok", "value": old_snapshot}
            if label == "source-truth-community_brief":
                return {"status": "timeout", "error": "source-truth-community_brief timed out after 20.0s"}
            widget_id = label.replace("source-truth-", "").replace("-", "_")
            return {"status": "ok", "value": payloads[widget_id]}

        with patch("api.dashboard_v2_compare._execute_with_timeout", side_effect=_fake_execute), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=_v2_result()):
            result = run_dashboard_v2_compare(store, from_value=ctx.from_date.isoformat(), to_value=ctx.to_date.isoformat())

        validation = result["widgetDiffs"]["community_brief"]["validation"]
        self.assertEqual(validation["blockingReasons"], [BLOCKING_REASON_SOURCE_TRUTH_TIMEOUT])
        self.assertEqual(result["semanticStatus"], "failed")
        self.assertFalse(result["semanticGateReady"])

    def test_compare_keeps_old_path_failure_non_blocking(self) -> None:
        store = _CompareReadyWindowStore()
        ctx = build_dashboard_date_context("2026-04-15", "2026-04-15")
        payloads = _direct_truth_payloads()

        def _fake_execute(*, label: str, timeout_seconds: float, fn):
            del timeout_seconds, fn
            if label == "old-path":
                return {"status": "error", "error": "legacy dashboard unavailable"}
            widget_id = label.replace("source-truth-", "").replace("-", "_")
            return {"status": "ok", "value": payloads[widget_id]}

        with patch("api.dashboard_v2_compare._execute_with_timeout", side_effect=_fake_execute), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=_v2_result()):
            result = run_dashboard_v2_compare(store, from_value=ctx.from_date.isoformat(), to_value=ctx.to_date.isoformat())

        self.assertEqual(result["semanticStatus"], "ready")
        self.assertTrue(result["semanticGateReady"])
        self.assertEqual(result["regressionStatus"], "unavailable")
        self.assertEqual(
            result["widgetDiffs"]["community_brief"]["validation"]["warnings"],
            [WARNING_REASON_OLD_PATH_UNAVAILABLE],
        )
        self.assertEqual(store.created_runs[0]["old_path_meta"]["status"], "unavailable")


if __name__ == "__main__":
    unittest.main()
