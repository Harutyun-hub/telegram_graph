from __future__ import annotations

import unittest
from unittest.mock import patch

from api.dashboard_dates import build_dashboard_date_context
from api.dashboard_v2_assembler import DashboardV2AssemblyResult
from api.dashboard_v2_compare import run_dashboard_v2_compare
from api.dashboard_v2_registry import ALL_WIDGET_IDS


class _CompareStore:
    def __init__(self) -> None:
        self.created_runs: list[dict] = []

    def create_compare_run(self, **kwargs) -> str:
        self.created_runs.append(dict(kwargs))
        return "compare-1"


class DashboardV2CompareTests(unittest.TestCase):
    def test_compare_uses_old_path_v2_and_direct_truth(self) -> None:
        store = _CompareStore()
        ctx = build_dashboard_date_context("2026-04-18", "2026-04-18")
        old_snapshot = {
            "communityBrief": {"messagesAnalyzed": 12, "postsAnalyzedInWindow": 4},
            "trendingTopics": [{"topic": "Road And Transit", "mentions": 10}],
            "trendLines": [{"topic": "Road And Transit", "points": [1, 2, 3]}],
        }
        v2_result = DashboardV2AssemblyResult(
            snapshot={
                "communityBrief": {"messagesAnalyzed": 14, "postsAnalyzedInWindow": 5},
                "trendingTopics": [{"topic": "Road And Transit", "mentions": 11}],
                "trendLines": [{"topic": "Road And Transit", "points": [2, 3, 4]}],
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
        direct_truth = {
            "community_brief": {"messagesAnalyzed": 14, "postsAnalyzedInWindow": 5},
            "community_health_score": {"currentScore": 55},
            "trending_topics_feed": [{"topic": "Road And Transit", "mentions": 11}],
            "conversation_trends": [{"topic": "Road And Transit"}],
            "topic_lifecycle": [{"stage": "emerging", "topics": [{"topic": "Road And Transit"}]}],
            "sentiment_by_topic": [{"topic": "Road And Transit", "positive": 60}],
            "week_over_week_shifts": [{"metric": "volume", "delta": 7}],
        }

        with patch("api.dashboard_v2_compare.aggregator.get_dashboard_data", return_value=old_snapshot), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=v2_result), \
             patch("api.dashboard_v2_compare.build_direct_truth_snapshot", return_value=direct_truth):
            result = run_dashboard_v2_compare(store, from_value=ctx.from_date.isoformat(), to_value=ctx.to_date.isoformat())

        self.assertEqual(result["compareId"], "compare-1")
        self.assertEqual(result["v2Status"], "ready")
        self.assertEqual(len(result["widgetDiffs"]), len(ALL_WIDGET_IDS))
        self.assertTrue(result["widgetDiffs"]["community_brief"]["dashboardV2"]["present"])
        self.assertTrue(result["widgetDiffs"]["community_brief"]["directTruth"]["present"])
        self.assertEqual(store.created_runs[0]["v2_meta"]["rangeResolutionPath"], "v2_assembled_exact_from_facts")


if __name__ == "__main__":
    unittest.main()
