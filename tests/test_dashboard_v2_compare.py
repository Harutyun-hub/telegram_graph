from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
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


def _fact_row(fact_date: str, widget_payloads: dict) -> dict:
    return {
        "fact_date": date.fromisoformat(fact_date),
        "fact_version": 2,
        "materialized_at": datetime(2026, 4, 18, 11, 0, tzinfo=timezone.utc),
        "source_watermark": datetime(2026, 4, 18, 11, 0, tzinfo=timezone.utc),
        "payload_json": {
            "kind": "day_summary",
            "dimensions": {},
            "metrics": {},
            "evidenceRefs": [],
            "sourceRefs": [],
            "factHints": {"widgetPayloads": widget_payloads},
        },
    }


class _CompareReadyWindowStore(_CompareStore):
    def __init__(self) -> None:
        super().__init__()
        self.rows_by_family = {
            "content": [
                _fact_row(
                    "2026-04-15",
                    {
                        "communityBrief": {
                            "messagesAnalyzed": 14,
                            "postsAnalyzedInWindow": 5,
                            "commentScopesAnalyzedInWindow": 9,
                            "totalAnalysesInWindow": 14,
                        }
                    },
                )
            ],
            "topics": [
                _fact_row(
                    "2026-04-15",
                    {
                        "trendingTopics": [{"topic": "Road And Transit", "mentions": 11}],
                        "trendLines": [{"topic": "Road And Transit", "points": [2, 3, 4]}],
                    },
                )
            ],
        }
        self.secondary_rows: dict[tuple[str, str, str], dict] = {}
        self.secondary_upserts: list[dict] = []
        self.secondary_stale_marks: list[dict] = []
        self.artifact_upserts: list[dict] = []

    def summarize_v2_route_readiness(self, *, min_fact_version: int = 1, lookback_days: int = 400, from_date=None, to_date=None):
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

    def latest_dependency_watermarks_for_range(self, *, from_date, to_date, fact_families, secondary_dependencies, min_fact_version: int = 1):
        del from_date, to_date, fact_families, secondary_dependencies, min_fact_version
        return {"content": "2026-04-18T10:00:00+00:00", "topics": "2026-04-18T11:00:00+00:00"}

    def get_range_artifact(self, cache_key: str):
        del cache_key
        return None

    def exact_artifact_has_newer_same_key(self, *, cache_key: str, materialized_at):
        del cache_key, materialized_at
        return False

    def fetch_fact_rows_for_range(self, *, fact_family: str, from_date, to_date, min_fact_version: int = 1):
        del from_date, to_date, min_fact_version
        return list(self.rows_by_family.get(fact_family, []))

    def get_exact_secondary_materialization(self, *, storage_key: str, widget_id: str, window_start, window_end):
        return self.secondary_rows.get((storage_key, widget_id, f"{window_start.isoformat()}:{window_end.isoformat()}"))

    def upsert_secondary_materialization(self, *, storage_key: str, widget_id: str, window_start, window_end, payload_json, meta_json=None, source_watermark=None):
        self.secondary_upserts.append(
            {
                "storage_key": storage_key,
                "widget_id": widget_id,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "payload_json": payload_json,
                "meta_json": meta_json or {},
                "source_watermark": source_watermark,
            }
        )
        self.secondary_rows[(storage_key, widget_id, f"{window_start.isoformat()}:{window_end.isoformat()}")] = {
            "status": "ready",
            "payload_json": payload_json,
            "meta_json": meta_json or {},
            "materialized_at": "2026-04-18T12:00:00+00:00",
            "source_watermark": source_watermark,
        }

    def mark_secondary_materialization_stale(self, *, dependency_name: str, window_start, window_end, new_watermark=None, reason=None):
        self.secondary_stale_marks.append(
            {
                "dependency_name": dependency_name,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "new_watermark": new_watermark,
                "reason": reason,
            }
        )

    def upsert_range_artifact(self, **kwargs) -> None:
        self.artifact_upserts.append(dict(kwargs))


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

    def test_compare_succeeds_for_exact_window_when_global_readiness_is_still_false(self) -> None:
        store = _CompareReadyWindowStore()
        ctx = build_dashboard_date_context("2026-04-09", "2026-04-15")
        old_snapshot = {
            "communityBrief": {"messagesAnalyzed": 12, "postsAnalyzedInWindow": 4},
            "trendingTopics": [{"topic": "Road And Transit", "mentions": 10}],
            "trendLines": [{"topic": "Road And Transit", "points": [1, 2, 3]}],
        }
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
             patch("api.dashboard_v2_compare.build_direct_truth_snapshot", return_value=direct_truth):
            result = run_dashboard_v2_compare(store, from_value=ctx.from_date.isoformat(), to_value=ctx.to_date.isoformat())

        self.assertEqual(result["v2Status"], "ready")
        self.assertTrue(result["widgetDiffs"]["community_brief"]["dashboardV2"]["present"])
        self.assertEqual(store.created_runs[0]["v2_meta"]["status"], "ready")
        self.assertEqual(store.created_runs[0]["v2_meta"]["rangeResolutionPath"], "v2_assembled_exact_from_facts")

    def test_compare_bypasses_stale_exact_last_known_good_artifacts(self) -> None:
        store = _CompareStore()
        ctx = build_dashboard_date_context("2026-04-15", "2026-04-15")
        old_snapshot = {
            "lifecycleStages": [{"topic": "Road And Transit", "stage": "growing"}],
        }
        v2_result = DashboardV2AssemblyResult(
            snapshot={
                "lifecycleStages": [{"topic": "Road And Transit", "stage": "growing"}],
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
            "topic_lifecycle": [{"topic": "Road And Transit", "stage": "growing"}],
            "sentiment_by_topic": [{"topic": "Road And Transit", "positive": 60}],
            "week_over_week_shifts": [{"metric": "volume", "delta": 7}],
        }

        with patch("api.dashboard_v2_compare.aggregator.get_dashboard_data", return_value=old_snapshot), \
             patch("api.dashboard_v2_compare.assemble_dashboard_v2_exact", return_value=v2_result) as assemble_mock, \
             patch("api.dashboard_v2_compare.build_direct_truth_snapshot", return_value=direct_truth):
            result = run_dashboard_v2_compare(store, from_value=ctx.from_date.isoformat(), to_value=ctx.to_date.isoformat())

        self.assertEqual(result["v2Status"], "ready")
        self.assertTrue(result["widgetDiffs"]["topic_lifecycle"]["dashboardV2"]["present"])
        assemble_mock.assert_called_once()
        self.assertEqual(assemble_mock.call_args.kwargs["allow_stale_exact_last_known_good"], False)
        self.assertEqual(assemble_mock.call_args.kwargs["prefer_cached_exact_artifacts"], False)


if __name__ == "__main__":
    unittest.main()
