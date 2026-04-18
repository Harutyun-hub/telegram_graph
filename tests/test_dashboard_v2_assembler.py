from __future__ import annotations

import socket
import unittest
import urllib.request
from datetime import date, datetime, timezone
from unittest.mock import patch

from api.dashboard_dates import build_dashboard_date_context
from api.dashboard_v2_assembler import (
    DashboardV2FactsNotReadyError,
    _MEMORY_EXACT_CACHE,
    assemble_dashboard_v2_exact,
)
from api.dashboard_v2_registry import FACT_FAMILIES
from api.dashboard_v2_secondary import build_request_time_secondary_snapshot


def _utc_iso(hour: int) -> str:
    return datetime(2026, 4, 18, hour, 0, tzinfo=timezone.utc).isoformat()


class _AssemblerStore:
    def __init__(self) -> None:
        self.route_ready = True
        self.exact_route_ready = True
        self.range_ready = True
        self.range_missing_dates: list[str] = []
        self.range_missing_families: list[str] = []
        self.range_artifact: dict | None = None
        self.newer_exact_exists = False
        self.rows_by_family = {family: [] for family in FACT_FAMILIES}
        self.secondary_rows: dict[tuple[str, str, str], dict] = {}
        self.secondary_upserts: list[dict] = []
        self.secondary_stale_marks: list[dict] = []
        self.artifact_upserts: list[dict] = []
        self.route_readiness_calls: list[dict[str, str | None]] = []

    def summarize_v2_route_readiness(
        self,
        *,
        min_fact_version: int = 1,
        lookback_days: int = 400,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict:
        del min_fact_version, lookback_days
        self.route_readiness_calls.append(
            {
                "from_date": from_date.isoformat() if from_date else None,
                "to_date": to_date.isoformat() if to_date else None,
            }
        )
        route_ready = self.exact_route_ready if from_date and to_date else self.route_ready
        return {
            "coverageStart": "2025-03-15",
            "coverageEnd": "2026-04-18",
            "routeReadyWindowStart": from_date.isoformat() if from_date and route_ready else "2025-03-15" if route_ready else None,
            "routeReadyWindowEnd": to_date.isoformat() if to_date and route_ready else "2026-04-18" if route_ready else None,
            "requestedFrom": from_date.isoformat() if from_date else None,
            "requestedTo": to_date.isoformat() if to_date else None,
            "v2RouteReady": route_ready,
            "missingFamilies": [] if route_ready else ["content"],
        }

    def get_range_readiness(self, *, from_date: date, to_date: date, fact_families, min_fact_version: int = 1) -> dict:
        del from_date, to_date, fact_families, min_fact_version
        return {
            "availabilityStart": "2025-03-15",
            "availabilityEnd": "2026-04-18",
            "missingFactFamilies": list(self.range_missing_families),
            "missingDates": list(self.range_missing_dates),
            "ready": self.range_ready,
        }

    def latest_dependency_watermarks_for_range(self, *, from_date: date, to_date: date, fact_families, secondary_dependencies, min_fact_version: int = 1) -> dict:
        del from_date, to_date, fact_families, secondary_dependencies, min_fact_version
        return {
            "content": _utc_iso(10),
            "topics": _utc_iso(11),
            "secondary:question_cloud": _utc_iso(11),
        }

    def get_range_artifact(self, cache_key: str) -> dict | None:
        del cache_key
        return self.range_artifact

    def exact_artifact_has_newer_same_key(self, *, cache_key: str, materialized_at) -> bool:
        del cache_key, materialized_at
        return self.newer_exact_exists

    def fetch_fact_rows_for_range(self, *, fact_family: str, from_date: date, to_date: date, min_fact_version: int = 1):
        del from_date, to_date, min_fact_version
        return list(self.rows_by_family.get(fact_family, []))

    def get_exact_secondary_materialization(self, *, storage_key: str, widget_id: str, window_start: date, window_end: date):
        return self.secondary_rows.get((storage_key, widget_id, f"{window_start.isoformat()}:{window_end.isoformat()}"))

    def upsert_secondary_materialization(self, *, storage_key: str, widget_id: str, window_start: date, window_end: date, payload_json, meta_json=None, source_watermark=None) -> None:
        record = {
            "storage_key": storage_key,
            "widget_id": widget_id,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "payload_json": payload_json,
            "meta_json": meta_json or {},
            "source_watermark": source_watermark.isoformat() if isinstance(source_watermark, datetime) else source_watermark,
        }
        self.secondary_upserts.append(record)
        self.secondary_rows[(storage_key, widget_id, f"{window_start.isoformat()}:{window_end.isoformat()}")] = {
            "status": "ready",
            "payload_json": payload_json,
            "meta_json": meta_json or {},
            "materialized_at": _utc_iso(12),
            "source_watermark": source_watermark,
        }

    def mark_secondary_materialization_stale(self, *, dependency_name: str, window_start: date, window_end: date, new_watermark=None, reason=None) -> None:
        self.secondary_stale_marks.append(
            {
                "dependency_name": dependency_name,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "new_watermark": new_watermark.isoformat() if isinstance(new_watermark, datetime) else new_watermark,
                "reason": reason,
            }
        )

    def upsert_range_artifact(self, **kwargs) -> None:
        self.artifact_upserts.append(dict(kwargs))


def _make_fact_row(fact_date: str, widget_payloads: dict) -> dict:
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


class DashboardV2AssemblerTests(unittest.TestCase):
    def setUp(self) -> None:
        _MEMORY_EXACT_CACHE.clear()

    def test_assembler_rejects_range_when_v2_facts_are_not_ready(self) -> None:
        store = _AssemblerStore()
        store.exact_route_ready = False
        store.range_ready = False
        store.range_missing_families = ["topics", "users"]
        store.range_missing_dates = ["2026-04-16", "2026-04-17"]

        with self.assertRaises(DashboardV2FactsNotReadyError) as ctx:
            assemble_dashboard_v2_exact(store, ctx=build_dashboard_date_context("2026-04-16", "2026-04-18"))

        self.assertEqual(ctx.exception.detail["code"], "v2_facts_not_ready")
        self.assertEqual(ctx.exception.detail["missingFactFamilies"], ["topics", "users"])
        self.assertEqual(ctx.exception.detail["missingDates"], ["2026-04-16", "2026-04-17"])

    def test_assembler_uses_exact_window_readiness_when_global_window_is_not_ready(self) -> None:
        store = _AssemblerStore()
        store.route_ready = False
        store.exact_route_ready = True
        store.rows_by_family["content"] = [
            _make_fact_row(
                "2026-04-15",
                {
                    "communityBrief": {
                        "messagesAnalyzed": 12,
                        "postsAnalyzedInWindow": 4,
                        "commentScopesAnalyzedInWindow": 8,
                        "totalAnalysesInWindow": 12,
                    }
                },
            )
        ]

        result = assemble_dashboard_v2_exact(store, ctx=build_dashboard_date_context("2026-04-09", "2026-04-15"))

        self.assertEqual(result.range_resolution_path, "v2_assembled_exact_from_facts")
        self.assertEqual(
            store.route_readiness_calls[-1],
            {"from_date": "2026-04-09", "to_date": "2026-04-15"},
        )

    def test_assembler_builds_exact_snapshot_from_facts_and_persists_artifact(self) -> None:
        store = _AssemblerStore()
        store.rows_by_family["content"] = [
            _make_fact_row(
                "2026-04-18",
                {
                    "communityBrief": {
                        "messagesAnalyzed": 24,
                        "postsAnalyzedInWindow": 10,
                        "commentScopesAnalyzedInWindow": 14,
                        "totalAnalysesInWindow": 24,
                        "positiveIntentPct24h": 55,
                        "negativeIntentPct24h": 20,
                    }
                },
            )
        ]
        store.rows_by_family["topics"] = [
            _make_fact_row(
                "2026-04-18",
                {
                    "trendingTopics": [
                        {"topic": "Road And Transit", "mentions": 12, "category": "Transport"},
                        {"topic": "Visa And Residency", "mentions": 8, "category": "Services"},
                    ]
                },
            )
        ]

        result = assemble_dashboard_v2_exact(store, ctx=build_dashboard_date_context("2026-04-18", "2026-04-18"))

        self.assertEqual(result.range_resolution_path, "v2_assembled_exact_from_facts")
        self.assertEqual(result.cache_status, "assembled_exact_from_facts")
        self.assertEqual(result.snapshot["communityBrief"]["postsAnalyzedInWindow"], 10)
        self.assertGreater(len(result.snapshot["questionCategories"]), 0)
        self.assertTrue(store.artifact_upserts)
        self.assertTrue(store.secondary_upserts)
        self.assertEqual(store.secondary_upserts[0]["meta_json"]["llmUsed"], False)
        self.assertEqual(store.secondary_upserts[0]["meta_json"]["networkUsed"], False)

    def test_same_key_last_known_good_requires_exact_range_match(self) -> None:
        store = _AssemblerStore()
        store.range_artifact = {
            "cache_key": "2026-04-18:2026-04-18",
            "from_date": date(2026, 4, 17),
            "to_date": date(2026, 4, 18),
            "range_mode": "exact",
            "payload_json": {"communityBrief": {"messagesAnalyzed": 12}},
            "dependency_watermarks": {"content": _utc_iso(9)},
            "artifact_version": 1,
            "materialized_at": _utc_iso(9),
            "fact_watermark": _utc_iso(9),
            "is_stale": True,
            "stale_fact_families": ["content"],
        }
        store.rows_by_family["content"] = [
            _make_fact_row("2026-04-18", {"communityBrief": {"messagesAnalyzed": 1, "postsAnalyzedInWindow": 1}})
        ]

        result = assemble_dashboard_v2_exact(store, ctx=build_dashboard_date_context("2026-04-18", "2026-04-18"))

        self.assertEqual(result.range_resolution_path, "v2_assembled_exact_from_facts")

    def test_request_time_secondary_builder_stays_non_networked(self) -> None:
        snapshot = {
            "trendingTopics": [{"topic": "Road And Transit", "mentions": 3, "category": "Transport"}],
            "topicBubbles": [],
        }

        with patch.object(socket, "create_connection", side_effect=AssertionError("network not allowed")), \
             patch.object(urllib.request, "urlopen", side_effect=AssertionError("network not allowed")):
            payload = build_request_time_secondary_snapshot("question_cloud", snapshot)

        self.assertIn("questionCategories", payload)

    def test_request_path_does_not_call_legacy_query_modules(self) -> None:
        store = _AssemblerStore()
        store.rows_by_family["content"] = [
            _make_fact_row("2026-04-18", {"communityBrief": {"messagesAnalyzed": 5, "postsAnalyzedInWindow": 2}})
        ]

        with patch("api.aggregator.get_dashboard_data", side_effect=AssertionError("legacy path not allowed")), \
             patch("api.queries.pulse.get_community_brief", side_effect=AssertionError("legacy pulse not allowed")), \
             patch("api.queries.strategic.get_trend_lines", side_effect=AssertionError("legacy strategic not allowed")), \
             patch("api.queries.comparative.get_weekly_shifts", side_effect=AssertionError("legacy comparative not allowed")):
            result = assemble_dashboard_v2_exact(store, ctx=build_dashboard_date_context("2026-04-18", "2026-04-18"))

        self.assertEqual(result.cache_source, "assembled")


if __name__ == "__main__":
    unittest.main()
