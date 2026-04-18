from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

os.environ.setdefault("TELEGRAM_API_ID", "1")
os.environ.setdefault("TELEGRAM_API_HASH", "hash")
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "service-role")
os.environ.setdefault("NEO4J_URI", "neo4j+s://example.databases.neo4j.io")
os.environ.setdefault("NEO4J_PASSWORD", "password")
os.environ.setdefault("OPENAI_API_KEY", "sk-test")

from api import server
from api.dashboard_dates import build_dashboard_date_context
from api.dashboard_v2_assembler import DashboardV2FactsNotReadyError


class _RouteStore:
    def summarize_v2_route_readiness(
        self,
        *,
        min_fact_version: int = 1,
        lookback_days: int = 400,
        from_date=None,
        to_date=None,
    ):
        del min_fact_version, lookback_days
        return {
            "coverageStart": "2025-03-15",
            "coverageEnd": "2026-04-18",
            "routeReadyWindowStart": from_date.isoformat() if from_date else "2025-03-15",
            "routeReadyWindowEnd": to_date.isoformat() if to_date else "2026-04-18",
            "requestedFrom": from_date.isoformat() if from_date else None,
            "requestedTo": to_date.isoformat() if to_date else None,
            "v2RouteReady": True,
            "missingFamilies": [],
            "missingDates": [],
            "degradedFamilies": [],
            "degradedDates": [],
        }

    def get_range_readiness(self, *, from_date, to_date, fact_families, min_fact_version: int = 1):
        del from_date, to_date, fact_families, min_fact_version
        return {
            "availabilityStart": "2025-03-15",
            "availabilityEnd": "2026-04-18",
            "missingFactFamilies": [],
            "missingDates": [],
            "ready": True,
        }

    def latest_dependency_watermarks_for_range(self, *, from_date, to_date, fact_families, secondary_dependencies, min_fact_version: int = 1):
        del from_date, to_date, fact_families, secondary_dependencies, min_fact_version
        return {"content": "2026-04-18T11:00:00+00:00", "topics": "2026-04-18T11:00:00+00:00"}

    def get_range_artifact(self, cache_key):
        del cache_key
        return None

    def exact_artifact_has_newer_same_key(self, *, cache_key, materialized_at):
        del cache_key, materialized_at
        return False

    def fetch_fact_rows_for_range(self, *, fact_family, from_date, to_date, min_fact_version: int = 1):
        del from_date, to_date, min_fact_version
        if fact_family == "content":
            return [
                {
                    "fact_date": build_dashboard_date_context("2026-04-18", "2026-04-18").from_date,
                    "fact_version": 2,
                    "materialized_at": "2026-04-18T11:00:00+00:00",
                    "source_watermark": "2026-04-18T11:00:00+00:00",
                    "payload_json": {
                        "kind": "day_summary",
                        "dimensions": {},
                        "metrics": {},
                        "evidenceRefs": [],
                        "sourceRefs": [],
                        "factHints": {
                            "widgetPayloads": {
                                "communityBrief": {
                                    "messagesAnalyzed": 9,
                                    "postsAnalyzedInWindow": 4,
                                    "commentScopesAnalyzedInWindow": 5,
                                    "totalAnalysesInWindow": 9,
                                }
                            }
                        },
                    },
                }
            ]
        if fact_family == "topics":
            return [
                {
                    "fact_date": build_dashboard_date_context("2026-04-18", "2026-04-18").from_date,
                    "fact_version": 2,
                    "materialized_at": "2026-04-18T11:00:00+00:00",
                    "source_watermark": "2026-04-18T11:00:00+00:00",
                    "payload_json": {
                        "kind": "topic_day",
                        "dimensions": {"topicKey": "road_and_transit"},
                        "metrics": {},
                        "evidenceRefs": [],
                        "sourceRefs": [],
                        "factHints": {
                            "widgetPayloads": {
                                "trendingTopics": [{"topic": "Road And Transit", "mentions": 9, "category": "Transport"}]
                            }
                        },
                    },
                }
            ]
        return []

    def get_exact_secondary_materialization(self, *, storage_key, widget_id, window_start, window_end):
        del storage_key, widget_id, window_start, window_end
        return None

    def upsert_secondary_materialization(self, **kwargs):
        return None

    def mark_secondary_materialization_stale(self, **kwargs):
        return None

    def upsert_range_artifact(self, **kwargs):
        return None


class _StatusRouteStore:
    def summarize_v2_route_readiness(self, *, min_fact_version: int = 1, lookback_days: int = 400, from_date=None, to_date=None):
        del min_fact_version, lookback_days
        return {
            "coverageStart": "2026-04-12",
            "coverageEnd": "2026-04-18",
            "routeReadyWindowStart": from_date.isoformat() if from_date else "2026-04-12",
            "routeReadyWindowEnd": to_date.isoformat() if to_date else "2026-04-18",
            "requestedFrom": from_date.isoformat() if from_date else None,
            "requestedTo": to_date.isoformat() if to_date else None,
            "v2RouteReady": True,
            "missingFamilies": [],
            "degradedFamilies": [],
        }

    def status_snapshot(self, *, run_limit: int = 20, artifact_limit: int = 20, job_limit: int = 10, min_fact_version: int = 1, lookback_days: int = 400, from_date=None, to_date=None):
        del run_limit, artifact_limit, job_limit, min_fact_version, lookback_days
        return {
            "factRuns": [],
            "artifacts": [],
            "compareRuns": [],
            "materializeJobs": [],
            "activeJob": {
                "jobId": "job-1",
                "mode": "backfill",
                "status": "running",
                "totalSlices": 8,
                "completedSlices": 3,
                "failedSlices": 0,
                "currentSlice": {"sliceId": "slice-4", "factFamily": "topics"},
                "lastHeartbeatAt": "2026-04-18T12:00:00+00:00",
            },
            "readiness": self.summarize_v2_route_readiness(from_date=from_date, to_date=to_date),
        }

    def get_active_materialize_job(self):
        return {"jobId": "job-1", "mode": "backfill", "status": "running"}

    def get_materialize_job(self, job_id, *, include_slices=False):
        return {
            "jobId": job_id,
            "mode": "backfill",
            "status": "running",
            "requestedStart": "2026-04-12",
            "requestedEnd": "2026-04-18",
            "totalSlices": 8,
            "completedSlices": 3,
            "failedSlices": 0,
            "currentSlice": {"sliceId": "slice-4", "factFamily": "topics"},
            "slices": [{"sliceId": "slice-1"}] if include_slices else [],
        }


class DashboardV2ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._startup_handlers = list(server.app.router.on_startup)
        cls._shutdown_handlers = list(server.app.router.on_shutdown)
        server.app.router.on_startup = []
        server.app.router.on_shutdown = []
        cls.client = TestClient(server.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        server.app.router.on_startup = cls._startup_handlers
        server.app.router.on_shutdown = cls._shutdown_handlers

    def setUp(self) -> None:
        server._analytics_rate_limit_buckets.clear()

    def test_dashboard_v2_returns_404_when_api_disabled(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.config, "DASH_V2_API_ENABLED", False):
            response = self.client.get("/api/dashboard-v2?from=2026-04-18&to=2026-04-18")

        self.assertEqual(response.status_code, 404)

    def test_dashboard_v2_requires_explicit_exact_range(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.config, "DASH_V2_API_ENABLED", True):
            response = self.client.get("/api/dashboard-v2")

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["detail"]["code"], "v2_exact_range_required")

    def test_dashboard_v2_returns_truthful_422_for_ranges_over_365_days(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.config, "DASH_V2_API_ENABLED", True):
            response = self.client.get("/api/dashboard-v2?from=2025-01-01&to=2026-04-18")

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["detail"]["code"], "v2_summary_mode_deferred")

    def test_dashboard_v2_returns_structured_503_when_facts_are_not_ready(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.config, "DASH_V2_API_ENABLED", True), \
             patch.object(
                 server,
                 "_build_dashboard_v2_response_payload",
                 side_effect=DashboardV2FactsNotReadyError(
                     {
                         "code": "v2_facts_not_ready",
                         "requestedFrom": "2026-04-18",
                         "requestedTo": "2026-04-18",
                         "missingFactFamilies": ["topics"],
                         "missingDates": ["2026-04-18"],
                         "degradedFactFamilies": ["content"],
                         "degradedDates": ["2026-04-18"],
                     }
                 ),
             ):
            response = self.client.get("/api/dashboard-v2?from=2026-04-18&to=2026-04-18")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"]["code"], "v2_facts_not_ready")
        self.assertEqual(response.json()["detail"]["degradedFactFamilies"], ["content"])

    def test_dashboard_v2_serves_fact_only_response_without_legacy_queries(self) -> None:
        ctx = build_dashboard_date_context("2026-04-18", "2026-04-18")
        request_resolution = {
            "ctx": ctx,
            "requestedFrom": "2026-04-18",
            "requestedTo": "2026-04-18",
            "trustedEndDate": "2026-04-18",
            "freshnessSnapshot": {"health": {"status": "healthy"}, "generated_at": "2026-04-18T11:00:00+00:00"},
            "freshnessSource": "dashboard_v2_test",
            "persistedReadStatus": None,
            "persistedReadMs": None,
        }

        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.config, "DASH_V2_API_ENABLED", True), \
             patch.object(server, "_resolve_dashboard_request_context", return_value=request_resolution), \
             patch.object(server, "get_dashboard_v2_store", return_value=_RouteStore()), \
             patch("api.aggregator.get_dashboard_data", side_effect=AssertionError("legacy dashboard path not allowed")), \
             patch("api.queries.pulse.get_community_brief", side_effect=AssertionError("legacy pulse not allowed")), \
             patch("api.queries.strategic.get_trend_lines", side_effect=AssertionError("legacy strategic not allowed")), \
             patch("api.queries.comparative.get_weekly_shifts", side_effect=AssertionError("legacy comparative not allowed")):
            response = self.client.get("/api/dashboard-v2?from=2026-04-18&to=2026-04-18")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["meta"]["from"], "2026-04-18")
        self.assertEqual(payload["meta"]["to"], "2026-04-18")
        self.assertEqual(payload["meta"]["rangeMode"], "exact")
        self.assertEqual(payload["meta"]["dataPlane"], "dashboard_v2")
        self.assertEqual(payload["meta"]["skippedTiers"], [])
        self.assertEqual(payload["data"]["communityBrief"]["postsAnalyzedInWindow"], 4)

    def test_dashboard_v2_materialize_enqueues_job_without_running_inline(self) -> None:
        with patch.object(server.config, "ADMIN_API_KEY", ""), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "get_dashboard_v2_store", return_value=object()), \
             patch(
                 "api.server.enqueue_dashboard_v2_materialize_job",
                 return_value={
                     "jobId": "job-1",
                     "status": "queued",
                     "requestedStart": "2026-04-12",
                     "requestedEnd": "2026-04-18",
                     "totalSlices": 8,
                     "completedSlices": 0,
                     "failedSlices": 0,
                 },
             ):
            response = self.client.post("/api/dashboard-v2/materialize?mode=backfill&from=2026-04-12&to=2026-04-18")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["result"]["jobId"], "job-1")
        self.assertEqual(response.json()["result"]["status"], "queued")

    def test_dashboard_v2_status_accepts_exact_window_and_returns_active_job(self) -> None:
        with patch.object(server.config, "ADMIN_API_KEY", ""), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "get_dashboard_v2_store", return_value=_StatusRouteStore()):
            response = self.client.get("/api/dashboard-v2/status?from=2026-04-12&to=2026-04-18")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"]["activeJob"]["jobId"], "job-1")
        self.assertEqual(payload["status"]["readiness"]["requestedFrom"], "2026-04-12")
        self.assertEqual(payload["status"]["readiness"]["requestedTo"], "2026-04-18")

    def test_dashboard_v2_materialize_job_route_returns_job_detail(self) -> None:
        with patch.object(server.config, "ADMIN_API_KEY", ""), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "get_dashboard_v2_store", return_value=_StatusRouteStore()):
            response = self.client.get("/api/dashboard-v2/materialize/jobs/job-1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["result"]["jobId"], "job-1")
        self.assertEqual(response.json()["result"]["currentSlice"]["factFamily"], "topics")


if __name__ == "__main__":
    unittest.main()
