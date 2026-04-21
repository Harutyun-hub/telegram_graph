from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api import dashboard_perf
from api import server
from api.queries import comparative


class DashboardPerfSummaryTests(unittest.TestCase):
    def test_summarize_dashboard_profile_groups_queries(self) -> None:
        with dashboard_perf.capture_dashboard_profile("test.dashboard") as profile:
            dashboard_perf.record_neo4j_query(
                label="neo4j.query",
                elapsed_ms=84.2,
                row_count=12,
                metadata={"driver": "request"},
            )
            dashboard_perf.record_supabase_query(
                label="supabase.query",
                elapsed_ms=31.4,
                row_count=4,
            )

        summary = dashboard_perf.summarize_dashboard_profile(profile, top_n=5)
        self.assertIsNotNone(summary)
        self.assertEqual(summary["label"], "test.dashboard")
        self.assertEqual(summary["neo4j"]["queryCount"], 1)
        self.assertEqual(summary["supabase"]["queryCount"], 1)
        self.assertEqual(summary["neo4j"]["slowest"][0]["label"], "neo4j.query")
        self.assertEqual(summary["supabase"]["slowest"][0]["rowCount"], 4)


class DashboardWarmEndpointTests(unittest.TestCase):
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

    def test_warm_endpoint_requires_paired_dates(self) -> None:
        with patch.object(server.config, "ADMIN_API_KEY", "admin-secret"):
            response = self.client.post(
                "/api/admin/dashboard/warm",
                headers={"Authorization": "Bearer admin-secret"},
                json={"from_date": "2026-04-01"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("provided together", response.text)

    def test_warm_endpoint_returns_profile_payload(self) -> None:
        expected = {
            "success": True,
            "status": "completed",
            "cacheKey": "2026-04-01:2026-04-30",
            "requestedFrom": "2026-04-01",
            "requestedTo": "2026-04-30",
            "trustedEndDate": "2026-04-30",
            "meta": {"cacheStatus": "memory_fresh"},
            "profile": {"neo4j": {"queryCount": 1}, "supabase": {"queryCount": 2}},
        }
        with patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "_warm_dashboard_range", return_value=expected) as warm_mock:
            response = self.client.post(
                "/api/admin/dashboard/warm",
                headers={"Authorization": "Bearer admin-secret"},
                json={
                    "from_date": "2026-04-01",
                    "to_date": "2026-04-30",
                    "profile": True,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)
        warm_mock.assert_called_once_with(
            "2026-04-01",
            "2026-04-30",
            wait=True,
            force_refresh=False,
            include_profile=True,
        )


class ComparativeVitalityIndicatorTests(unittest.TestCase):
    def test_vitality_indicators_use_single_query_payload(self) -> None:
        with patch.object(
            comparative,
            "run_single",
            return_value={
                "totalUsers": 120,
                "activeUsers7d": 48,
                "totalTopics": 35,
                "totalPosts": 200,
                "totalComments": 500,
            },
        ) as run_single_mock:
            payload = comparative.get_vitality_indicators()

        self.assertEqual(payload["totalUsers"], 120)
        self.assertEqual(payload["activeUsers7d"], 48)
        self.assertEqual(payload["avgCommentsPerPost"], 2.5)
        run_single_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
