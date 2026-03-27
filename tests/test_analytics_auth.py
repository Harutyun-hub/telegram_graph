from __future__ import annotations

from datetime import date
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api import server


class AnalyticsAuthTests(unittest.TestCase):
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

    def _dashboard_context(self) -> SimpleNamespace:
        return SimpleNamespace(
            from_date=date(2026, 3, 15),
            to_date=date(2026, 3, 22),
            days=7,
            is_operational=False,
            range_label="Last 7 Days",
            cache_key="2026-03-15:2026-03-22",
        )

    def _dashboard_snapshot(self) -> tuple[dict, dict]:
        return (
            {"communityHealth": {"score": 50}},
            {
                "cacheStatus": "test",
                "degradedTiers": [],
                "suppressedDegradedTiers": [],
                "tierTimes": {},
                "snapshotBuiltAt": "2026-03-22T00:00:00Z",
                "isStale": False,
                "buildElapsedSeconds": 0.01,
                "buildMode": "test",
                "refreshFailureCount": 0,
            },
        )

    def test_health_stays_public_when_auth_enabled(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.db, "run_single", return_value={"ok": 1}):
            response = self.client.get("/api/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    def test_dashboard_allows_unauthenticated_access_when_auth_disabled(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_dashboard_freshness_snapshot", return_value={"health": {"status": "ok"}, "generated_at": "2026-03-22T00:00:00Z"}), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=date(2026, 3, 22)), \
             patch.object(server, "_default_dashboard_context", return_value=self._dashboard_context()), \
             patch.object(server, "get_dashboard_snapshot", return_value=self._dashboard_snapshot()):
            response = self.client.get("/api/dashboard")

        self.assertEqual(response.status_code, 200)
        self.assertIn("data", response.json())

    def test_dashboard_requires_token_when_auth_enabled(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False):
            response = self.client.get("/api/dashboard")

        self.assertEqual(response.status_code, 401)
        self.assertIn("Missing Authorization", response.text)

    def test_dashboard_accepts_frontend_token(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_dashboard_freshness_snapshot", return_value={"health": {"status": "ok"}, "generated_at": "2026-03-22T00:00:00Z"}), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=date(2026, 3, 22)), \
             patch.object(server, "_default_dashboard_context", return_value=self._dashboard_context()), \
             patch.object(server, "get_dashboard_snapshot", return_value=self._dashboard_snapshot()):
            response = self.client.get(
                "/api/dashboard",
                headers={"Authorization": "Bearer frontend-secret"},
            )

        self.assertEqual(response.status_code, 200)

    def test_dashboard_accepts_openclaw_token(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_dashboard_freshness_snapshot", return_value={"health": {"status": "ok"}, "generated_at": "2026-03-22T00:00:00Z"}), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=date(2026, 3, 22)), \
             patch.object(server, "_default_dashboard_context", return_value=self._dashboard_context()), \
             patch.object(server, "get_dashboard_snapshot", return_value=self._dashboard_snapshot()):
            response = self.client.get(
                "/api/dashboard",
                headers={"Authorization": "Bearer openclaw-secret"},
            )

        self.assertEqual(response.status_code, 200)

    def test_freshness_is_protected(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False):
            response = self.client.get("/api/freshness")

        self.assertEqual(response.status_code, 401)

    def test_invalid_token_is_logged_without_raw_value(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.logger, "warning") as warning_mock:
            response = self.client.get(
                "/api/dashboard",
                headers={"Authorization": "Bearer bad-secret-token"},
            )

        self.assertEqual(response.status_code, 401)
        logged_message = warning_mock.call_args[0][0]
        self.assertIn("reason=invalid_token", logged_message)
        self.assertNotIn("bad-secret-token", logged_message)

    def test_rate_limit_returns_429(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", True), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_WINDOW_SECONDS", 60), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_MAX_REQUESTS", 1), \
             patch.object(server, "_dashboard_freshness_snapshot", return_value={"health": {"status": "ok"}, "generated_at": "2026-03-22T00:00:00Z"}), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=date(2026, 3, 22)), \
             patch.object(server, "_default_dashboard_context", return_value=self._dashboard_context()), \
             patch.object(server, "get_dashboard_snapshot", return_value=self._dashboard_snapshot()):
            first = self.client.get("/api/dashboard", headers={"Authorization": "Bearer frontend-secret"})
            second = self.client.get("/api/dashboard", headers={"Authorization": "Bearer frontend-secret"})

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)


if __name__ == "__main__":
    unittest.main()
