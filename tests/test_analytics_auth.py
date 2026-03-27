from __future__ import annotations

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

    def _dashboard_payload(self) -> dict:
        return {
            "data": {"communityHealth": {"score": 50}},
            "meta": {
                "from": "2026-03-15",
                "to": "2026-03-22",
                "requestedFrom": "2026-03-15",
                "requestedTo": "2026-03-22",
                "days": 7,
                "mode": "intelligence",
                "rangeLabel": "2026-03-15..2026-03-22",
                "trustedEndDate": "2026-03-22",
                "degradedTiers": [],
                "suppressedDegradedTiers": [],
                "tierTimes": {},
                "snapshotBuiltAt": "2026-03-22T00:00:00Z",
                "cacheStatus": "test",
                "isStale": False,
                "buildElapsedSeconds": 0.01,
                "buildMode": "test",
                "refreshFailureCount": 0,
                "cacheSource": "memory",
                "freshnessSource": "memory",
                "freshness": {"status": "healthy", "generatedAt": "2026-03-22T00:00:00Z"},
            },
        }

    def test_health_stays_public_when_auth_enabled(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.db, "run_single", return_value={"ok": 1}):
            response = self.client.get("/api/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    def test_dashboard_allows_unauthenticated_access_when_auth_disabled(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_build_dashboard_response_payload", return_value=self._dashboard_payload()):
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
             patch.object(server, "_build_dashboard_response_payload", return_value=self._dashboard_payload()):
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
             patch.object(server, "_build_dashboard_response_payload", return_value=self._dashboard_payload()):
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
             patch.object(server, "_build_dashboard_response_payload", return_value=self._dashboard_payload()):
            first = self.client.get("/api/dashboard", headers={"Authorization": "Bearer frontend-secret"})
            second = self.client.get("/api/dashboard", headers={"Authorization": "Bearer frontend-secret"})

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 429)


if __name__ == "__main__":
    unittest.main()
