from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from api import server


class _FakeWriter:
    def __init__(self, user_id: str, email: str = "admin@example.com") -> None:
        self.client = SimpleNamespace(
            auth=SimpleNamespace(
                get_user=lambda _token: SimpleNamespace(
                    user=SimpleNamespace(id=user_id, email=email),
                )
            )
        )


class OperatorAuthTests(unittest.TestCase):
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

    def test_admin_config_requires_operator_auth(self) -> None:
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "_admin_config_response", return_value={"widgets": {}}):
            response = self.client.get("/api/admin/config")

        self.assertEqual(response.status_code, 401)

    def test_admin_config_accepts_admin_api_key(self) -> None:
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "_admin_config_response", return_value={"widgets": {}}):
            response = self.client.get(
                "/api/admin/config",
                headers={"Authorization": "Bearer admin-secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["widgets"], {})

    def test_operator_route_accepts_supabase_admin_session_even_with_proxy_auth_header(self) -> None:
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server.config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"), \
             patch.object(server.config, "AI_HELPER_ADMIN_EMAIL", ""), \
             patch.object(server, "get_supabase_writer", return_value=_FakeWriter("admin-user")), \
             patch.object(server, "_admin_config_response", return_value={"widgets": {"w1": {"enabled": True}}}):
            response = self.client.get(
                "/api/admin/config",
                headers={
                    "Authorization": "Bearer analytics-proxy-token",
                    "X-Supabase-Authorization": "Bearer user-session-token",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("widgets", response.json())

    def test_debug_refresh_returns_404_when_disabled(self) -> None:
        with patch.object(server.config, "ENABLE_DEBUG_ENDPOINTS", False), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"):
            response = self.client.post(
                "/api/question-briefs/debug/refresh",
                headers={"Authorization": "Bearer admin-secret"},
            )

        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    unittest.main()
