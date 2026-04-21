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


class _FakeStore:
    def __init__(self) -> None:
        self.collections = [
            {
                "name": "test",
                "description": "",
                "chunk_count": 52,
                "doc_count": 1,
            }
        ]

    def list_collections(self) -> list[dict]:
        return list(self.collections)


class KBApiTests(unittest.TestCase):
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

    def test_kb_collections_accepts_analytics_token(self) -> None:
        fake_store = _FakeStore()
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_kb_components", return_value=(fake_store, object())):
            response = self.client.get(
                "/api/kb/collections",
                headers={"Authorization": "Bearer openclaw-secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["collections"][0]["name"], "test")

    def test_kb_collections_accepts_operator_supabase_session(self) -> None:
        fake_store = _FakeStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server.config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"), \
             patch.object(server.config, "AI_HELPER_ADMIN_EMAIL", ""), \
             patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "get_supabase_writer", return_value=_FakeWriter("admin-user")), \
             patch.object(server, "_kb_components", return_value=(fake_store, object())):
            response = self.client.get(
                "/api/kb/collections",
                headers={
                    "Authorization": "Bearer analytics-proxy-token",
                    "X-Supabase-Authorization": "Bearer user-session-token",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["collections"][0]["chunk_count"], 52)

    def test_kb_collections_accepts_admin_api_key_fallback(self) -> None:
        fake_store = _FakeStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_kb_components", return_value=(fake_store, object())):
            response = self.client.get(
                "/api/kb/collections",
                headers={"Authorization": "Bearer admin-secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["collections"]), 1)

    def test_kb_collections_accepts_simple_auth_credentials(self) -> None:
        fake_store = _FakeStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", ""), \
             patch.object(server.config, "SIMPLE_AUTH_USERNAME", "Admin"), \
             patch.object(server.config, "SIMPLE_AUTH_PASSWORD", "secret-pass"), \
             patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_kb_components", return_value=(fake_store, object())):
            response = self.client.get(
                "/api/kb/collections",
                headers={"X-Admin-Authorization": "Basic QWRtaW46c2VjcmV0LXBhc3M="},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["collections"][0]["name"], "test")

    def test_kb_collections_returns_503_when_dependencies_are_missing(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.config, "GEMINI_API_KEY", "gemini-key"), \
             patch.object(
                 server,
                 "make_kb_components",
                 side_effect=ImportError("Missing dependency 'chromadb'.  pip install chromadb"),
             ):
            response = self.client.get("/api/kb/collections")

        self.assertEqual(response.status_code, 503)
        self.assertIn("chromadb", response.json()["detail"])


if __name__ == "__main__":
    unittest.main()
