from __future__ import annotations

import unittest
from copy import deepcopy
from unittest.mock import patch

from fastapi.testclient import TestClient

from api import server


class _FakeSocialSourceStore:
    def __init__(self) -> None:
        self.items = [
            {
                "id": "account-1",
                "entity_id": "entity-1",
                "company_name": "Unibank",
                "platform": "facebook",
                "source_kind": "meta_ads",
                "display_url": "https://www.facebook.com/unibank",
                "account_external_id": "unibank",
                "is_active": True,
                "health_status": "healthy",
                "last_collected_at": "2026-04-23T10:00:00+00:00",
                "last_error": None,
                "metadata": {"source_url": "https://www.facebook.com/unibank"},
            }
        ]

    def list_source_rows(self) -> list[dict]:
        return [deepcopy(item) for item in self.items]

    def create_or_update_source(self, *, source_type: str, source_key: str | None, source_url: str | None, display_name: str) -> dict:
        for item in self.items:
            same_source = False
            if source_type == "facebook_page":
                same_source = item["source_kind"] == source_type and item["metadata"].get("source_url") == source_url
            elif source_type == "meta_ads":
                same_source = item["source_kind"] == source_type and item["account_external_id"] == source_key
            if item["platform"] == "facebook" and same_source:
                action = "exists"
                if not item["is_active"]:
                    item["is_active"] = True
                    action = "reactivated"
                item["display_url"] = source_url
                item["metadata"]["source_url"] = source_url
                return {"action": action, "item": deepcopy(item)}

        item = {
            "id": "account-2",
            "entity_id": "entity-2",
            "company_name": display_name,
            "platform": "facebook" if source_type in {"facebook_page", "meta_ads"} else "instagram" if source_type == "instagram_profile" else "google",
            "source_kind": source_type,
            "display_url": source_url,
            "account_external_id": source_key if source_type == "meta_ads" else None,
            "is_active": True,
            "health_status": "unknown",
            "last_collected_at": None,
            "last_error": None,
            "metadata": {"source_url": source_url},
        }
        self.items.append(item)
        return {"action": "created", "item": deepcopy(item)}

    def update_source_account(self, account_id: str, *, is_active: bool) -> dict:
        for item in self.items:
            if item["id"] == account_id:
                item["is_active"] = bool(is_active)
                return deepcopy(item)
        raise ValueError("Social source not found")


class _FakeSocialRuntimeControl:
    def __init__(self) -> None:
        self._status = {
            "status": "stopped",
            "is_active": False,
            "interval_minutes": 360,
            "running_now": False,
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_success_at": None,
            "next_run_at": None,
            "last_error": None,
            "last_result": {
                "accounts_total": 1,
                "accounts_processed": 1,
                "activities_collected": 3,
                "activities_analyzed": 2,
                "activities_graph_synced": 1,
                "collect_failures": 0,
                "analysis_failures": 0,
                "graph_failures": 0,
            },
            "run_history": [],
        }

    async def start(self) -> dict:
        self._status["status"] = "active"
        self._status["is_active"] = True
        return deepcopy(self._status)

    async def stop(self) -> dict:
        self._status["status"] = "stopped"
        self._status["is_active"] = False
        return deepcopy(self._status)

    async def set_interval(self, interval_minutes: int) -> dict:
        self._status["interval_minutes"] = interval_minutes
        return deepcopy(self._status)


class SocialSourcesApiTests(unittest.TestCase):
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

    def test_list_social_sources_returns_flat_rows(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.get("/api/sources/social", headers={"Authorization": "Bearer admin-secret"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["platform"], "facebook")
        self.assertEqual(payload["items"][0]["source_kind"], "meta_ads")
        self.assertEqual(payload["items"][0]["company_name"], "Unibank")

    def test_create_social_source_is_idempotent(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            created = self.client.post(
                "/api/sources/social",
                headers={"Authorization": "Bearer admin-secret"},
                json={"source_type": "facebook_page", "value": "https://www.facebook.com/nikol.pashinyan/?ref=bookmarks"},
            )
            existing = self.client.post(
                "/api/sources/social",
                headers={"Authorization": "Bearer admin-secret"},
                json={"source_type": "facebook_page", "value": "facebook.com/nikol.pashinyan"},
            )

        self.assertEqual(created.status_code, 200)
        self.assertEqual(created.json()["action"], "created")
        self.assertEqual(created.json()["item"]["display_url"], "https://www.facebook.com/nikol.pashinyan")
        self.assertEqual(created.json()["item"]["source_kind"], "facebook_page")
        self.assertEqual(existing.status_code, 200)
        self.assertEqual(existing.json()["action"], "exists")
        self.assertEqual(len(fake_store.items), 2)

    def test_create_social_source_rejects_invalid_host(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.post(
                "/api/sources/social",
                headers={"Authorization": "Bearer admin-secret"},
                json={"source_type": "facebook_page", "value": "https://instagram.com/not-facebook"},
            )

        self.assertEqual(response.status_code, 400)

    def test_create_meta_ads_source_accepts_numeric_id(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.post(
                "/api/sources/social",
                headers={"Authorization": "Bearer admin-secret"},
                json={"source_type": "meta_ads", "value": "1378368079150250"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["item"]["source_kind"], "meta_ads")

    def test_update_social_source_toggles_active_state(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.patch(
                "/api/sources/social/account-1",
                headers={"Authorization": "Bearer admin-secret"},
                json={"is_active": False},
            )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["item"]["is_active"])

    def test_social_runtime_controls_are_additive(self) -> None:
        fake_runtime = _FakeSocialRuntimeControl()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_runtime", return_value=fake_runtime):
            started = self.client.post("/api/social/runtime/start", headers={"Authorization": "Bearer admin-secret"})
            updated = self.client.patch(
                "/api/social/runtime",
                headers={"Authorization": "Bearer admin-secret"},
                json={"interval_minutes": 60},
            )
            stopped = self.client.post("/api/social/runtime/stop", headers={"Authorization": "Bearer admin-secret"})

        self.assertEqual(started.status_code, 200)
        self.assertTrue(started.json()["is_active"])
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.json()["interval_minutes"], 60)
        self.assertEqual(stopped.status_code, 200)
        self.assertFalse(stopped.json()["is_active"])


if __name__ == "__main__":
    unittest.main()
