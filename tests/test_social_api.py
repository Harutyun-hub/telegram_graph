from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api import server


class _FakeSocialStore:
    def __init__(self) -> None:
        self.entities = [
            {
                "id": "entity-1",
                "legacy_company_id": "company-1",
                "name": "Unibank",
                "industry": "Finance",
                "website": "https://unibank.am",
                "is_active": True,
                "accounts": [
                    {
                        "platform": "facebook",
                        "account_external_id": "196765077044445",
                        "account_handle": None,
                        "domain": None,
                        "is_active": True,
                    }
                ],
                "platform_accounts": {
                    "facebook": {
                        "platform": "facebook",
                        "account_external_id": "196765077044445",
                        "account_handle": None,
                        "domain": None,
                        "is_active": True,
                    },
                    "instagram": None,
                    "google": None,
                    "tiktok": None,
                },
            }
        ]
        self.updated_payload: dict | None = None

    def get_overview(self) -> dict:
        return {
            "entities_total": 1,
            "entities_active": 1,
            "activities_total": 2,
            "platform_counts": {"facebook": 2},
            "analysis_status_counts": {"analyzed": 2},
            "account_health_counts": {"healthy": 1},
            "queue_depth": {"analysis": 0, "graph": 0},
            "dead_letter_failures": 0,
            "stale_entities": [],
        }

    def list_activities(self, *, limit: int = 100, entity_id: str | None = None, platform: str | None = None) -> list[dict]:
        items = [
            {
                "id": "activity-1",
                "activity_uid": "facebook:ad:123",
                "platform": "facebook",
                "source_kind": "ad",
                "source_url": "https://facebook.com/ad/123",
                "text_content": "Zero monthly fees.",
                "published_at": "2026-03-28T09:00:00+00:00",
                "author_handle": "unibank",
                "cta_type": "Learn More",
                "content_format": "Image",
                "region_name": "Armenia",
                "ingest_status": "normalized",
                "analysis_status": "analyzed",
                "graph_status": "synced",
                "entity": {"id": "entity-1", "name": "Unibank"},
                "analysis": {
                    "summary": "Promotes a fee-free card offer.",
                    "analysis_payload": {
                        "summary": "Promotes a fee-free card offer.",
                        "marketing_intent": "Acquire new cardholders",
                        "topics": ["Credit Cards"],
                    },
                },
            }
        ]
        filtered = items
        if entity_id:
            filtered = [row for row in filtered if row["entity"]["id"] == entity_id]
        if platform:
            filtered = [row for row in filtered if row["platform"] == platform]
        return filtered[:limit]

    def list_entities(self) -> list[dict]:
        return list(self.entities)

    def ensure_entity_from_company(self, legacy_company_id: str) -> dict:
        if legacy_company_id != "company-1":
            raise ValueError("Company not found in master registry")
        return dict(self.entities[0])

    def get_entity(self, entity_id: str) -> dict | None:
        return next((dict(item) for item in self.entities if item["id"] == entity_id), None)

    def update_entity(self, entity_id: str, *, is_active=None, metadata=None, accounts=None) -> dict:
        self.updated_payload = {
            "entity_id": entity_id,
            "is_active": is_active,
            "metadata": metadata,
            "accounts": accounts or [],
        }
        entity = self.get_entity(entity_id)
        if entity is None:
            raise ValueError("Social entity not found")
        if is_active is not None:
            entity["is_active"] = is_active
        return entity

    def list_failures(self, *, dead_letter_only: bool = False, stage: str | None = None, limit: int = 100) -> list[dict]:
        items = [
            {
                "id": "failure-1",
                "stage": "analysis",
                "scope_key": "facebook:ad:123",
                "last_error": "invalid json",
                "is_dead_letter": True,
                "last_failed_at": "2026-03-29T00:00:00+00:00",
            }
        ]
        if dead_letter_only:
            items = [item for item in items if item["is_dead_letter"]]
        if stage:
            items = [item for item in items if item["stage"] == stage]
        return items[:limit]


class _FakeSocialRuntime:
    async def run_once(self) -> dict:
        return {
            "status": "stopped",
            "running_now": False,
            "last_result": {"activities_collected": 4},
        }

    async def retry_failure(self, *, stage: str, scope_key: str) -> dict:
        return {
            "stage": stage,
            "retry": {"scope_key": scope_key, "activities_collected": 1},
        }

    async def replay_activities(self, *, stage: str, activity_uids: list[str]) -> dict:
        return {
            "stage": stage,
            "replay": {"activity_uids": activity_uids, "activities_analyzed": len(activity_uids)},
        }

    def status(self) -> dict:
        return {
            "status": "stopped",
            "is_active": False,
            "interval_minutes": 360,
            "running_now": False,
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_success_at": "2026-03-29T00:00:00+00:00",
            "next_run_at": None,
            "last_error": None,
            "last_result": {"activities_collected": 4},
            "run_history": [],
            "runtime_enabled": True,
            "tiktok_enabled": False,
            "postgres_worker_enabled": True,
            "worker_id": "worker-1",
        }


class SocialApiTests(unittest.TestCase):
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

    def test_social_entities_requires_operator_auth(self) -> None:
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"):
            response = self.client.get("/api/social/entities")

        self.assertEqual(response.status_code, 401)

    def test_social_overview_returns_store_and_runtime_data(self) -> None:
        fake_store = _FakeSocialStore()
        fake_runtime = _FakeSocialRuntime()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store), \
             patch.object(server, "get_current_social_runtime_status", return_value=fake_runtime.status()):
            response = self.client.get(
                "/api/social/overview",
                headers={"Authorization": "Bearer admin-secret"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["entities_total"], 1)
        self.assertEqual(payload["runtime"]["last_result"]["activities_collected"], 4)

    def test_social_create_entity_syncs_from_company_registry(self) -> None:
        fake_store = _FakeSocialStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.post(
                "/api/social/entities",
                headers={"Authorization": "Bearer admin-secret"},
                json={"legacy_company_id": "company-1"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["item"]["legacy_company_id"], "company-1")

    def test_social_entity_patch_updates_accounts(self) -> None:
        fake_store = _FakeSocialStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.patch(
                "/api/social/entities/entity-1",
                headers={"Authorization": "Bearer admin-secret"},
                json={
                    "is_active": False,
                    "accounts": [
                        {
                            "platform": "instagram",
                            "account_handle": "unibankojsc",
                            "is_active": True,
                        }
                    ],
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(fake_store.updated_payload)
        self.assertEqual(fake_store.updated_payload["is_active"], False)
        self.assertEqual(fake_store.updated_payload["accounts"][0]["platform"], "instagram")

    def test_social_runtime_run_once_uses_social_runtime_service(self) -> None:
        fake_runtime = _FakeSocialRuntime()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_runtime", return_value=fake_runtime):
            response = self.client.post(
                "/api/social/runtime/run-once",
                headers={"Authorization": "Bearer admin-secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["last_result"]["activities_collected"], 4)

    def test_social_runtime_retry_uses_runtime_service(self) -> None:
        fake_runtime = _FakeSocialRuntime()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_runtime", return_value=fake_runtime):
            response = self.client.post(
                "/api/social/runtime/retry",
                headers={"Authorization": "Bearer admin-secret"},
                json={"stage": "analysis", "scope_key": "facebook:ad:123"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["retry"]["scope_key"], "facebook:ad:123")

    def test_social_runtime_replay_uses_runtime_service(self) -> None:
        fake_runtime = _FakeSocialRuntime()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_runtime", return_value=fake_runtime):
            response = self.client.post(
                "/api/social/runtime/replay",
                headers={"Authorization": "Bearer admin-secret"},
                json={"stage": "graph", "activity_uids": ["facebook:ad:123"]},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["stage"], "graph")
        self.assertEqual(response.json()["replay"]["activities_analyzed"], 1)


if __name__ == "__main__":
    unittest.main()
