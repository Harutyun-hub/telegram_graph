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
                "company_id": "company-1",
                "company_name": "Unibank",
                "company_website": "https://www.unibank.am",
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
        self.companies: dict[str, dict] = {
            "company-1": {
                "id": "company-1",
                "name": "Unibank",
                "website": "https://www.unibank.am",
                "entity_id": "entity-1",
            }
        }

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

    def create_or_update_company_sources(
        self,
        *,
        company_name: str,
        website: str | None,
        website_domain: str | None,
        sources: list[dict],
        company_id: str | None = None,
    ) -> dict:
        company = self.companies.get(company_id or "") if company_id else None
        if company_id and not company:
            raise ValueError("Company not found")
        if company is None:
            company = next(
                (
                    row
                    for row in self.companies.values()
                    if row["name"] == company_name or (website and row.get("website") == website)
                ),
                None,
            )
        action = "updated" if company else "created"
        if company is None:
            next_index = len(self.companies) + 1
            company = {
                "id": f"company-{next_index}",
                "name": company_name,
                "website": website,
                "entity_id": f"entity-{next_index}",
            }
            self.companies[company["id"]] = company
        else:
            company["name"] = company_name
            if website:
                company["website"] = website

        def _platform(source_type: str) -> str:
            return "facebook" if source_type in {"facebook_page", "meta_ads"} else "instagram" if source_type == "instagram_profile" else "google"

        for source in sources:
            source_type = source["source_type"]
            existing = next(
                (
                    item
                    for item in self.items
                    if item["entity_id"] == company["entity_id"] and item["source_kind"] == source_type
                ),
                None,
            )
            item = existing or {
                "id": f"account-{len(self.items) + 1}",
                "entity_id": company["entity_id"],
                "company_id": company["id"],
                "platform": _platform(source_type),
                "source_kind": source_type,
                "is_active": True,
                "health_status": "unknown",
                "last_collected_at": None,
                "last_error": None,
                "metadata": {},
            }
            item.update(
                {
                    "company_name": company["name"],
                    "company_website": company.get("website"),
                    "display_url": source.get("source_url") or (f"https://{source.get('source_key')}" if source_type == "google_domain" else None),
                    "account_external_id": source.get("source_key") if source_type == "meta_ads" else None,
                    "metadata": {
                        "source_url": source.get("source_url"),
                        "source_key": source.get("source_key"),
                    },
                }
            )
            if existing is None:
                self.items.append(item)

        items = [deepcopy(item) for item in self.items if item["entity_id"] == company["entity_id"]]
        return {
            "action": action,
            "company": {"id": company["id"], "name": company["name"], "website": company.get("website")},
            "entity": {"id": company["entity_id"], "company_id": company["id"], "name": company["name"]},
            "items": items,
        }

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


class _FakeTelegramSourceWriter:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.created_rows: list[dict] = []

    def get_channel_by_handle(self, handle: str):
        normalized = str(handle).strip().lower().lstrip("@")
        for row in self.rows.values():
            if str(row.get("channel_username") or "").strip().lower().lstrip("@") == normalized:
                return deepcopy(row)
        return None

    def create_channel(self, payload: dict) -> dict:
        row = {"id": "chan-1", **payload}
        self.rows[row["id"]] = deepcopy(row)
        self.created_rows.append(deepcopy(row))
        return deepcopy(row)

    def update_channel(self, channel_uuid: str, payload: dict) -> dict:
        row = deepcopy(self.rows.get(channel_uuid, {"id": channel_uuid}))
        row.update(payload)
        self.rows[channel_uuid] = deepcopy(row)
        return deepcopy(row)

    def get_channel_by_id(self, channel_uuid: str) -> dict | None:
        row = self.rows.get(channel_uuid)
        return deepcopy(row) if row else None


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

    def test_create_facebook_page_source_accepts_numeric_page_id(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.post(
                "/api/sources/social",
                headers={"Authorization": "Bearer admin-secret"},
                json={"source_type": "facebook_page", "value": "100063669491743"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["item"]["source_kind"], "facebook_page")

    def test_create_social_company_sources_groups_rows_under_one_entity(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.post(
                "/api/sources/social/company",
                headers={"Authorization": "Bearer admin-secret"},
                json={
                    "company_name": "XTB",
                    "website": "https://www.xtb.com",
                    "sources": {
                        "facebook_page": "https://www.facebook.com/xtb",
                        "instagram_profile": "https://www.instagram.com/xtb_de",
                        "meta_ads": "138239466852",
                        "google_domain": "xtb.com",
                    },
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "created")
        self.assertEqual(payload["company"]["name"], "XTB")
        self.assertEqual(len(payload["items"]), 4)
        entity_ids = {item["entity_id"] for item in payload["items"]}
        self.assertEqual(entity_ids, {payload["entity"]["id"]})
        self.assertEqual({item["source_kind"] for item in payload["items"]}, {"facebook_page", "instagram_profile", "meta_ads", "google_domain"})

    def test_create_social_company_sources_requires_at_least_one_source(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.post(
                "/api/sources/social/company",
                headers={"Authorization": "Bearer admin-secret"},
                json={"company_name": "XTB", "website": "https://www.xtb.com", "sources": {}},
            )

        self.assertEqual(response.status_code, 400)

    def test_update_social_company_sources_adds_missing_source_to_existing_company(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.patch(
                "/api/sources/social/company/company-1",
                headers={"Authorization": "Bearer admin-secret"},
                json={
                    "company_name": "Unibank",
                    "website": "https://www.unibank.am",
                    "sources": {"instagram_profile": "@unibank"},
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["action"], "updated")
        self.assertEqual(payload["entity"]["id"], "entity-1")
        self.assertTrue(any(item["source_kind"] == "instagram_profile" for item in payload["items"]))
        self.assertEqual(len(fake_store.companies), 1)

    def test_agent_social_source_accepts_openclaw_token(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.post(
                "/api/agent/sources/social",
                headers={"Authorization": "Bearer openclaw-secret"},
                json={"source_type": "facebook_page", "value": "facebook.com/nikol.pashinyan"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["action"], "created")

    def test_agent_social_source_rejects_frontend_token(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.post(
                "/api/agent/sources/social",
                headers={"Authorization": "Bearer frontend-secret"},
                json={"source_type": "facebook_page", "value": "facebook.com/nikol.pashinyan"},
            )

        self.assertEqual(response.status_code, 403)

    def test_agent_social_source_accepts_admin_api_key(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.post(
                "/api/agent/sources/social",
                headers={"Authorization": "Bearer admin-secret"},
                json={"source_type": "instagram_profile", "value": "@unibank"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["item"]["source_kind"], "instagram_profile")

    def test_agent_telegram_source_accepts_openclaw_token(self) -> None:
        fake_writer = _FakeTelegramSourceWriter()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.config, "FEATURE_SOURCE_RESOLUTION_QUEUE", True), \
             patch.object(server, "get_supabase_writer", return_value=fake_writer), \
             patch.object(server, "ensure_resolution_job", return_value={"id": "job-1"}):
            response = self.client.post(
                "/api/agent/sources/telegram",
                headers={"Authorization": "Bearer openclaw-secret"},
                json={"channel_username": "@docschat", "channel_title": "Docs Chat"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["action"], "created")
        self.assertEqual(response.json()["item"]["channel_username"], "@docschat")

    def test_agent_telegram_source_rejects_frontend_token(self) -> None:
        fake_writer = _FakeTelegramSourceWriter()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.config, "FEATURE_SOURCE_RESOLUTION_QUEUE", True), \
             patch.object(server, "get_supabase_writer", return_value=fake_writer), \
             patch.object(server, "ensure_resolution_job", return_value={"id": "job-1"}):
            response = self.client.post(
                "/api/agent/sources/telegram",
                headers={"Authorization": "Bearer frontend-secret"},
                json={"channel_username": "@docschat", "channel_title": "Docs Chat"},
            )

        self.assertEqual(response.status_code, 403)

    def test_agent_telegram_source_accepts_admin_api_key(self) -> None:
        fake_writer = _FakeTelegramSourceWriter()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.config, "FEATURE_SOURCE_RESOLUTION_QUEUE", True), \
             patch.object(server, "get_supabase_writer", return_value=fake_writer), \
             patch.object(server, "ensure_resolution_job", return_value={"id": "job-1"}):
            response = self.client.post(
                "/api/agent/sources/telegram",
                headers={"Authorization": "Bearer admin-secret"},
                json={"channel_username": "@docschat", "channel_title": "Docs Chat"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["action"], "created")
        self.assertEqual(response.json()["item"]["channel_title"], "Docs Chat")

    def test_agent_telegram_source_reactivates_existing_inactive_channel(self) -> None:
        fake_writer = _FakeTelegramSourceWriter()
        fake_writer.rows["chan-1"] = {
            "id": "chan-1",
            "channel_username": "@docschat",
            "channel_title": "Docs Chat",
            "is_active": False,
            "resolution_status": "resolved",
        }
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server.config, "FEATURE_SOURCE_RESOLUTION_QUEUE", True), \
             patch.object(server, "get_supabase_writer", return_value=fake_writer), \
             patch.object(server, "ensure_resolution_job", return_value={"id": "job-1"}):
            response = self.client.post(
                "/api/agent/sources/telegram",
                headers={"Authorization": "Bearer openclaw-secret"},
                json={"channel_username": "@docschat", "channel_title": "Docs Chat"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["action"], "reactivated")
        self.assertTrue(response.json()["item"]["is_active"])

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

    def test_create_instagram_source_accepts_handle_url_and_at_handle(self) -> None:
        cases = [
            "unibank",
            "@unibank",
            "https://www.instagram.com/unibank/",
        ]
        for value in cases:
            with self.subTest(value=value):
                fake_store = _FakeSocialSourceStore()
                with patch.object(server.config, "IS_LOCKED_ENV", True), \
                     patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
                     patch.object(server, "get_social_store", return_value=fake_store):
                    response = self.client.post(
                        "/api/sources/social",
                        headers={"Authorization": "Bearer admin-secret"},
                        json={"source_type": "instagram_profile", "value": value},
                    )

                self.assertEqual(response.status_code, 200)
                item = response.json()["item"]
                self.assertEqual(item["platform"], "instagram")
                self.assertEqual(item["source_kind"], "instagram_profile")
                self.assertEqual(item["display_url"], "https://www.instagram.com/unibank")

    def test_create_instagram_source_rejects_invalid_host(self) -> None:
        fake_store = _FakeSocialSourceStore()
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
             patch.object(server, "get_social_store", return_value=fake_store):
            response = self.client.post(
                "/api/sources/social",
                headers={"Authorization": "Bearer admin-secret"},
                json={"source_type": "instagram_profile", "value": "https://facebook.com/unibank"},
            )

        self.assertEqual(response.status_code, 400)

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
