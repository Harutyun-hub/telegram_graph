from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from social.runtime import SocialRuntimeService
from social.website_promotions import WebsitePromotionResearchError, WebsitePromotionResearchResult


class _StoreStub:
    def __init__(self) -> None:
        self.settings = {
            "scheduler": {"is_active": False, "interval_minutes": 360},
        }
        self.entities = [
            {
                "id": "entity-1",
                "name": "Example Bank",
                "website": "https://example.am",
                "metadata": {},
                "is_active": True,
            }
        ]
        self.activities: dict[str, dict] = {}
        self.failures: list[dict] = []
        self.website_successes: list[dict] = []
        self.website_failures: list[dict] = []

    def get_runtime_setting(self, key: str, default: dict) -> dict:
        return dict(self.settings.get(key, default))

    def save_runtime_setting(self, key: str, value: dict) -> dict:
        self.settings[key] = dict(value)
        return dict(value)

    def create_ingest_run(self, **kwargs):
        return {"id": "run-1", **kwargs}

    def finish_ingest_run(self, *args, **kwargs):
        return None

    def record_failure(self, **kwargs):
        row = {"is_dead_letter": False, **kwargs}
        self.failures.append(row)
        return row

    def mark_activity_failure(self, **kwargs):
        return None

    def mark_graph_synced(self, **kwargs):
        return None

    def save_analysis(self, **kwargs):
        return kwargs

    def clear_failure(self, **kwargs):
        return None

    def get_failure(self, **kwargs):
        return None

    def prepare_activity_replay(self, *args, **kwargs):
        return []

    def get_account_by_scope_key(self, scope_key: str):
        return {"id": "account-1", "platform": "facebook", "entity_id": "entity-1", "source_key": scope_key}

    def get_entity(self, entity_id: str):
        return next((dict(item) for item in self.entities if item["id"] == entity_id), None)

    def mark_account_collect_success(self, account_id: str):
        return account_id

    def mark_account_collect_failure(self, account_id: str, **kwargs):
        return {"id": account_id, **kwargs}

    def list_active_accounts(self, enabled_platforms):
        return []

    def list_pending_analysis(self, limit: int):
        return []

    def list_pending_graph(self, limit: int):
        return []

    def upsert_activities(self, items):
        for item in items:
            existing = self.activities.get(item["activity_uid"])
            row = {
                **(existing or {}),
                **item,
                "id": (existing or {}).get("id") or f"activity-{len(self.activities) + 1}",
            }
            self.activities[item["activity_uid"]] = row
        return [self.activities[item["activity_uid"]] for item in items]

    def list_entities_for_website_monitor(self, *, limit: int, interval_hours: int):
        return [dict(item) for item in self.entities[:limit]]

    def list_website_promotion_activities(self, entity_id: str):
        return [dict(item) for item in self.activities.values() if item.get("entity_id") == entity_id]

    def mark_missing_website_promotions(self, *, entity_id: str, seen_activity_uids: set[str], checked_at: str, expire_after_misses: int):
        result = {"missing": 0, "expired": 0}
        for uid, item in list(self.activities.items()):
            if item.get("entity_id") != entity_id or uid in seen_activity_uids:
                continue
            payload = dict(item.get("provider_payload") or {})
            monitor = dict(payload.get("website_monitor") or {})
            missed = int(monitor.get("missed_scans") or 0) + 1
            status = "expired" if missed >= expire_after_misses else "missing"
            monitor.update({"status": status, "missed_scans": missed, "last_missing_at": checked_at})
            payload["website_monitor"] = monitor
            item["provider_payload"] = payload
            result[status] += 1
        return result

    def mark_entity_website_scan_success(
        self,
        entity_id: str,
        *,
        checked_at: str,
        promotion_count: int,
        pages_visited_count: int | None = None,
        visited_urls: list[str] | None = None,
        max_pages: int | None = None,
        prompt_version: str | None = None,
    ):
        self.website_successes.append({
            "entity_id": entity_id,
            "checked_at": checked_at,
            "promotion_count": promotion_count,
            "pages_visited_count": pages_visited_count,
            "visited_urls": visited_urls,
            "max_pages": max_pages,
            "prompt_version": prompt_version,
        })

    def mark_entity_website_scan_failure(self, entity_id: str, *, checked_at: str, error: str):
        self.website_failures.append({"entity_id": entity_id, "checked_at": checked_at, "error": error})


class _FakeWebsiteResearcher:
    def __init__(self, results):
        self.results = list(results)

    def research_sync(self, entity):
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _research_result(*, evidence: str = "0% fee until June 30, 2026", promotions=None):
    return WebsitePromotionResearchResult(
        company_name="Example Bank",
        website="https://example.am",
        checked_at="2026-05-05T10:00:00+00:00",
        promotions=promotions if promotions is not None else [
            {
                "title": "0% transfer fee",
                "source_url": "https://example.am/promotions/cards",
                "evidence_text": evidence,
                "valid_from": None,
                "valid_until": "2026-06-30",
                "conditions": "For new card customers only",
                "detected_offer_type": "fee",
                "confidence": 0.92,
            }
        ],
        prompt_version="website-promotion-v2",
        max_pages=8,
        visited_urls=["https://example.am", "https://example.am/promotions/cards"],
        raw_text="{}",
        raw_payload={"visited_urls": ["https://example.am", "https://example.am/promotions/cards"], "promotions": promotions or []},
    )


class SocialRuntimeTests(unittest.TestCase):
    def test_set_interval_persists_scheduler_state(self) -> None:
        async def scenario() -> None:
            service = SocialRuntimeService(_StoreStub())
            status = await service.set_interval(420)
            self.assertEqual(status["interval_minutes"], 420)
            self.assertEqual(service.store.get_runtime_setting("scheduler", {})["interval_minutes"], 420)

        asyncio.run(scenario())

    def test_run_once_schedules_background_cycle_when_idle(self) -> None:
        async def scenario() -> None:
            service = SocialRuntimeService(_StoreStub())
            run_cycle_mock = AsyncMock()

            with patch.object(service, "_run_cycle", run_cycle_mock):
                status = await service.run_once()
                await asyncio.sleep(0)

            self.assertFalse(status["running_now"])
            run_cycle_mock.assert_awaited_once_with()

        asyncio.run(scenario())

    def test_startup_schedules_website_cron_for_social_worker(self) -> None:
        async def scenario() -> None:
            service = SocialRuntimeService(_StoreStub())
            with patch("social.runtime._is_social_worker_owner", return_value=True), \
                patch("social.runtime._ensure_non_issue_topics_hidden", return_value=0), \
                patch("social.runtime.config.SOCIAL_WEBSITE_MONITOR_CRON_ENABLED", True), \
                patch("social.runtime.config.SOCIAL_WEBSITE_MONITOR_ENABLED", True), \
                patch("social.runtime.config.SOCIAL_WEBSITE_MONITOR_CRON_TIMEZONE", "Asia/Yerevan"), \
                patch("social.runtime.config.SOCIAL_WEBSITE_MONITOR_CRON_HOUR", 2), \
                patch("social.runtime.config.SOCIAL_WEBSITE_MONITOR_CRON_MINUTE", 0):
                await service.startup()
                status = service.status()
                self.assertTrue(status["website_cron_enabled"])
                self.assertEqual(status["website_cron_timezone"], "Asia/Yerevan")
                self.assertEqual(status["website_cron_hour"], 2)
                self.assertEqual(status["website_cron_minute"], 0)
                self.assertIsNotNone(status["website_next_run_at"])
                await service.shutdown()

        asyncio.run(scenario())

    def test_website_research_stage_creates_promotion_activity(self) -> None:
        async def scenario() -> None:
            store = _StoreStub()
            service = SocialRuntimeService(store)
            service._website_researcher = _FakeWebsiteResearcher([_research_result()])

            result = service._run_website_research_stage_sync()

            self.assertEqual(result["promotions_collected"], 1)
            saved = next(iter(store.activities.values()))
            self.assertEqual(saved["platform"], "website")
            self.assertEqual(saved["source_kind"], "ad")
            self.assertEqual(saved["content_format"], "website_promotion")
            self.assertEqual(saved["provider_payload"]["website_monitor"]["status"], "new")
            self.assertEqual(saved["provider_payload"]["website_monitor"]["prompt_version"], "website-promotion-v2")
            self.assertEqual(saved["provider_payload"]["website_monitor"]["max_pages"], 8)
            self.assertEqual(saved["provider_payload"]["website_monitor"]["pages_visited_count"], 2)
            self.assertEqual(saved["provider_payload"]["website_monitor"]["visited_urls"], ["https://example.am", "https://example.am/promotions/cards"])
            self.assertEqual(store.website_successes[0]["promotion_count"], 1)
            self.assertEqual(store.website_successes[0]["pages_visited_count"], 2)
            self.assertEqual(result["activity_uids_requiring_analysis"], [saved["activity_uid"]])

        asyncio.run(scenario())

    def test_website_research_stage_marks_same_promotion_ongoing(self) -> None:
        async def scenario() -> None:
            store = _StoreStub()
            service = SocialRuntimeService(store)
            service._website_researcher = _FakeWebsiteResearcher([_research_result(), _research_result()])

            service._run_website_research_stage_sync()
            service._run_website_research_stage_sync()

            self.assertEqual(len(store.activities), 1)
            saved = next(iter(store.activities.values()))
            self.assertEqual(saved["provider_payload"]["website_monitor"]["status"], "ongoing")

        asyncio.run(scenario())

    def test_website_research_stage_marks_changed_promotion_updated(self) -> None:
        async def scenario() -> None:
            store = _StoreStub()
            service = SocialRuntimeService(store)
            service._website_researcher = _FakeWebsiteResearcher([
                _research_result(evidence="0% fee until June 30, 2026"),
                _research_result(evidence="0% fee and 5% cashback until June 30, 2026"),
            ])

            service._run_website_research_stage_sync()
            service._run_website_research_stage_sync()

            self.assertEqual(len(store.activities), 1)
            saved = next(iter(store.activities.values()))
            self.assertEqual(saved["provider_payload"]["website_monitor"]["status"], "updated")
            self.assertIn("5% cashback", saved["text_content"])

        asyncio.run(scenario())

    def test_website_research_stage_expires_missing_promotion_after_grace(self) -> None:
        async def scenario() -> None:
            store = _StoreStub()
            service = SocialRuntimeService(store)
            service._website_researcher = _FakeWebsiteResearcher([
                _research_result(),
                _research_result(promotions=[]),
                _research_result(promotions=[]),
            ])

            with patch("social.runtime.config.SOCIAL_WEBSITE_PROMOTION_EXPIRE_AFTER_MISSES", 2):
                service._run_website_research_stage_sync()
                service._run_website_research_stage_sync()
                service._run_website_research_stage_sync()

            saved = next(iter(store.activities.values()))
            self.assertEqual(saved["provider_payload"]["website_monitor"]["status"], "expired")
            self.assertEqual(saved["provider_payload"]["website_monitor"]["missed_scans"], 2)

        asyncio.run(scenario())

    def test_website_research_stage_records_openclaw_failure(self) -> None:
        async def scenario() -> None:
            store = _StoreStub()
            service = SocialRuntimeService(store)
            service._website_researcher = _FakeWebsiteResearcher([WebsitePromotionResearchError("OpenClaw unavailable")])

            result = service._run_website_research_stage_sync()

            self.assertEqual(result["website_research_failures"], 1)
            self.assertEqual(store.failures[0]["stage"], "website_research")
            self.assertEqual(store.failures[0]["scope_key"], "website:entity-1")
            self.assertIn("OpenClaw unavailable", store.website_failures[0]["error"])

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
