from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from social.providers.base import SocialProviderError
from social.runtime import SocialRuntimeService


class _FakeStore:
    def __init__(self) -> None:
        self.failure_calls: list[dict] = []
        self.success_calls: list[tuple[str, str | None]] = []
        self.control_command = {
            "request_id": "cmd-1",
            "action": "run_once",
            "status": "pending",
            "requested_at": "2026-04-18T09:00:00+00:00",
        }

    def get_runtime_setting(self, key: str, default: dict) -> dict:
        if key == "scheduler":
            return {"is_active": False, "interval_minutes": 360}
        return dict(default)

    def save_runtime_setting(self, key: str, value: dict) -> dict:
        self.saved_setting = (key, dict(value))
        return dict(value)

    def save_runtime_snapshot(self, payload: dict) -> dict:
        self.snapshot = dict(payload)
        return dict(payload)

    def get_runtime_control_command(self, default=None) -> dict:
        return dict(self.control_command or (default or {}))

    def save_runtime_control_command(self, payload: dict) -> dict:
        self.control_command = dict(payload)
        return dict(payload)

    def create_ingest_run(self, **kwargs) -> dict:
        return {"id": "run-1", **kwargs}

    def finish_ingest_run(self, *args, **kwargs) -> dict:
        return {"args": args, "kwargs": kwargs}

    def mark_source_collect_success(self, source_id: str, *, next_collect_after: str | None) -> None:
        self.success_calls.append((source_id, next_collect_after))

    def mark_source_collect_failure(self, source_id: str, *, health_status: str, error: str) -> None:
        self.failure_marker = {
            "source_id": source_id,
            "health_status": health_status,
            "error": error,
        }

    def record_failure(self, **kwargs) -> dict:
        self.failure_calls.append(dict(kwargs))
        return {"is_dead_letter": False}

    def upsert_activities(self, activities: list[dict]) -> list[dict]:
        self.saved_activities = list(activities)
        return activities

    def clear_failure(self, **kwargs) -> None:
        self.cleared_failure = kwargs

    def list_pending_analysis(self, limit: int = 100) -> list[dict]:
        del limit
        return []

    def list_pending_graph(self, limit: int = 100) -> list[dict]:
        del limit
        return []

    def list_due_sources(self, *, limit: int | None = None, **kwargs) -> list[dict]:
        del limit, kwargs
        return []


class SocialRuntimeTests(unittest.TestCase):
    def _service(self, store: _FakeStore) -> SocialRuntimeService:
        service = SocialRuntimeService(store)
        service.pg_store = SimpleNamespace(enabled=False, cleanup=lambda **kwargs: kwargs)
        return service

    def test_collect_stage_marks_success_and_advances_next_due(self) -> None:
        store = _FakeStore()
        service = self._service(store)
        source = {
            "id": "source-1",
            "entity_id": "entity-1",
            "provider_key": "scrapecreators",
            "platform": "facebook",
            "source_key": "scrapecreators:facebook:page_id:196765077044445",
            "cadence_minutes": 60,
        }
        adapter = SimpleNamespace(
            collect_source=lambda *args, **kwargs: [
                {
                    "entity_id": "entity-1",
                    "account_id": "source-1",
                    "activity_uid": "social:abc",
                    "provider_key": "scrapecreators",
                    "source_key": "scrapecreators:facebook:page_id:196765077044445",
                    "platform": "facebook",
                    "source_kind": "ad",
                    "provider_item_id": "ad-1",
                    "source_url": "https://facebook.com/ad/1",
                    "engagement_metrics": {},
                    "assets": [],
                    "provider_context": {},
                    "provider_payload": {},
                    "normalization_version": "social-v2",
                    "ingest_status": "normalized",
                }
            ]
        )

        with patch("social.runtime.get_provider_adapter", return_value=adapter):
            result = service._run_collect_stage_sync(sources=[source])

        self.assertEqual(result["sources_processed"], 1)
        self.assertEqual(store.success_calls[0][0], "source-1")
        self.assertIsNotNone(store.success_calls[0][1])

    def test_collect_stage_records_source_scoped_failure(self) -> None:
        store = _FakeStore()
        service = self._service(store)
        source = {
            "id": "source-1",
            "entity_id": "entity-1",
            "provider_key": "scrapecreators",
            "platform": "instagram",
            "source_key": "scrapecreators:instagram:handle:unibank_armenia",
        }
        adapter = SimpleNamespace(
            collect_source=lambda *args, **kwargs: (_ for _ in ()).throw(
                SocialProviderError("not found", health_status="provider_404")
            )
        )

        with patch("social.runtime.get_provider_adapter", return_value=adapter):
            result = service._run_collect_stage_sync(sources=[source])

        self.assertEqual(result["collect_failures"], 1)
        self.assertEqual(store.failure_calls[0]["scope_key"], "scrapecreators:instagram:handle:unibank_armenia")
        self.assertEqual(store.failure_calls[0]["account_id"], "source-1")

    def test_control_cycle_processes_pending_run_once_command(self) -> None:
        async def scenario() -> None:
            store = _FakeStore()
            service = self._service(store)
            run_cycle_mock = AsyncMock()
            with patch.object(service, "_run_cycle", run_cycle_mock), \
                 patch.object(service, "status", return_value={"status": "stopped", "running_now": False}):
                await service._run_control_cycle()

            self.assertEqual(store.control_command["status"], "completed")
            self.assertEqual(store.control_command["runtime_status"]["status"], "stopped")
            run_cycle_mock.assert_awaited_once_with(use_runtime_lock=False)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
