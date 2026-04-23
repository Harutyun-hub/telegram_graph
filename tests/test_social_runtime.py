from __future__ import annotations

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, Mock, patch

from social import runtime
from social.runtime import SocialRuntimeService


class _StoreStub:
    def __init__(self) -> None:
        self.settings = {
            "scheduler": {"is_active": False, "interval_minutes": 360},
        }

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
        return {"is_dead_letter": False, **kwargs}

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
        return items


class SocialRuntimeTests(unittest.TestCase):
    def _make_service(self) -> SocialRuntimeService:
        loop = asyncio.new_event_loop()
        self.addCleanup(loop.close)
        old_loop = None
        try:
            old_loop = asyncio.get_event_loop()
        except RuntimeError:
            old_loop = None
        asyncio.set_event_loop(loop)
        if old_loop is None:
            self.addCleanup(asyncio.set_event_loop, None)
        else:
            self.addCleanup(asyncio.set_event_loop, old_loop)
        return SocialRuntimeService(_StoreStub())

    def test_set_interval_persists_scheduler_state(self) -> None:
        async def scenario() -> None:
            service = self._make_service()
            status = await service.set_interval(420)
            self.assertEqual(status["interval_minutes"], 420)
            self.assertEqual(service.store.get_runtime_setting("scheduler", {})["interval_minutes"], 420)

        asyncio.run(scenario())

    def test_control_cycle_processes_pending_run_once_command(self) -> None:
        async def scenario() -> None:
            store = _StoreStub()
            store.settings["control_command"] = {
                "request_id": "cmd-1",
                "action": "run_once",
                "status": "pending",
                "requested_at": "2026-04-21T10:00:00+00:00",
            }
            service = SocialRuntimeService(store)

            run_cycle_mock = AsyncMock()

            with patch.object(service, "_run_cycle", run_cycle_mock), \
                 patch.object(service, "status", return_value={"status": "active", "running_now": False}):
                await service._run_control_cycle()

            self.assertEqual(store.settings["control_command"]["status"], "completed")
            self.assertEqual(store.settings["control_command"]["runtime_status"]["status"], "active")
            run_cycle_mock.assert_awaited_once_with()

        asyncio.run(scenario())

    def test_cleanup_helper_runs_only_for_social_worker_role(self) -> None:
        writer_factory = Mock()
        writer_factory.return_value.mark_non_issue_topics_proposed.return_value = 4

        with patch.dict(os.environ, {"APP_ROLE": "web"}, clear=False), \
             patch.object(runtime, "_non_issue_cleanup_completed", False):
            updated = runtime._ensure_non_issue_topics_hidden(writer_factory)

        self.assertEqual(updated, 0)
        writer_factory.assert_not_called()

    def test_cleanup_helper_is_idempotent_per_process(self) -> None:
        writer_factory = Mock()
        writer_factory.return_value.mark_non_issue_topics_proposed.return_value = 7

        with patch.dict(os.environ, {"APP_ROLE": "social-worker"}, clear=False), \
             patch.object(runtime, "_non_issue_cleanup_completed", False):
            first = runtime._ensure_non_issue_topics_hidden(writer_factory)
            second = runtime._ensure_non_issue_topics_hidden(writer_factory)

        self.assertEqual(first, 7)
        self.assertEqual(second, 0)
        writer_factory.return_value.mark_non_issue_topics_proposed.assert_called_once_with()

    def test_cleanup_helper_logs_and_continues_on_failure(self) -> None:
        writer_factory = Mock(side_effect=RuntimeError("neo4j offline"))

        with patch.dict(os.environ, {"APP_ROLE": "social-worker"}, clear=False), \
             patch.object(runtime, "_non_issue_cleanup_completed", False), \
             patch.object(runtime.logger, "warning") as warning:
            updated = runtime._ensure_non_issue_topics_hidden(writer_factory)

        self.assertEqual(updated, 0)
        warning.assert_called_once()

    def test_startup_triggers_cleanup_only_in_social_worker_role(self) -> None:
        async def scenario(role: str) -> int:
            service = self._make_service()
            with patch.dict(os.environ, {"APP_ROLE": role}, clear=False), \
                 patch.object(service, "_ensure_scheduler_started"), \
                 patch.object(service, "_upsert_interval_job"), \
                 patch.object(service, "_upsert_control_job"), \
                 patch.object(service, "_persist_runtime_snapshot"), \
                 patch.object(runtime, "_ensure_non_issue_topics_hidden", return_value=3) as cleanup:
                await service.startup()
            return cleanup.call_count

        self.assertEqual(asyncio.run(scenario("social-worker")), 1)
        self.assertEqual(asyncio.run(scenario("web")), 0)

    def test_startup_passes_graph_factory_through_cleanup_helper(self) -> None:
        async def scenario() -> None:
            service = self._make_service()
            with patch.dict(os.environ, {"APP_ROLE": "social-worker"}, clear=False), \
                 patch.object(service, "_ensure_scheduler_started"), \
                 patch.object(service, "_upsert_interval_job"), \
                 patch.object(service, "_upsert_control_job"), \
                 patch.object(service, "_persist_runtime_snapshot"), \
                 patch.object(runtime, "_ensure_non_issue_topics_hidden", return_value=2) as cleanup:
                await service.startup()
            args, _kwargs = cleanup.call_args
            self.assertEqual(len(args), 1)
            self.assertTrue(callable(args[0]))

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
