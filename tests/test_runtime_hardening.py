from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import config
from api import server
from api import worker
from api.scraper_scheduler import ScraperSchedulerService


class _CoordinatorStub:
    def __init__(self, *, ping_result: bool = True) -> None:
        self._ping_result = ping_result
        self._locks: dict[str, str] = {}

    def ping(self) -> bool:
        return self._ping_result

    def acquire_lock(self, name: str, ttl_seconds: int) -> str | None:
        del ttl_seconds
        if name in self._locks:
            return None
        token = f"{name}:{len(self._locks) + 1}"
        self._locks[name] = token
        return token

    def release_lock(self, name: str, token: str | None) -> None:
        if token and self._locks.get(name) == token:
            self._locks.pop(name, None)


class RuntimeStartupHardeningTests(unittest.TestCase):
    def test_staging_forces_web_only_defaults(self) -> None:
        with patch.object(server.config, "IS_STAGING", True), \
             patch.object(server.config, "STAGING_ENABLE_BACKGROUND_JOBS", False):
            role, warmers = server._apply_testing_release_invariants("all", True)

        self.assertEqual(role, "web")
        self.assertFalse(warmers)

    def test_staging_allows_dedicated_worker_when_enabled(self) -> None:
        with patch.object(server.config, "IS_STAGING", True), \
             patch.object(server.config, "STAGING_ENABLE_BACKGROUND_JOBS", True):
            role, warmers = server._apply_testing_release_invariants("worker", True)

        self.assertEqual(role, "worker")
        self.assertFalse(warmers)

    def test_config_validation_role_allows_staging_worker_when_enabled(self) -> None:
        with patch.object(config, "IS_STAGING", True), \
             patch.object(config, "STAGING_ENABLE_BACKGROUND_JOBS", True):
            role = config._normalize_app_role_for_validation("worker")

        self.assertEqual(role, "worker")

    def test_web_role_skips_background_scheduler_startup(self) -> None:
        async def enter_lifespan() -> None:
            async with server.app_lifespan(server.app):
                return None

        fake_scheduler = SimpleNamespace(startup=AsyncMock(), shutdown=AsyncMock())

        with patch.object(server, "APP_ROLE", "web"), \
             patch.object(server.config, "IS_LOCKED_ENV", False), \
             patch.object(server.config, "REDIS_URL", ""), \
             patch.object(server, "RUN_STARTUP_WARMERS", False), \
             patch.object(server, "get_runtime_coordinator", return_value=_CoordinatorStub(ping_result=True)), \
             patch.object(server, "get_scraper_scheduler", return_value=fake_scheduler) as scheduler_factory, \
             patch.object(server, "_start_question_cards_scheduler") as question_scheduler, \
             patch.object(server, "_start_behavioral_cards_scheduler") as behavioral_scheduler, \
             patch.object(server, "_start_opportunity_cards_scheduler") as opportunity_scheduler, \
             patch.object(server, "_start_topic_overviews_scheduler") as topic_scheduler, \
             patch.object(server.db, "close"):
            asyncio.run(enter_lifespan())

        scheduler_factory.assert_not_called()
        fake_scheduler.startup.assert_not_awaited()
        question_scheduler.assert_not_called()
        behavioral_scheduler.assert_not_called()
        opportunity_scheduler.assert_not_called()
        topic_scheduler.assert_not_called()

    def test_worker_role_starts_background_scheduler_stack(self) -> None:
        async def enter_lifespan() -> None:
            async with server.app_lifespan(server.app):
                return None

        fake_scheduler = SimpleNamespace(startup=AsyncMock(), shutdown=AsyncMock())

        with patch.object(server, "APP_ROLE", "worker"), \
             patch.object(server.config, "IS_LOCKED_ENV", False), \
             patch.object(server.config, "REDIS_URL", ""), \
             patch.object(server, "RUN_STARTUP_WARMERS", False), \
             patch.object(server, "get_runtime_coordinator", return_value=_CoordinatorStub(ping_result=True)), \
             patch.object(server, "get_scraper_scheduler", return_value=fake_scheduler) as scheduler_factory, \
             patch.object(server, "_start_question_cards_scheduler") as question_scheduler, \
             patch.object(server, "_start_behavioral_cards_scheduler") as behavioral_scheduler, \
             patch.object(server, "_start_opportunity_cards_scheduler") as opportunity_scheduler, \
             patch.object(server, "_start_topic_overviews_scheduler") as topic_scheduler, \
             patch.object(server.db, "close"):
            asyncio.run(enter_lifespan())

        scheduler_factory.assert_called_once()
        fake_scheduler.startup.assert_awaited_once()
        question_scheduler.assert_called_once()
        behavioral_scheduler.assert_called_once()
        opportunity_scheduler.assert_called_once()
        topic_scheduler.assert_called_once()

    def test_web_role_reads_shared_scheduler_snapshot(self) -> None:
        shared = {
            "status": "active",
            "is_active": True,
            "interval_minutes": 15,
            "running_now": True,
            "last_run_started_at": "2026-04-11T10:00:00+00:00",
            "last_run_finished_at": None,
            "last_success_at": "2026-04-11T09:45:00+00:00",
            "next_run_at": "2026-04-11T10:15:00+00:00",
            "last_error": None,
            "last_result": {"mode": "normal"},
            "last_mode": "normal",
        }
        writer = SimpleNamespace(
            get_shared_scraper_runtime_snapshot=lambda default=None, timeout_seconds=1.5: shared
        )

        with patch.object(server, "APP_ROLE", "web"), \
             patch.object(server, "scraper_scheduler", None), \
             patch.object(server, "get_supabase_writer", return_value=writer):
            payload = server.get_current_scraper_scheduler_status()

        self.assertEqual(payload["status"], "active")
        self.assertEqual(payload["next_run_at"], "2026-04-11T10:15:00+00:00")
        self.assertTrue(payload["running_now"])

    def test_web_role_scheduler_status_falls_back_to_default_when_shared_read_fails(self) -> None:
        writer = SimpleNamespace(
            get_shared_scraper_runtime_snapshot=lambda default=None, timeout_seconds=1.5: (_ for _ in ()).throw(
                RuntimeError("storage timed out")
            )
        )
        old_cached = server._last_shared_scraper_status

        try:
            server._last_shared_scraper_status = None
            with patch.object(server, "APP_ROLE", "web"), \
                 patch.object(server, "scraper_scheduler", None), \
                 patch.object(server, "get_supabase_writer", return_value=writer):
                payload = server.get_current_scraper_scheduler_status()
        finally:
            server._last_shared_scraper_status = old_cached

        self.assertEqual(payload["status"], "stopped")
        self.assertFalse(payload["is_active"])
        self.assertIsNone(payload["next_run_at"])

    def test_web_run_now_enqueues_worker_control_command(self) -> None:
        saved: dict[str, object] = {}

        def save_control(payload: dict) -> bool:
            saved.update(payload)
            return True

        writer = SimpleNamespace(
            save_shared_scraper_control_command=save_control,
            get_shared_scraper_runtime_snapshot=lambda default=None, timeout_seconds=1.5: {
                "status": "active",
                "is_active": True,
                "interval_minutes": 15,
                "running_now": False,
                "last_run_started_at": None,
                "last_run_finished_at": None,
                "last_success_at": None,
                "next_run_at": "2026-04-11T10:15:00+00:00",
                "last_error": None,
                "last_result": None,
                "last_mode": "normal",
            },
        )

        with patch.object(server, "APP_ROLE", "web"), \
             patch.object(server, "get_supabase_writer", return_value=writer):
            payload = asyncio.run(server.run_scraper_once())

        self.assertEqual(saved["action"], "run_once")
        self.assertEqual(saved["status"], "pending")
        self.assertEqual(payload["worker_control"]["action"], "run_once")
        self.assertEqual(payload["worker_control"]["status"], "pending")

    def test_web_set_interval_enqueues_worker_control_command(self) -> None:
        saved: dict[str, object] = {}

        def save_control(payload: dict) -> bool:
            saved.update(payload)
            return True

        writer = SimpleNamespace(
            save_shared_scraper_control_command=save_control,
            get_shared_scraper_runtime_snapshot=lambda default=None, timeout_seconds=1.5: {
                "status": "active",
                "is_active": True,
                "interval_minutes": 15,
                "running_now": False,
                "last_run_started_at": None,
                "last_run_finished_at": None,
                "last_success_at": None,
                "next_run_at": "2026-04-11T10:15:00+00:00",
                "last_error": None,
                "last_result": None,
                "last_mode": "normal",
            },
        )

        payload_model = server.ScraperSchedulerUpdateRequest(interval_minutes=30)

        with patch.object(server, "APP_ROLE", "web"), \
             patch.object(server, "get_supabase_writer", return_value=writer):
            payload = asyncio.run(server.update_scraper_scheduler(payload_model))

        self.assertEqual(saved["action"], "set_interval")
        self.assertEqual(saved["interval_minutes"], 30)
        self.assertEqual(payload["interval_minutes"], 30)

    def test_app_lifespan_requires_healthy_redis_in_locked_env(self) -> None:
        async def enter_lifespan() -> None:
            async with server.app_lifespan(server.app):
                return None

        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server, "get_runtime_coordinator", return_value=_CoordinatorStub(ping_result=False)), \
             patch.object(server, "_should_run_background_jobs", return_value=False):
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(enter_lifespan())

        self.assertIn("healthy Redis runtime coordinator", str(ctx.exception))

    def test_worker_requires_healthy_redis_in_locked_env(self) -> None:
        with patch.object(worker.config, "IS_LOCKED_ENV", True), \
             patch.object(worker, "get_runtime_coordinator", return_value=_CoordinatorStub(ping_result=False)):
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(worker.run_worker())

        self.assertIn("healthy Redis runtime coordinator", str(ctx.exception))

    def test_worker_is_blocked_in_staging_testing_environment(self) -> None:
        with patch.object(worker.config, "IS_STAGING", True):
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(worker.run_worker())

        self.assertIn("web-only", str(ctx.exception))


class SchedulerDistributedLockTests(unittest.TestCase):
    def _service(self) -> ScraperSchedulerService:
        return ScraperSchedulerService(SimpleNamespace())

    def test_set_interval_preserves_active_state_in_web_only_runtime(self) -> None:
        async def scenario() -> None:
            writer = SimpleNamespace(
                get_scraper_scheduler_settings=lambda default_interval_minutes=15: {
                    "is_active": True,
                    "interval_minutes": default_interval_minutes,
                    "updated_at": None,
                },
                save_scraper_scheduler_settings=lambda *, is_active, interval_minutes: {
                    "is_active": is_active,
                    "interval_minutes": interval_minutes,
                    "updated_at": None,
                },
            )
            service = ScraperSchedulerService(writer)
            with patch.dict("os.environ", {"APP_ROLE": "web"}, clear=False):
                status = await service.set_interval(45)
            self.assertTrue(status["is_active"])
            self.assertEqual(status["interval_minutes"], 45)

        asyncio.run(scenario())

    def test_worker_control_cycle_processes_pending_run_once_command(self) -> None:
        async def scenario() -> None:
            command_store = {
                "request_id": "cmd-1",
                "action": "run_once",
                "status": "pending",
                "requested_at": "2026-04-11T10:00:00+00:00",
            }

            writer = SimpleNamespace(
                get_shared_scraper_control_command=lambda default=None, timeout_seconds=1.5: dict(command_store),
                save_shared_scraper_control_command=lambda payload: command_store.update(payload) or True,
            )

            service = ScraperSchedulerService(writer)
            service.desired_active = True

            async def fake_run_cycle(*, use_runtime_lock: bool = True) -> None:
                self.assertFalse(use_runtime_lock)
                service.last_success_at = service.last_run_started_at

            run_cycle_mock = AsyncMock(side_effect=fake_run_cycle)

            with patch("api.scraper_scheduler._runtime_role_allows_background_jobs", return_value=True), \
                 patch.object(service, "_run_cycle", run_cycle_mock), \
                 patch.object(service, "status", return_value={"status": "active", "running_now": False}):
                await service._run_control_cycle()

            self.assertEqual(command_store["status"], "completed")
            self.assertEqual(command_store["scheduler_status"]["status"], "active")
            run_cycle_mock.assert_awaited_once_with(use_runtime_lock=False)

        asyncio.run(scenario())

    def test_worker_control_cycle_marks_failed_only_after_completion_writeback_retries_exhausted(self) -> None:
        async def scenario() -> None:
            command_store = {
                "request_id": "cmd-2",
                "action": "run_once",
                "status": "pending",
                "requested_at": "2026-04-11T10:00:00+00:00",
            }

            def save_control(payload: dict) -> bool:
                status = str(payload.get("status") or "")
                if status == "processing":
                    command_store.update(payload)
                    return True
                if status == "completed":
                    return False
                if status == "failed":
                    command_store.update(payload)
                    return True
                command_store.update(payload)
                return True

            writer = SimpleNamespace(
                get_shared_scraper_control_command=lambda default=None, timeout_seconds=1.5: dict(command_store),
                save_shared_scraper_control_command=save_control,
            )

            service = ScraperSchedulerService(writer)
            service.desired_active = True

            with patch("api.scraper_scheduler._runtime_role_allows_background_jobs", return_value=True), \
                 patch.object(service, "_run_cycle", AsyncMock()), \
                 patch.object(service, "status", return_value={"status": "active", "running_now": False}):
                await service._run_control_cycle()

            self.assertEqual(command_store["status"], "failed")
            self.assertEqual(
                command_store["error"],
                "shared control completion writeback failed after retries",
            )

        asyncio.run(scenario())

    def test_run_cycle_persists_terminal_running_false_snapshot(self) -> None:
        coordinator = _CoordinatorStub()
        shared_status_writes: list[dict] = []

        async def scenario() -> None:
            writer = SimpleNamespace(
                save_shared_scraper_runtime_snapshot=lambda payload: shared_status_writes.append(dict(payload)) or True,
                save_shared_freshness_snapshot=lambda payload: True,
            )
            service = ScraperSchedulerService(writer)

            with patch("api.scraper_scheduler.get_runtime_coordinator", return_value=coordinator), \
                 patch("api.scraper_scheduler.run_full_cycle", AsyncMock(return_value={"ok": True})), \
                 patch.object(service, "_get_or_create_client", AsyncMock(return_value=object())), \
                 patch("api.scraper_scheduler.get_freshness_snapshot", return_value={"generated_at": "now"}):
                await service._run_cycle()

        asyncio.run(scenario())

        self.assertGreaterEqual(len(shared_status_writes), 2)
        self.assertTrue(shared_status_writes[0]["running_now"])
        self.assertFalse(shared_status_writes[-1]["running_now"])
        self.assertIsNotNone(shared_status_writes[-1]["last_run_finished_at"])

    def test_run_cycle_is_exclusive_across_scheduler_instances(self) -> None:
        coordinator = _CoordinatorStub()
        run_calls = 0

        async def scenario() -> None:
            cycle_started = asyncio.Event()
            allow_cycle_finish = asyncio.Event()

            async def fake_run_full_cycle(_client, _db):
                nonlocal run_calls
                run_calls += 1
                cycle_started.set()
                await allow_cycle_finish.wait()
                return {"mode": "normal", "ok": True}

            service_a = self._service()
            service_b = self._service()
            fake_client = object()

            with patch("api.scraper_scheduler.get_runtime_coordinator", return_value=coordinator), \
                 patch("api.scraper_scheduler.run_full_cycle", side_effect=fake_run_full_cycle), \
                 patch.object(service_a, "_get_or_create_client", AsyncMock(return_value=fake_client)), \
                 patch.object(service_b, "_get_or_create_client", AsyncMock(return_value=fake_client)):
                task_a = asyncio.create_task(service_a._run_cycle())
                await cycle_started.wait()
                task_b = asyncio.create_task(service_b._run_cycle())
                await asyncio.sleep(0)

                self.assertEqual(run_calls, 1)
                self.assertTrue(service_a.running_now)
                self.assertFalse(service_b.running_now)

                allow_cycle_finish.set()
                await asyncio.gather(task_a, task_b)

                self.assertEqual(run_calls, 1)
                self.assertEqual(service_a.last_result, {"mode": "normal", "ok": True})
                self.assertIsNone(service_b.last_result)
                self.assertEqual(coordinator._locks, {})

        asyncio.run(scenario())

    def test_catchup_cycle_is_exclusive_across_scheduler_instances(self) -> None:
        coordinator = _CoordinatorStub()
        run_calls = 0

        async def scenario() -> None:
            cycle_started = asyncio.Event()
            allow_cycle_finish = asyncio.Event()

            async def fake_run_catchup_cycle(_client, _db):
                nonlocal run_calls
                run_calls += 1
                cycle_started.set()
                await allow_cycle_finish.wait()
                return {"mode": "catchup", "ok": True}

            service_a = self._service()
            service_b = self._service()
            fake_client = object()

            with patch("api.scraper_scheduler.get_runtime_coordinator", return_value=coordinator), \
                 patch("api.scraper_scheduler.run_catchup_cycle", side_effect=fake_run_catchup_cycle), \
                 patch.object(service_a, "_get_or_create_client", AsyncMock(return_value=fake_client)), \
                 patch.object(service_b, "_get_or_create_client", AsyncMock(return_value=fake_client)):
                task_a = asyncio.create_task(service_a._run_catchup_cycle())
                await cycle_started.wait()
                task_b = asyncio.create_task(service_b._run_catchup_cycle())
                await asyncio.sleep(0)

                self.assertEqual(run_calls, 1)
                self.assertTrue(service_a.running_now)
                self.assertFalse(service_b.running_now)

                allow_cycle_finish.set()
                await asyncio.gather(task_a, task_b)

                self.assertEqual(run_calls, 1)
                self.assertEqual(service_a.last_result, {"mode": "catchup", "ok": True})
                self.assertIsNone(service_b.last_result)
                self.assertEqual(coordinator._locks, {})

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
