from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

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
        with patch.object(server.config, "IS_STAGING", True):
            role, warmers = server._apply_testing_release_invariants("all", True)

        self.assertEqual(role, "web")
        self.assertFalse(warmers)

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
             patch.object(server, "_warm_dashboard_cache", new=AsyncMock()) as warm_dashboard_cache, \
             patch.object(server.db, "close"):
            asyncio.run(enter_lifespan())

        scheduler_factory.assert_not_called()
        fake_scheduler.startup.assert_not_awaited()
        question_scheduler.assert_not_called()
        behavioral_scheduler.assert_not_called()
        opportunity_scheduler.assert_not_called()
        topic_scheduler.assert_not_called()
        warm_dashboard_cache.assert_awaited_once()

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
