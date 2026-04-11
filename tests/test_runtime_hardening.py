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
             patch.object(server.config, "validate"), \
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
             patch.object(server.config, "validate"), \
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
        writer = SimpleNamespace(get_shared_scraper_runtime_snapshot=lambda default=None: shared)

        with patch.object(server, "APP_ROLE", "web"), \
             patch.object(server, "scraper_scheduler", None), \
             patch.object(server, "get_supabase_writer", return_value=writer):
            payload = server.get_current_scraper_scheduler_status()

        self.assertEqual(payload["status"], "active")
        self.assertEqual(payload["next_run_at"], "2026-04-11T10:15:00+00:00")
        self.assertTrue(payload["running_now"])

    def test_app_lifespan_requires_healthy_redis_in_locked_env(self) -> None:
        async def enter_lifespan() -> None:
            async with server.app_lifespan(server.app):
                return None

        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "validate"), \
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

    def test_startup_retries_scheduler_settings_and_registers_job(self) -> None:
        async def scenario() -> None:
            calls = {"count": 0}

            def load_scraper_scheduler_settings(*, default_interval_minutes=15, raise_on_error=False):
                del default_interval_minutes, raise_on_error
                calls["count"] += 1
                if calls["count"] == 1:
                    raise RuntimeError("transient storage read failure")
                return {
                    "is_active": True,
                    "interval_minutes": 30,
                    "updated_at": "2026-04-11T06:09:59.590617+00:00",
                }

            service = ScraperSchedulerService(
                SimpleNamespace(load_scraper_scheduler_settings=load_scraper_scheduler_settings)
            )
            with patch("api.scraper_scheduler.config.IS_LOCKED_ENV", True), \
                 patch("api.scraper_scheduler.config.FEATURE_SOURCE_RESOLUTION_WORKER", False), \
                 patch("api.scraper_scheduler.config.PIPELINE_QUEUE_ENABLED", False):
                await service.startup()
                self.assertEqual(calls["count"], 2)
                self.assertTrue(service.desired_active)
                self.assertIsNotNone(service.scheduler.get_job(service.job_id))
                await service.shutdown()

        asyncio.run(scenario())

    def test_startup_raises_in_locked_env_when_settings_read_fails(self) -> None:
        async def scenario() -> None:
            def load_scraper_scheduler_settings(*, default_interval_minutes=15, raise_on_error=False):
                del default_interval_minutes, raise_on_error
                raise RuntimeError("storage unavailable")

            service = ScraperSchedulerService(
                SimpleNamespace(load_scraper_scheduler_settings=load_scraper_scheduler_settings)
            )
            with patch("api.scraper_scheduler.config.IS_LOCKED_ENV", True), \
                 patch("api.scraper_scheduler.config.FEATURE_SOURCE_RESOLUTION_WORKER", False), \
                 patch("api.scraper_scheduler.config.PIPELINE_QUEUE_ENABLED", False):
                with self.assertRaises(RuntimeError) as ctx:
                    await service.startup()
                self.assertIn("Failed to load persisted scraper scheduler settings", str(ctx.exception))
                await service.shutdown()

        asyncio.run(scenario())

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

    def test_pipeline_queue_repair_cycle_exits_when_flag_disabled(self) -> None:
        async def scenario() -> None:
            db = SimpleNamespace(repair_pipeline_stage_queues=AsyncMock())
            service = ScraperSchedulerService(db)

            with patch.object(server.config, "PIPELINE_QUEUE_ENABLED", False), \
                 patch("api.scraper_scheduler.config.PIPELINE_QUEUE_ENABLED", False):
                await service._run_pipeline_queue_repair_cycle()

            db.repair_pipeline_stage_queues.assert_not_called()
            self.assertIsNone(service.pipeline_queue_repair_last_result)

        asyncio.run(scenario())

    def test_pipeline_queue_reclaim_cycle_runs_all_reclaimers(self) -> None:
        coordinator = _CoordinatorStub()

        async def scenario() -> None:
            db = SimpleNamespace(
                reclaim_expired_ai_post_jobs=lambda: 2,
                reclaim_expired_ai_comment_group_jobs=lambda: 3,
                reclaim_expired_neo4j_sync_jobs=lambda: 4,
            )
            service = ScraperSchedulerService(db)

            with patch("api.scraper_scheduler.get_runtime_coordinator", return_value=coordinator), \
                 patch("api.scraper_scheduler.config.PIPELINE_QUEUE_ENABLED", True), \
                 patch("api.scraper_scheduler.config.PIPELINE_QUEUE_RECLAIM_INTERVAL_MINUTES", 1):
                await service._run_pipeline_queue_reclaim_cycle()

            self.assertEqual(
                service.pipeline_queue_reclaim_last_result,
                {
                    "ai_post_jobs_reclaimed": 2,
                    "ai_comment_group_jobs_reclaimed": 3,
                    "neo4j_sync_jobs_reclaimed": 4,
                },
            )
            self.assertIsNone(service.pipeline_queue_reclaim_last_error)
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
