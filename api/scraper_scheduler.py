"""
scraper_scheduler.py — Runtime scheduler for scrape/process/sync orchestration.
"""
from __future__ import annotations

import asyncio
import os
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from telethon import TelegramClient
from telethon.sessions import StringSession

import config
from api.freshness import get_freshness_snapshot, get_passive_freshness_snapshot
from api.runtime_coordinator import get_runtime_coordinator
from api.source_resolution import run_source_resolution_cycle
from scraper.scrape_orchestrator import run_full_cycle, run_catchup_cycle


def _iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _runtime_role_allows_background_jobs() -> bool:
    role = str(os.getenv("APP_ROLE", "") or "").strip().lower()
    if role not in {"web", "worker", "all"}:
        role = "all"
    return role in {"worker", "all"}


class ScraperSchedulerService:
    _SETTINGS_READ_ATTEMPTS = 3
    _SETTINGS_READ_RETRY_SECONDS = 0.5

    def __init__(self, supabase_writer):
        self.db = supabase_writer
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.job_id = "scraper_runtime_job"
        self.resolution_job_id = "source_resolution_runtime_job"
        self.control_job_id = "scraper_control_runtime_job"
        self.pipeline_queue_repair_job_id = "pipeline_queue_repair_job"
        self.pipeline_queue_reclaim_job_id = "pipeline_queue_reclaim_job"

        self.interval_minutes = 15
        self.desired_active = False

        self.running_now = False
        self.last_run_started_at: Optional[datetime] = None
        self.last_run_finished_at: Optional[datetime] = None
        self.last_success_at: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.last_result: Optional[dict] = None
        self.last_mode: str = "normal"
        self._run_history = deque(maxlen=12)
        self.resolution_running_now = False
        self.resolution_last_run_started_at: Optional[datetime] = None
        self.resolution_last_run_finished_at: Optional[datetime] = None
        self.resolution_last_success_at: Optional[datetime] = None
        self.resolution_last_error: Optional[str] = None
        self.resolution_last_result: Optional[dict] = None
        self._resolution_run_history = deque(maxlen=20)
        self.pipeline_queue_repair_running_now = False
        self.pipeline_queue_repair_last_run_started_at: Optional[datetime] = None
        self.pipeline_queue_repair_last_run_finished_at: Optional[datetime] = None
        self.pipeline_queue_repair_last_success_at: Optional[datetime] = None
        self.pipeline_queue_repair_last_error: Optional[str] = None
        self.pipeline_queue_repair_last_result: Optional[dict] = None
        self.pipeline_queue_reclaim_running_now = False
        self.pipeline_queue_reclaim_last_run_started_at: Optional[datetime] = None
        self.pipeline_queue_reclaim_last_run_finished_at: Optional[datetime] = None
        self.pipeline_queue_reclaim_last_success_at: Optional[datetime] = None
        self.pipeline_queue_reclaim_last_error: Optional[str] = None
        self.pipeline_queue_reclaim_last_result: Optional[dict] = None

        self._run_lock = asyncio.Lock()
        self._resolution_run_lock = asyncio.Lock()
        self._control_run_lock = asyncio.Lock()
        self._pipeline_queue_repair_run_lock = asyncio.Lock()
        self._pipeline_queue_reclaim_run_lock = asyncio.Lock()
        self._client: Optional[TelegramClient] = None
        self._background_tasks: set[asyncio.Task] = set()
        self._last_control_request_id: Optional[str] = None

    async def startup(self) -> None:
        self._ensure_scheduler_started()
        settings = await self._load_startup_scheduler_settings()
        self.interval_minutes = int(settings.get("interval_minutes", 15))
        self.desired_active = bool(settings.get("is_active", False))

        if self.desired_active:
            self._upsert_interval_job()
            job = self.scheduler.get_job(self.job_id)
            next_run_at = _iso(job.next_run_time) if job else None
            if job is None:
                logger.error("Scraper scheduler failed to register interval job despite active persisted state")
                if config.IS_LOCKED_ENV:
                    raise RuntimeError("Scraper scheduler interval job registration failed during startup.")
            else:
                logger.info(
                    "Scraper scheduler interval job registered | interval={}m next_run_at={}",
                    self.interval_minutes,
                    next_run_at,
                )
        else:
            logger.warning(
                "Scraper scheduler started without interval job because persisted state is inactive | interval={}m",
                self.interval_minutes,
            )
        if config.FEATURE_SOURCE_RESOLUTION_WORKER:
            self._upsert_resolution_job()
        if _runtime_role_allows_background_jobs():
            self._upsert_control_job()
        if config.PIPELINE_QUEUE_ENABLED:
            self._upsert_pipeline_queue_repair_job()
            self._upsert_pipeline_queue_reclaim_job()

        logger.info(
            f"Scraper scheduler ready | active={self.desired_active} interval={self.interval_minutes}m"
        )
        self._persist_shared_status()
        self._persist_shared_freshness()

    async def _load_startup_scheduler_settings(self) -> dict:
        default = {
            "is_active": False,
            "interval_minutes": 15,
            "updated_at": None,
        }
        last_error: Exception | None = None

        for attempt in range(1, self._SETTINGS_READ_ATTEMPTS + 1):
            try:
                if hasattr(self.db, "load_scraper_scheduler_settings"):
                    settings = self.db.load_scraper_scheduler_settings(
                        default_interval_minutes=15,
                        raise_on_error=True,
                    )
                else:
                    settings = self.db.get_scraper_scheduler_settings(default_interval_minutes=15)
                if attempt > 1:
                    logger.warning(
                        "Scraper scheduler settings read succeeded after retry | attempt={}/{}",
                        attempt,
                        self._SETTINGS_READ_ATTEMPTS,
                    )
                return settings
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Scraper scheduler settings read failed on startup | attempt={}/{} error={}",
                    attempt,
                    self._SETTINGS_READ_ATTEMPTS,
                    exc,
                )
                if attempt < self._SETTINGS_READ_ATTEMPTS:
                    await asyncio.sleep(self._SETTINGS_READ_RETRY_SECONDS * attempt)

        if config.IS_LOCKED_ENV:
            raise RuntimeError(
                "Failed to load persisted scraper scheduler settings during startup."
            ) from last_error

        logger.warning(
            "Scraper scheduler falling back to default inactive settings after startup read failures"
        )
        return default

    def _ensure_scheduler_started(self) -> None:
        if not self.scheduler.running:
            self.scheduler.start()

    async def shutdown(self) -> None:
        try:
            self.scheduler.shutdown(wait=False)
        except Exception:
            pass

        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                pass
            self._client = None

        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    def _upsert_interval_job(self) -> None:
        try:
            self.scheduler.remove_job(self.job_id)
        except Exception:
            pass

        self.scheduler.add_job(
            self._run_cycle,
            "interval",
            minutes=self.interval_minutes,
            id=self.job_id,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
        )

    def _next_run_iso(self) -> Optional[str]:
        if not self.desired_active:
            return None
        job = self.scheduler.get_job(self.job_id)
        if not job:
            return None
        return _iso(job.next_run_time)

    async def _run_blocking(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    def _upsert_resolution_job(self) -> None:
        try:
            self.scheduler.remove_job(self.resolution_job_id)
        except Exception:
            pass

        self.scheduler.add_job(
            self._run_source_resolution_cycle,
            "interval",
            minutes=max(1, int(config.SOURCE_RESOLUTION_INTERVAL_MINUTES)),
            id=self.resolution_job_id,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
        )

    def _upsert_control_job(self) -> None:
        try:
            self.scheduler.remove_job(self.control_job_id)
        except Exception:
            pass

        self.scheduler.add_job(
            self._run_control_cycle,
            "interval",
            seconds=max(2, int(config.SCRAPER_CONTROL_POLL_SECONDS)),
            id=self.control_job_id,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=10,
        )

    def _upsert_pipeline_queue_repair_job(self) -> None:
        try:
            self.scheduler.remove_job(self.pipeline_queue_repair_job_id)
        except Exception:
            pass

        self.scheduler.add_job(
            self._run_pipeline_queue_repair_cycle,
            "interval",
            minutes=max(1, int(config.PIPELINE_QUEUE_REPAIR_INTERVAL_MINUTES)),
            id=self.pipeline_queue_repair_job_id,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
        )

    def _upsert_pipeline_queue_reclaim_job(self) -> None:
        try:
            self.scheduler.remove_job(self.pipeline_queue_reclaim_job_id)
        except Exception:
            pass

        self.scheduler.add_job(
            self._run_pipeline_queue_reclaim_cycle,
            "interval",
            minutes=max(1, int(config.PIPELINE_QUEUE_RECLAIM_INTERVAL_MINUTES)),
            id=self.pipeline_queue_reclaim_job_id,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
        )

    def _next_resolution_run_iso(self) -> Optional[str]:
        if not config.FEATURE_SOURCE_RESOLUTION_WORKER:
            return None
        job = self.scheduler.get_job(self.resolution_job_id)
        if not job:
            return None
        return _iso(job.next_run_time)

    def _next_pipeline_queue_repair_run_iso(self) -> Optional[str]:
        if not config.PIPELINE_QUEUE_ENABLED:
            return None
        job = self.scheduler.get_job(self.pipeline_queue_repair_job_id)
        if not job:
            return None
        return _iso(job.next_run_time)

    def _next_pipeline_queue_reclaim_run_iso(self) -> Optional[str]:
        if not config.PIPELINE_QUEUE_ENABLED:
            return None
        job = self.scheduler.get_job(self.pipeline_queue_reclaim_job_id)
        if not job:
            return None
        return _iso(job.next_run_time)

    async def _get_or_create_client(self) -> TelegramClient:
        if self._client and self._client.is_connected():
            return self._client

        # Check for session string from environment (Railway deployment)
        session_string = os.getenv("TELEGRAM_SESSION_STRING")

        if session_string:
            logger.info("Using session from TELEGRAM_SESSION_STRING environment variable")
            client = TelegramClient(
                StringSession(session_string),
                config.TELEGRAM_API_ID,
                config.TELEGRAM_API_HASH,
            )
        else:
            # Use file-based session for local development
            logger.info(f"Using file-based session: {config.TELEGRAM_SESSION_NAME}.session")
            client = TelegramClient(
                config.TELEGRAM_SESSION_NAME,
                config.TELEGRAM_API_ID,
                config.TELEGRAM_API_HASH,
            )

        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            if session_string:
                raise RuntimeError(
                    "TELEGRAM_SESSION_STRING is invalid or expired. "
                    "Run 'python scripts/export_telegram_session.py' locally and update Railway environment variable."
                )
            else:
                raise RuntimeError(
                    "Telegram session is not authorized. Run authentication first (session_manager/test_auth)."
                )

        self._client = client
        return client

    async def _run_cycle(self) -> None:
        if self._run_lock.locked():
            logger.warning("Scraper cycle skipped: previous run still active")
            return

        async with self._run_lock:
            coordinator = get_runtime_coordinator()
            lock_token = coordinator.acquire_lock(
                "worker:scrape-cycle",
                ttl_seconds=max(300, int(self.interval_minutes) * 60, config.AI_PROCESS_STAGE_MAX_SECONDS),
            )
            if not lock_token:
                logger.warning("Scraper cycle skipped: runtime coordinator lock is already held")
                return
            self.running_now = True
            self.last_error = None
            self.last_run_started_at = datetime.now(timezone.utc)
            self.last_run_finished_at = None
            self._persist_shared_status()

            try:
                client = await self._get_or_create_client()
                result = await run_full_cycle(client, self.db)
                self.last_mode = "normal"
                self.last_result = result
                self.last_success_at = datetime.now(timezone.utc)
                self._record_success_run(result=result, mode="normal")
                logger.success(f"Pipeline cycle completed: {result}")
            except Exception as e:
                self.last_error = str(e)
                logger.error(f"Pipeline cycle failed: {e}")
            finally:
                coordinator.release_lock("worker:scrape-cycle", lock_token)
                self.last_run_finished_at = datetime.now(timezone.utc)
                self.running_now = False
                self._persist_shared_status()
                self._persist_shared_freshness()

    async def _run_catchup_cycle(self) -> None:
        if self._run_lock.locked():
            logger.warning("Catch-up cycle skipped: previous run still active")
            return

        async with self._run_lock:
            coordinator = get_runtime_coordinator()
            lock_token = coordinator.acquire_lock(
                "worker:catchup-cycle",
                ttl_seconds=max(
                    300,
                    config.AI_PROCESS_STAGE_MAX_SECONDS + config.AI_SYNC_STAGE_MAX_SECONDS,
                ),
            )
            if not lock_token:
                logger.warning("Catch-up cycle skipped: runtime coordinator lock is already held")
                return
            self.running_now = True
            self.last_error = None
            self.last_run_started_at = datetime.now(timezone.utc)
            self.last_run_finished_at = None
            self._persist_shared_status()

            try:
                client = await self._get_or_create_client()
                result = await run_catchup_cycle(client, self.db)
                self.last_mode = "catchup"
                self.last_result = result
                self.last_success_at = datetime.now(timezone.utc)
                self._record_success_run(result=result, mode="catchup")
                logger.success(f"Catch-up cycle completed: {result}")
            except Exception as e:
                self.last_error = str(e)
                logger.error(f"Catch-up cycle failed: {e}")
            finally:
                coordinator.release_lock("worker:catchup-cycle", lock_token)
                self.last_run_finished_at = datetime.now(timezone.utc)
                self.running_now = False
                self._persist_shared_status()
                self._persist_shared_freshness()

    async def _run_source_resolution_cycle(self) -> None:
        if not config.FEATURE_SOURCE_RESOLUTION_WORKER:
            return
        if self._resolution_run_lock.locked():
            logger.warning("Source resolution cycle skipped: previous run still active")
            return

        async with self._resolution_run_lock:
            coordinator = get_runtime_coordinator()
            lock_token = coordinator.acquire_lock(
                "worker:source-resolution-cycle",
                ttl_seconds=max(120, int(config.SOURCE_RESOLUTION_INTERVAL_MINUTES) * 60),
            )
            if not lock_token:
                logger.warning("Source resolution cycle skipped: runtime coordinator lock is already held")
                return

            self.resolution_running_now = True
            self.resolution_last_error = None
            self.resolution_last_run_started_at = datetime.now(timezone.utc)
            self.resolution_last_run_finished_at = None

            try:
                client = await self._get_or_create_client()
                result = await run_source_resolution_cycle(
                    client=client,
                    writer=self.db,
                    session_slot="primary",
                    max_jobs=config.SOURCE_RESOLUTION_MAX_JOBS_PER_RUN,
                    min_interval_seconds=config.SOURCE_RESOLUTION_MIN_INTERVAL_SECONDS,
                )
                self.resolution_last_result = result
                self.resolution_last_success_at = datetime.now(timezone.utc)
                self._record_resolution_run(result=result)
                logger.success(f"Source resolution cycle completed: {result}")
            except Exception as e:
                self.resolution_last_error = str(e)
                logger.error(f"Source resolution cycle failed: {e}")
            finally:
                coordinator.release_lock("worker:source-resolution-cycle", lock_token)
                self.resolution_last_run_finished_at = datetime.now(timezone.utc)
                self.resolution_running_now = False

    async def _run_pipeline_queue_repair_cycle(self) -> None:
        if not config.PIPELINE_QUEUE_ENABLED:
            return
        if self._pipeline_queue_repair_run_lock.locked():
            logger.warning("Pipeline queue repair skipped: previous run still active")
            return

        async with self._pipeline_queue_repair_run_lock:
            coordinator = get_runtime_coordinator()
            lock_token = coordinator.acquire_lock(
                "worker:pipeline-queue-repair",
                ttl_seconds=max(120, int(config.PIPELINE_QUEUE_REPAIR_INTERVAL_MINUTES) * 60),
            )
            if not lock_token:
                logger.warning("Pipeline queue repair skipped: runtime coordinator lock is already held")
                return

            self.pipeline_queue_repair_running_now = True
            self.pipeline_queue_repair_last_error = None
            self.pipeline_queue_repair_last_run_started_at = datetime.now(timezone.utc)
            self.pipeline_queue_repair_last_run_finished_at = None

            try:
                result = await self._run_blocking(
                    self.db.repair_pipeline_stage_queues,
                    limit=config.PIPELINE_QUEUE_REPAIR_BATCH_SIZE,
                )
                self.pipeline_queue_repair_last_result = result
                self.pipeline_queue_repair_last_success_at = datetime.now(timezone.utc)
                logger.success(f"Pipeline queue repair completed: {result}")
            except Exception as e:
                self.pipeline_queue_repair_last_error = str(e)
                logger.error(f"Pipeline queue repair failed: {e}")
            finally:
                coordinator.release_lock("worker:pipeline-queue-repair", lock_token)
                self.pipeline_queue_repair_last_run_finished_at = datetime.now(timezone.utc)
                self.pipeline_queue_repair_running_now = False

    async def _run_pipeline_queue_reclaim_cycle(self) -> None:
        if not config.PIPELINE_QUEUE_ENABLED:
            return
        if self._pipeline_queue_reclaim_run_lock.locked():
            logger.warning("Pipeline queue reclaim skipped: previous run still active")
            return

        async with self._pipeline_queue_reclaim_run_lock:
            coordinator = get_runtime_coordinator()
            lock_token = coordinator.acquire_lock(
                "worker:pipeline-queue-reclaim",
                ttl_seconds=max(120, int(config.PIPELINE_QUEUE_RECLAIM_INTERVAL_MINUTES) * 60),
            )
            if not lock_token:
                logger.warning("Pipeline queue reclaim skipped: runtime coordinator lock is already held")
                return

            self.pipeline_queue_reclaim_running_now = True
            self.pipeline_queue_reclaim_last_error = None
            self.pipeline_queue_reclaim_last_run_started_at = datetime.now(timezone.utc)
            self.pipeline_queue_reclaim_last_run_finished_at = None

            try:
                result = await self._run_blocking(
                    lambda: {
                        "ai_post_jobs_reclaimed": self.db.reclaim_expired_ai_post_jobs(),
                        "ai_comment_group_jobs_reclaimed": self.db.reclaim_expired_ai_comment_group_jobs(),
                        "neo4j_sync_jobs_reclaimed": self.db.reclaim_expired_neo4j_sync_jobs(),
                    }
                )
                self.pipeline_queue_reclaim_last_result = result
                self.pipeline_queue_reclaim_last_success_at = datetime.now(timezone.utc)
                logger.success(f"Pipeline queue reclaim completed: {result}")
            except Exception as e:
                self.pipeline_queue_reclaim_last_error = str(e)
                logger.error(f"Pipeline queue reclaim failed: {e}")
            finally:
                coordinator.release_lock("worker:pipeline-queue-reclaim", lock_token)
                self.pipeline_queue_reclaim_last_run_finished_at = datetime.now(timezone.utc)
                self.pipeline_queue_reclaim_running_now = False

    async def start(self) -> dict:
        self._ensure_scheduler_started()
        self.desired_active = True
        self._upsert_interval_job()
        persisted = self.db.save_scraper_scheduler_settings(
            is_active=self.desired_active,
            interval_minutes=self.interval_minutes,
        )
        status = self.status(persisted)
        self._persist_shared_status(status)
        return status

    async def stop(self) -> dict:
        self.desired_active = False
        try:
            self.scheduler.remove_job(self.job_id)
        except Exception:
            pass

        persisted = self.db.save_scraper_scheduler_settings(
            is_active=self.desired_active,
            interval_minutes=self.interval_minutes,
        )
        status = self.status(persisted)
        self._persist_shared_status(status)
        return status

    async def set_interval(self, interval_minutes: int) -> dict:
        self.interval_minutes = int(interval_minutes)
        if self.interval_minutes < 1:
            self.interval_minutes = 1

        if self.desired_active:
            self._ensure_scheduler_started()
            self._upsert_interval_job()

        persisted_is_active = self.desired_active
        if not _runtime_role_allows_background_jobs():
            current_settings = self.db.get_scraper_scheduler_settings(default_interval_minutes=self.interval_minutes)
            persisted_is_active = bool(current_settings.get("is_active", False))

        persisted = self.db.save_scraper_scheduler_settings(
            is_active=persisted_is_active,
            interval_minutes=self.interval_minutes,
        )
        self.desired_active = persisted_is_active
        status = self.status(persisted)
        self._persist_shared_status(status)
        return status

    async def run_once(self) -> dict:
        if self.running_now:
            return self.status()

        task = asyncio.create_task(self._run_cycle())
        self._background_tasks.add(task)

        def _on_done(done_task: asyncio.Task) -> None:
            self._background_tasks.discard(done_task)
            try:
                exc = done_task.exception()
            except asyncio.CancelledError:
                return
            if exc:
                logger.error(f"Manual scheduler run crashed: {exc}")

        task.add_done_callback(_on_done)
        return self.status()

    async def run_catchup_once(self) -> dict:
        if self.running_now:
            return self.status()

        task = asyncio.create_task(self._run_catchup_cycle())
        self._background_tasks.add(task)

        def _on_done(done_task: asyncio.Task) -> None:
            self._background_tasks.discard(done_task)
            try:
                exc = done_task.exception()
            except asyncio.CancelledError:
                return
            if exc:
                logger.error(f"Catch-up run crashed: {exc}")

        task.add_done_callback(_on_done)
        return self.status()

    async def run_source_resolution_once(self) -> dict:
        if self.resolution_running_now or not config.FEATURE_SOURCE_RESOLUTION_WORKER:
            return self.status()

        task = asyncio.create_task(self._run_source_resolution_cycle())
        self._background_tasks.add(task)

        def _on_done(done_task: asyncio.Task) -> None:
            self._background_tasks.discard(done_task)
            try:
                exc = done_task.exception()
            except asyncio.CancelledError:
                return
            if exc:
                logger.error(f"Manual source resolution run crashed: {exc}")

        task.add_done_callback(_on_done)
        return self.status()

    async def _run_control_cycle(self) -> None:
        if not _runtime_role_allows_background_jobs():
            return
        if self._control_run_lock.locked():
            return

        async with self._control_run_lock:
            try:
                load_fn = getattr(self.db, "get_shared_scraper_control_command", None)
                save_fn = getattr(self.db, "save_shared_scraper_control_command", None)
                if not callable(load_fn) or not callable(save_fn):
                    return

                command = load_fn(default={}) or {}
                if not isinstance(command, dict):
                    return
                request_id = str(command.get("request_id") or "").strip()
                if not request_id or request_id == self._last_control_request_id:
                    return
                if str(command.get("status") or "").strip().lower() != "pending":
                    return

                action = str(command.get("action") or "").strip().lower()
                in_progress = {
                    **command,
                    "status": "processing",
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "processed_by_role": "worker",
                }
                save_fn(in_progress)

                try:
                    if action == "start":
                        status = await self.start()
                    elif action == "stop":
                        status = await self.stop()
                    elif action == "set_interval":
                        interval = int(command.get("interval_minutes") or self.interval_minutes or 15)
                        status = await self.set_interval(interval)
                    elif action == "run_once":
                        status = await self.run_once()
                    elif action == "catchup_once":
                        status = await self.run_catchup_once()
                    else:
                        raise ValueError(f"Unsupported scheduler control action: {action}")

                    save_fn(
                        {
                            **in_progress,
                            "status": "completed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "scheduler_status": status,
                        }
                    )
                except Exception as exc:
                    save_fn(
                        {
                            **in_progress,
                            "status": "failed",
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                            "error": str(exc),
                        }
                    )
                    raise
                finally:
                    self._last_control_request_id = request_id
            except Exception as exc:
                logger.warning(f"Scheduler control cycle failed: {exc}")

    def status(self, persisted: Optional[dict] = None, *, include_resolution_snapshot: bool = False) -> dict:
        resolution_snapshot = None
        if include_resolution_snapshot:
            resolution_snapshot = (
                self.db.get_source_resolution_snapshot(session_slot="primary")
                if hasattr(self.db, "get_source_resolution_snapshot")
                else {
                    "slot_key": "primary",
                    "due_jobs": 0,
                    "leased_jobs": 0,
                    "dead_letter_jobs": 0,
                    "cooldown_slots": 0,
                    "cooldown_until": None,
                    "oldest_due_age_seconds": None,
                    "active_pending_sources": 0,
                    "active_missing_peer_refs": 0,
                }
            )
        return {
            "status": "active" if self.desired_active else "stopped",
            "is_active": self.desired_active,
            "interval_minutes": self.interval_minutes,
            "running_now": self.running_now,
            "last_run_started_at": _iso(self.last_run_started_at),
            "last_run_finished_at": _iso(self.last_run_finished_at),
            "last_success_at": _iso(self.last_success_at),
            "next_run_at": self._next_run_iso(),
            "last_error": self.last_error,
            "last_result": self.last_result,
            "last_mode": self.last_mode,
            "catchup_limits": {
                "comment_limit": config.AI_CATCHUP_COMMENT_LIMIT,
                "post_limit": config.AI_CATCHUP_POST_LIMIT,
                "sync_limit": config.AI_CATCHUP_SYNC_LIMIT,
            },
            "normal_limits": {
                "comment_limit": config.AI_NORMAL_COMMENT_LIMIT,
                "post_limit": config.AI_NORMAL_POST_LIMIT,
                "sync_limit": config.AI_NORMAL_SYNC_LIMIT,
            },
            "run_history": list(self._run_history),
            "resolution": {
                "enabled": bool(config.FEATURE_SOURCE_RESOLUTION_WORKER),
                "running_now": self.resolution_running_now,
                "interval_minutes": max(1, int(config.SOURCE_RESOLUTION_INTERVAL_MINUTES)),
                "last_run_started_at": _iso(self.resolution_last_run_started_at),
                "last_run_finished_at": _iso(self.resolution_last_run_finished_at),
                "last_success_at": _iso(self.resolution_last_success_at),
                "next_run_at": self._next_resolution_run_iso(),
                "last_error": self.resolution_last_error,
                "last_result": self.resolution_last_result,
                "run_history": list(self._resolution_run_history),
                "snapshot": resolution_snapshot,
            },
            "pipeline_queue": {
                "enabled": bool(config.PIPELINE_QUEUE_ENABLED),
                "repair": {
                    "running_now": self.pipeline_queue_repair_running_now,
                    "interval_minutes": max(1, int(config.PIPELINE_QUEUE_REPAIR_INTERVAL_MINUTES)),
                    "last_run_started_at": _iso(self.pipeline_queue_repair_last_run_started_at),
                    "last_run_finished_at": _iso(self.pipeline_queue_repair_last_run_finished_at),
                    "last_success_at": _iso(self.pipeline_queue_repair_last_success_at),
                    "next_run_at": self._next_pipeline_queue_repair_run_iso(),
                    "last_error": self.pipeline_queue_repair_last_error,
                    "last_result": self.pipeline_queue_repair_last_result,
                },
                "reclaim": {
                    "running_now": self.pipeline_queue_reclaim_running_now,
                    "interval_minutes": max(1, int(config.PIPELINE_QUEUE_RECLAIM_INTERVAL_MINUTES)),
                    "last_run_started_at": _iso(self.pipeline_queue_reclaim_last_run_started_at),
                    "last_run_finished_at": _iso(self.pipeline_queue_reclaim_last_run_finished_at),
                    "last_success_at": _iso(self.pipeline_queue_reclaim_last_success_at),
                    "next_run_at": self._next_pipeline_queue_reclaim_run_iso(),
                    "last_error": self.pipeline_queue_reclaim_last_error,
                    "last_result": self.pipeline_queue_reclaim_last_result,
                },
            },
            "persisted": persisted,
        }

    def _record_success_run(self, *, result: dict, mode: str) -> None:
        started = self.last_run_started_at
        finished = self.last_success_at

        duration_minutes: Optional[float] = None
        if started and finished:
            duration_minutes = max(0.01, (finished - started).total_seconds() / 60.0)

        ai_processed_items = int(result.get("ai_analysis_saved", 0) or 0) + int(result.get("posts_processed", 0) or 0)
        ai_failed_items = int(result.get("ai_failed_items", 0) or 0)
        ai_blocked_items = int(result.get("ai_blocked_items", 0) or 0)
        ai_deferred_items = int(result.get("ai_deferred_items", 0) or 0)

        self._run_history.append(
            {
                "finished_at": _iso(finished),
                "mode": mode,
                "duration_minutes": round(duration_minutes, 2) if duration_minutes is not None else None,
                "scraped_items": int(result.get("posts_found", 0) or 0) + int(result.get("comments_found", 0) or 0),
                "ai_processed_items": ai_processed_items,
                "ai_failed_items": ai_failed_items,
                "ai_blocked_items": ai_blocked_items,
                "ai_deferred_items": ai_deferred_items,
                "neo4j_synced_posts": int(result.get("posts_synced", 0) or 0),
                "raw": result,
            }
        )

    def _record_resolution_run(self, *, result: dict) -> None:
        started = self.resolution_last_run_started_at
        finished = self.resolution_last_success_at
        duration_minutes: Optional[float] = None
        if started and finished:
            duration_minutes = max(0.01, (finished - started).total_seconds() / 60.0)

        self._resolution_run_history.append(
            {
                "finished_at": _iso(finished),
                "duration_minutes": round(duration_minutes, 2) if duration_minutes is not None else None,
                "jobs_claimed": int(result.get("jobs_claimed", 0) or 0),
                "jobs_processed": int(result.get("jobs_processed", 0) or 0),
                "jobs_resolved": int(result.get("jobs_resolved", 0) or 0),
                "jobs_requeued": int(result.get("jobs_requeued", 0) or 0),
                "jobs_dead_lettered": int(result.get("jobs_dead_lettered", 0) or 0),
                "raw": result,
            }
        )

    def _persist_shared_status(self, status_payload: Optional[dict] = None) -> None:
        payload = status_payload or self.status()
        try:
            save_fn = getattr(self.db, "save_shared_scraper_runtime_snapshot", None)
            if not callable(save_fn):
                return
            if not save_fn(payload):
                logger.warning("Failed to persist shared scraper runtime snapshot")
        except Exception as exc:
            logger.warning(f"Failed to persist shared scraper runtime snapshot: {exc}")

    def _persist_shared_freshness(self) -> None:
        try:
            if not callable(getattr(self.db, "save_shared_freshness_snapshot", None)):
                return
            get_freshness_snapshot(
                self.db,
                scheduler_status=self.status(),
                force_refresh=True,
                persist_shared_snapshot=True,
            )
        except Exception as exc:
            logger.warning(f"Failed to persist shared freshness snapshot: {exc}")
            try:
                fallback_snapshot = get_passive_freshness_snapshot(
                    self.db,
                    scheduler_status=self.status(),
                )
                if not self.db.save_shared_freshness_snapshot(fallback_snapshot):
                    logger.warning("Failed to persist fallback shared freshness snapshot")
            except Exception as inner_exc:
                logger.warning(f"Failed to persist fallback shared freshness snapshot: {inner_exc}")
