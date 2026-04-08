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
from api.runtime_coordinator import get_runtime_coordinator
from api.source_resolution import run_source_resolution_cycle
from scraper.scrape_orchestrator import run_full_cycle, run_catchup_cycle


def _iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


class ScraperSchedulerService:
    def __init__(self, supabase_writer):
        self.db = supabase_writer
        self.scheduler = AsyncIOScheduler(timezone="UTC")
        self.job_id = "scraper_runtime_job"
        self.resolution_job_id = "source_resolution_runtime_job"

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

        self._run_lock = asyncio.Lock()
        self._resolution_run_lock = asyncio.Lock()
        self._client: Optional[TelegramClient] = None
        self._background_tasks: set[asyncio.Task] = set()

    async def startup(self) -> None:
        self._ensure_scheduler_started()
        settings = self.db.get_scraper_scheduler_settings(default_interval_minutes=15)
        self.interval_minutes = int(settings.get("interval_minutes", 15))
        self.desired_active = bool(settings.get("is_active", False))

        if self.desired_active:
            self._upsert_interval_job()
        if config.FEATURE_SOURCE_RESOLUTION_WORKER:
            self._upsert_resolution_job()

        logger.info(
            f"Scraper scheduler ready | active={self.desired_active} interval={self.interval_minutes}m"
        )

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

    def _next_resolution_run_iso(self) -> Optional[str]:
        if not config.FEATURE_SOURCE_RESOLUTION_WORKER:
            return None
        job = self.scheduler.get_job(self.resolution_job_id)
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

    async def start(self) -> dict:
        self._ensure_scheduler_started()
        self.desired_active = True
        self._upsert_interval_job()
        persisted = self.db.save_scraper_scheduler_settings(
            is_active=self.desired_active,
            interval_minutes=self.interval_minutes,
        )
        return self.status(persisted)

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
        return self.status(persisted)

    async def set_interval(self, interval_minutes: int) -> dict:
        self.interval_minutes = int(interval_minutes)
        if self.interval_minutes < 1:
            self.interval_minutes = 1

        if self.desired_active:
            self._ensure_scheduler_started()
            self._upsert_interval_job()

        persisted = self.db.save_scraper_scheduler_settings(
            is_active=self.desired_active,
            interval_minutes=self.interval_minutes,
        )
        return self.status(persisted)

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

    def status(self, persisted: Optional[dict] = None) -> dict:
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
