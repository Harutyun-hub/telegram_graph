"""
scraper_scheduler.py — Runtime scheduler for scraper-only orchestration.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
from telethon import TelegramClient

import config
from scraper.scrape_orchestrator import run_scrape_cycle


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

        self.interval_minutes = 15
        self.desired_active = False

        self.running_now = False
        self.last_run_started_at: Optional[datetime] = None
        self.last_run_finished_at: Optional[datetime] = None
        self.last_success_at: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.last_result: Optional[dict] = None

        self._run_lock = asyncio.Lock()
        self._client: Optional[TelegramClient] = None

    async def startup(self) -> None:
        self.scheduler.start()
        settings = self.db.get_scraper_scheduler_settings(default_interval_minutes=15)
        self.interval_minutes = int(settings.get("interval_minutes", 15))
        self.desired_active = bool(settings.get("is_active", False))

        if self.desired_active:
            self._upsert_interval_job()

        logger.info(
            f"Scraper scheduler ready | active={self.desired_active} interval={self.interval_minutes}m"
        )

    async def shutdown(self) -> None:
        try:
            self.scheduler.shutdown(wait=False)
        except Exception:
            pass

        if self._client:
            try:
                self._client.disconnect()
            except Exception:
                pass
            self._client = None

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

    async def _get_or_create_client(self) -> TelegramClient:
        if self._client and self._client.is_connected():
            return self._client

        client = TelegramClient(
            config.TELEGRAM_SESSION_NAME,
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )
        await client.connect()
        if not await client.is_user_authorized():
            client.disconnect()
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
            self.running_now = True
            self.last_error = None
            self.last_run_started_at = datetime.now(timezone.utc)
            self.last_run_finished_at = None

            try:
                client = await self._get_or_create_client()
                result = await run_scrape_cycle(client, self.db)
                self.last_result = result
                self.last_success_at = datetime.now(timezone.utc)
                logger.success(f"Scraper cycle completed: {result}")
            except Exception as e:
                self.last_error = str(e)
                logger.error(f"Scraper cycle failed: {e}")
            finally:
                self.last_run_finished_at = datetime.now(timezone.utc)
                self.running_now = False

    async def start(self) -> dict:
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
            self._upsert_interval_job()

        persisted = self.db.save_scraper_scheduler_settings(
            is_active=self.desired_active,
            interval_minutes=self.interval_minutes,
        )
        return self.status(persisted)

    async def run_once(self) -> dict:
        if self.running_now:
            return self.status()

        asyncio.create_task(self._run_cycle())
        return self.status()

    def status(self, persisted: Optional[dict] = None) -> dict:
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
            "persisted": persisted,
        }
