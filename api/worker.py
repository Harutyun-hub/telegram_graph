"""
worker.py — Dedicated background worker entrypoint.

Run with:
  python -m api.worker
"""

from __future__ import annotations

import asyncio
import os

from loguru import logger

import config
from api import server
from api.runtime_coordinator import get_runtime_coordinator


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _allow_staging_background_worker() -> bool:
    return _env_bool("ALLOW_STAGING_BACKGROUND_WORKER", False)


async def _shutdown_background_services() -> None:
    for scheduler_name in (
        "question_cards_scheduler",
        "behavioral_cards_scheduler",
        "opportunity_cards_scheduler",
        "topic_overviews_scheduler",
    ):
        scheduler = getattr(server, scheduler_name, None)
        if scheduler is None:
            continue
        try:
            scheduler.shutdown(wait=False)
        except Exception:
            pass
        setattr(server, scheduler_name, None)

    if server.scraper_scheduler is not None:
        await server.scraper_scheduler.shutdown()
        server.scraper_scheduler = None


async def run_worker() -> None:
    logger.info("Starting dedicated worker runtime")
    staging_override_enabled = config.IS_STAGING and _allow_staging_background_worker()
    if config.IS_STAGING and not staging_override_enabled:
        raise RuntimeError("Staging/testing environments are web-only. Dedicated worker startup is disabled.")
    if staging_override_enabled:
        logger.warning("Staging background worker override enabled; starting dedicated worker runtime")
    coordinator = get_runtime_coordinator()
    if config.IS_LOCKED_ENV and not coordinator.ping():
        raise RuntimeError("Locked environments require a healthy Redis runtime coordinator.")

    scheduler = server.get_scraper_scheduler()
    await scheduler.startup()
    server._start_question_cards_scheduler()
    server._start_behavioral_cards_scheduler()
    server._start_opportunity_cards_scheduler()
    server._start_topic_overviews_scheduler()

    startup_tasks: list[asyncio.Task] = []
    run_startup_warmers = server.RUN_STARTUP_WARMERS or staging_override_enabled
    if run_startup_warmers:
        if config.QUESTION_BRIEFS_REFRESH_ON_STARTUP:
            startup_tasks.append(asyncio.create_task(server._materialize_question_cards_once(force=False)))
        if config.BEHAVIORAL_BRIEFS_REFRESH_ON_STARTUP:
            startup_tasks.append(asyncio.create_task(server._materialize_behavioral_cards_once(force=False)))
        if config.OPPORTUNITY_BRIEFS_REFRESH_ON_STARTUP:
            startup_tasks.append(asyncio.create_task(server._materialize_opportunity_cards_once(force=False)))
        if config.TOPIC_OVERVIEWS_REFRESH_ON_STARTUP:
            startup_tasks.append(asyncio.create_task(server._materialize_topic_overviews_once(force=False)))

    try:
        await asyncio.Event().wait()
    finally:
        for task in startup_tasks:
            task.cancel()
        if startup_tasks:
            await asyncio.gather(*startup_tasks, return_exceptions=True)
        await _shutdown_background_services()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
