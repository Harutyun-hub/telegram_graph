"""
worker.py — Dedicated background worker entrypoint.

Run with:
  python -m api.worker
"""

from __future__ import annotations

import asyncio

from loguru import logger

import config
from api import server
from api.runtime_coordinator import get_runtime_coordinator


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

    if server.social_runtime_service is not None:
        await server.social_runtime_service.shutdown()
        server.social_runtime_service = None


async def run_worker() -> None:
    logger.info("Starting dedicated worker runtime")
    if config.IS_STAGING and not config.ALLOW_STAGING_WORKER:
        raise RuntimeError("Staging/testing environments require ALLOW_STAGING_WORKER=true for dedicated worker startup.")
    coordinator = get_runtime_coordinator()
    if config.IS_LOCKED_ENV and not coordinator.ping():
        raise RuntimeError("Locked environments require a healthy Redis runtime coordinator.")

    scheduler = server.get_scraper_scheduler()
    await scheduler.startup()
    social_runtime = server.get_social_runtime()
    await social_runtime.startup()
    server._start_question_cards_scheduler()
    server._start_behavioral_cards_scheduler()
    server._start_opportunity_cards_scheduler()
    server._start_topic_overviews_scheduler()

    startup_tasks: list[asyncio.Task] = []
    if server.RUN_STARTUP_WARMERS:
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
