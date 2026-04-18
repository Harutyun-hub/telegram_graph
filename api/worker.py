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
        "default_dashboard_artifact_scheduler",
        "dashboard_v2_fact_scheduler",
        "dashboard_v2_compare_scheduler",
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
    if config.IS_STAGING and not config.STAGING_ENABLE_BACKGROUND_JOBS:
        raise RuntimeError("Staging/testing environments are web-only. Dedicated worker startup is disabled.")
    if config.IS_STAGING and config.STAGING_ENABLE_BACKGROUND_JOBS:
        logger.warning("Staging dedicated worker enabled for controlled AI backend testing")
    coordinator = get_runtime_coordinator()
    if config.IS_LOCKED_ENV and not coordinator.ping():
        raise RuntimeError("Locked environments require a healthy Redis runtime coordinator.")

    scheduler = server.get_scraper_scheduler()
    await scheduler.startup()
    server._start_question_cards_scheduler()
    server._start_behavioral_cards_scheduler()
    server._start_opportunity_cards_scheduler()
    server._start_topic_overviews_scheduler()
    server._start_default_dashboard_artifact_scheduler()
    if server._should_run_dashboard_v2_background_jobs():
        server._start_dashboard_v2_fact_scheduler()
        if config.DASH_V2_COMPARE_ENABLED:
            server._start_dashboard_v2_compare_scheduler()

    startup_tasks: list[asyncio.Task] = []
    if config.DASH_DEFAULT_ARTIFACT_SEEDER_ENABLED and config.DASH_DEFAULT_ARTIFACT_SEED_ON_STARTUP:
        startup_tasks.append(
            asyncio.create_task(server._seed_canonical_default_artifact_once(force=False, reason="startup"))
        )
    if server.RUN_STARTUP_WARMERS:
        if config.QUESTION_BRIEFS_REFRESH_ON_STARTUP:
            startup_tasks.append(asyncio.create_task(server._materialize_question_cards_once(force=False)))
        if config.BEHAVIORAL_BRIEFS_REFRESH_ON_STARTUP:
            startup_tasks.append(asyncio.create_task(server._materialize_behavioral_cards_once(force=False)))
        if config.OPPORTUNITY_BRIEFS_REFRESH_ON_STARTUP:
            startup_tasks.append(asyncio.create_task(server._materialize_opportunity_cards_once(force=False)))
        if config.TOPIC_OVERVIEWS_REFRESH_ON_STARTUP:
            startup_tasks.append(asyncio.create_task(server._materialize_topic_overviews_once(force=False)))
        if server._should_run_dashboard_v2_background_jobs():
            startup_tasks.append(asyncio.create_task(server._materialize_dashboard_v2_incremental_once(force=False)))
    if server._should_run_dashboard_v2_background_jobs():
        startup_tasks.append(asyncio.create_task(server._run_dashboard_v2_materialize_queue_loop()))

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
