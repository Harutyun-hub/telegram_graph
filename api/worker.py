from __future__ import annotations

import asyncio

from loguru import logger

import config
from api import server
from api.social_api import shutdown_social_runtime, startup_social_runtime


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
        await server.scraper_scheduler.shutdown(wait_for_cycle_seconds=30.0)
        server.scraper_scheduler = None

    await shutdown_social_runtime()


async def run_worker() -> None:
    logger.info("Starting dedicated worker runtime")
    if config.IS_STAGING:
        raise RuntimeError("Staging/testing environments are web-only. Dedicated worker startup is disabled.")

    if server._should_run_scraper_scheduler():
        scheduler = server.get_scraper_scheduler()
        await scheduler.startup()

    if server._should_run_any_card_materializers():
        server._start_question_cards_scheduler()
        server._start_behavioral_cards_scheduler()
        server._start_opportunity_cards_scheduler()
        server._start_topic_overviews_scheduler()

    if config.SOCIAL_RUNTIME_ENABLED:
        await startup_social_runtime()

    try:
        await asyncio.Event().wait()
    finally:
        await _shutdown_background_services()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
