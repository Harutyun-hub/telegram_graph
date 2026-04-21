"""
social_worker.py — Dedicated background worker entrypoint for social runtime.

Run with:
  python -m api.social_worker
"""

from __future__ import annotations

import asyncio

from loguru import logger

import config
from api.runtime_coordinator import get_runtime_coordinator
from social.runtime import SocialRuntimeService
from social.store import SocialStore


def _validate_social_worker_config() -> None:
    missing: list[str] = []
    if not config.SOCIAL_SUPABASE_URL:
        missing.append("SOCIAL_SUPABASE_URL/SUPABASE_URL")
    if not config.SOCIAL_SUPABASE_SERVICE_ROLE_KEY:
        missing.append("SOCIAL_SUPABASE_SERVICE_ROLE_KEY/SUPABASE_SERVICE_ROLE_KEY")
    if not config.SOCIAL_NEO4J_URI:
        missing.append("SOCIAL_NEO4J_URI/NEO4J_URI")
    if not config.SOCIAL_NEO4J_PASSWORD:
        missing.append("SOCIAL_NEO4J_PASSWORD/NEO4J_PASSWORD")
    if not config.OPENAI_API_KEY:
        missing.append("OPENAI_API_KEY/OpenAI_API")
    if not config.SCRAPECREATORS_API_KEY:
        missing.append("SCRAPECREATORS_API_KEY")
    if config.IS_LOCKED_ENV and not config.REDIS_URL:
        missing.append("REDIS_URL")
    if missing:
        raise EnvironmentError(
            "Missing required social-worker environment variables: " + ", ".join(missing)
        )


async def run_social_worker() -> None:
    logger.info("Starting dedicated social worker runtime")
    if config.IS_STAGING and not config.ALLOW_STAGING_SOCIAL_WORKER:
        raise RuntimeError(
            "Staging/testing environments keep the social worker disabled unless ALLOW_STAGING_SOCIAL_WORKER=true."
        )
    _validate_social_worker_config()

    coordinator = get_runtime_coordinator()
    if config.IS_LOCKED_ENV and not coordinator.ping():
        raise RuntimeError("Locked environments require a healthy Redis runtime coordinator.")

    runtime = SocialRuntimeService(SocialStore())
    await runtime.startup()

    try:
        await asyncio.Event().wait()
    finally:
        await runtime.shutdown()


def main() -> None:
    asyncio.run(run_social_worker())


if __name__ == "__main__":
    main()
