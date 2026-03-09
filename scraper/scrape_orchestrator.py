"""
scrape_orchestrator.py — Shared scraper cycle used by scheduler surfaces.
"""
from __future__ import annotations

import asyncio
from functools import partial

from loguru import logger
from telethon import TelegramClient

import config
from scraper.channel_scraper import scrape_channel
from scraper.comment_scraper import scrape_comments_for_post
from processor.intent_extractor import extract_intents, extract_post_intent
from ingester.neo4j_writer import Neo4jWriter


async def run_scrape_cycle(client: TelegramClient, supabase_writer) -> dict:
    """Run one complete scrape cycle for all active channel sources."""
    channels = supabase_writer.get_active_channels()
    if not channels:
        logger.warning("No active channels found in telegram_channels table")
        return {
            "channels_total": 0,
            "channels_processed": 0,
            "posts_found": 0,
            "comments_found": 0,
        }

    channels_processed = 0
    total_posts = 0
    total_comments = 0

    for channel in channels:
        username = channel["channel_username"]
        channel_uuid = channel["id"]
        try:
            post_count = await scrape_channel(client, channel, supabase_writer)
            total_posts += int(post_count)

            if channel.get("scrape_comments", True):
                posts_with_comments = supabase_writer.get_posts_with_comments_pending_for_channel(
                    channel_uuid,
                    limit=20,
                )
                if posts_with_comments:
                    entity = await client.get_entity(username)
                    for post in posts_with_comments:
                        comment_count = await scrape_comments_for_post(client, entity, post, supabase_writer)
                        total_comments += int(comment_count)
                        await asyncio.sleep(1)

            channels_processed += 1
        except Exception as e:
            logger.error(f"[{username}] Scrape cycle failed: {e}")

        await asyncio.sleep(5)

    return {
        "channels_total": len(channels),
        "channels_processed": channels_processed,
        "posts_found": total_posts,
        "comments_found": total_comments,
    }


def _run_ai_process_and_sync_blocking(
    supabase_writer,
    *,
    comment_limit: int,
    post_limit: int,
    sync_limit: int,
) -> dict:
    """Blocking AI + Neo4j sync stage (intended for background thread)."""
    result: dict = {
        "ai_analysis_saved": 0,
        "posts_processed": 0,
        "posts_pending_sync": 0,
        "posts_synced": 0,
        "sync_errors": 0,
    }

    # AI processing stage
    try:
        comments = supabase_writer.get_unprocessed_comments(limit=comment_limit)
        if comments:
            result["ai_analysis_saved"] = int(extract_intents(comments, supabase_writer) or 0)

        posts = supabase_writer.get_unprocessed_posts(limit=post_limit)
        processed_posts = 0
        for post in posts:
            if extract_post_intent(post, supabase_writer):
                processed_posts += 1
        result["posts_processed"] = processed_posts
    except Exception as e:
        logger.error(f"AI process stage failed: {e}")
        result["process_error"] = str(e)

    # Neo4j sync stage
    posts_to_sync = supabase_writer.get_unsynced_posts(limit=sync_limit)
    result["posts_pending_sync"] = len(posts_to_sync)
    if not posts_to_sync:
        return result

    writer: Neo4jWriter | None = None
    try:
        writer = Neo4jWriter()
        for post in posts_to_sync:
            try:
                bundle = supabase_writer.get_post_bundle(post)
                writer.sync_bundle(bundle)
                supabase_writer.mark_post_neo4j_synced(post["id"])
                for analysis in bundle["analyses"].values():
                    analysis_id = analysis.get("id")
                    if analysis_id:
                        supabase_writer.mark_analysis_synced(analysis_id)
                result["posts_synced"] += 1
            except Exception as e:
                result["sync_errors"] += 1
                logger.error(f"Neo4j sync failed for post {post.get('id')}: {e}")
                if "serviceunavailable" in str(e).lower() or "connection" in str(e).lower():
                    result["sync_error"] = str(e)
                    break
    except Exception as e:
        logger.error(f"Neo4j writer init failed: {e}")
        result["sync_error"] = str(e)
    finally:
        if writer:
            try:
                writer.close()
            except Exception:
                pass

    return result


async def run_ai_process_and_sync(
    supabase_writer,
    *,
    comment_limit: int = config.AI_NORMAL_COMMENT_LIMIT,
    post_limit: int = config.AI_NORMAL_POST_LIMIT,
    sync_limit: int = config.AI_NORMAL_SYNC_LIMIT,
) -> dict:
    """Run AI processing + Neo4j sync in worker thread to keep API responsive."""
    task = partial(
        _run_ai_process_and_sync_blocking,
        supabase_writer,
        comment_limit=max(1, int(comment_limit)),
        post_limit=max(1, int(post_limit)),
        sync_limit=max(1, int(sync_limit)),
    )

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, task)


async def run_full_cycle(client: TelegramClient, supabase_writer) -> dict:
    """Run scrape + AI process + Neo4j sync as one runtime cycle."""
    scrape_result = await run_scrape_cycle(client, supabase_writer)
    process_sync_result = await run_ai_process_and_sync(
        supabase_writer,
        comment_limit=config.AI_NORMAL_COMMENT_LIMIT,
        post_limit=config.AI_NORMAL_POST_LIMIT,
        sync_limit=config.AI_NORMAL_SYNC_LIMIT,
    )
    merged = {
        **scrape_result,
        **process_sync_result,
        "mode": "normal",
    }
    return merged


async def run_catchup_cycle(client: TelegramClient, supabase_writer) -> dict:
    """Run AI+sync-heavy catch-up cycle without new scraping."""
    process_sync_result = await run_ai_process_and_sync(
        supabase_writer,
        comment_limit=config.AI_CATCHUP_COMMENT_LIMIT,
        post_limit=config.AI_CATCHUP_POST_LIMIT,
        sync_limit=config.AI_CATCHUP_SYNC_LIMIT,
    )
    return {
        "channels_total": 0,
        "channels_processed": 0,
        "posts_found": 0,
        "comments_found": 0,
        **process_sync_result,
        "mode": "catchup",
    }
