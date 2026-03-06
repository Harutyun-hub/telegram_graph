"""
scrape_orchestrator.py — Shared scraper cycle used by scheduler surfaces.
"""
from __future__ import annotations

import asyncio

from loguru import logger
from telethon import TelegramClient

from scraper.channel_scraper import scrape_channel
from scraper.comment_scraper import scrape_comments_for_post


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
