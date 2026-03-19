"""
channel_scraper.py — Scrapes posts from a Telegram channel.

For each active channel in Supabase:
  1. Reads last_scraped_at to know where to resume
  2. Iterates messages backwards using iter_messages()
  3. Respects scrape_depth_days as the earliest cutoff
  4. Writes posts to Supabase buffer layer
  5. Updates last_scraped_at when done
"""
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient
from telethon.tl.types import Channel, Message
from telethon.errors import FloodWaitError, ChannelPrivateError
from loguru import logger
import asyncio
import config
from scraper.channel_metadata import get_full_channel_metadata


def _disable_unsupported_source(supabase_writer, channel_uuid: str, username: str, reason: str) -> None:
    """Pause a source that resolves to a non-channel Telegram peer."""
    try:
        supabase_writer.update_channel(channel_uuid, {"is_active": False})
        logger.warning(f"[{username}] Source auto-paused: {reason}")
    except Exception as exc:
        logger.error(f"[{username}] Failed to auto-pause unsupported source: {exc}")


async def scrape_channel(client: TelegramClient, channel_record: dict, supabase_writer) -> int:
    """
    Scrape new posts from a single channel.

    Args:
        client: authenticated Telethon client
        channel_record: row from telegram_channels table
        supabase_writer: SupabaseWriter instance

    Returns:
        Number of new posts found
    """
    username        = channel_record["channel_username"]
    channel_uuid    = channel_record["id"]
    depth_days      = channel_record.get("scrape_depth_days", 30)
    last_scraped_at = channel_record.get("last_scraped_at")

    # Determine the earliest date we care about
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=depth_days)

    # If we've scraped before, only go back to that point
    if last_scraped_at:
        if isinstance(last_scraped_at, str):
            # Supabase sometimes returns 5-digit microseconds (e.g. .29059+00:00)
            # which Python's fromisoformat() rejects. Pad to 6 digits.
            import re
            fixed = re.sub(r'\.(\d{1,5})([+-])', lambda m: f'.{m.group(1).ljust(6, "0")}{m.group(2)}', last_scraped_at.replace("Z", "+00:00"))
            last_scraped_at = datetime.fromisoformat(fixed)
        cutoff_date = max(cutoff_date, last_scraped_at)

    logger.info(f"[{username}] Scraping posts since {cutoff_date.isoformat()}")

    try:
        entity = await client.get_entity(username)
    except (ValueError, ChannelPrivateError) as e:
        logger.error(f"[{username}] Cannot access channel: {e}")
        return 0

    if not isinstance(entity, Channel):
        peer_type = type(entity).__name__
        reason = f"resolved peer is {peer_type}, not a Telegram channel"
        logger.warning(f"[{username}] Skipping unsupported source: {reason}")
        _disable_unsupported_source(supabase_writer, channel_uuid, username, reason)
        return 0

    # Refresh channel metadata when key fields are missing
    metadata_missing = any(
        channel_record.get(field) in (None, "")
        for field in ("telegram_channel_id", "member_count", "description")
    )
    if metadata_missing:
        try:
            metadata = await get_full_channel_metadata(client, username=username, entity=entity)
            supabase_writer.update_channel_metadata(channel_uuid, metadata)
        except Exception as e:
            logger.warning(f"[{username}] Metadata refresh failed: {e}")

    posts_found = 0
    new_posts   = []

    try:
        async for message in client.iter_messages(
            entity,
            wait_time=2,       # 2 second pause between API calls — prevents FloodWait
            reverse=False,     # Newest first
        ):
            if not isinstance(message, Message):
                continue

            # Stop if we've gone past our cutoff date
            msg_date = message.date.replace(tzinfo=timezone.utc) if message.date.tzinfo is None else message.date
            if msg_date <= cutoff_date:
                logger.debug(f"[{username}] Reached cutoff date, stopping.")
                break

            # Skip messages with no text content (pure media, service messages)
            if not message.text and not message.message:
                continue

            text = message.text or message.message or ""

            # Build reactions dict: {"👍": 120, "❤️": 45}
            reactions = {}
            if message.reactions and message.reactions.results:
                for r in message.reactions.results:
                    emoji = getattr(r.reaction, "emoticon", str(r.reaction))
                    reactions[emoji] = r.count

            # Detect media type
            media_type = None
            if message.photo:      media_type = "photo"
            elif message.video:    media_type = "video"
            elif message.document: media_type = "document"
            elif message.audio:    media_type = "audio"

            post = {
                "channel_id":           channel_uuid,
                "telegram_message_id":  message.id,
                "text":                 text[:4096],   # cap at 4096 chars
                "media_type":           media_type,
                "views":                message.views or 0,
                "forwards":             message.forwards or 0,
                "reactions":            reactions if reactions else None,
                "has_comments":         bool(message.replies and message.replies.replies > 0),
                "comment_count":        message.replies.replies if message.replies else 0,
                "posted_at":            msg_date.isoformat(),
                "is_processed":         False,
                "neo4j_synced":         False,
            }
            new_posts.append(post)
            posts_found += 1

            # Write in batches of 50 to avoid huge payloads
            if len(new_posts) >= 50:
                supabase_writer.upsert_posts(new_posts)
                logger.info(f"[{username}] Written {posts_found} posts so far...")
                new_posts = []

    except FloodWaitError as e:
        logger.warning(f"[{username}] FloodWait — sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds + 5)
    except Exception as e:
        logger.error(f"[{username}] Unexpected error during scrape: {e}")

    # Write remaining posts
    if new_posts:
        supabase_writer.upsert_posts(new_posts)

    # Update last_scraped_at
    supabase_writer.update_channel_last_scraped(channel_uuid)

    logger.success(f"[{username}] Done — {posts_found} new posts collected")
    return posts_found
