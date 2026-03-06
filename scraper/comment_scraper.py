"""
comment_scraper.py — Scrapes comments from posts that have discussion groups.

Telegram channel posts appear in the channel itself.
Comments live in a linked "discussion group" (a separate supergroup).
We use GetDiscussionMessage to find that linked group, then
iter_messages() on replies to the specific post.
"""
from telethon import TelegramClient
from telethon.tl.functions.messages import GetDiscussionMessageRequest
from telethon.tl.types import Message
from telethon.errors import FloodWaitError, MsgIdInvalidError, ChannelPrivateError
from loguru import logger
import asyncio


async def scrape_comments_for_post(
    client: TelegramClient,
    channel_entity,
    post_record: dict,
    supabase_writer,
) -> int:
    """
    Fetch all comments for a single post that has has_comments=True.

    Args:
        client: authenticated Telethon client
        channel_entity: resolved Telegram channel entity
        post_record: row from telegram_posts (must have id, telegram_message_id, channel_id)
        supabase_writer: SupabaseWriter instance

    Returns:
        Number of comments collected
    """
    post_uuid     = post_record["id"]
    channel_uuid  = post_record["channel_id"]
    message_id    = post_record["telegram_message_id"]

    comments_found = 0
    new_comments   = []

    try:
        # Iterate replies to this specific post message
        async for reply in client.iter_messages(
            channel_entity,
            reply_to=message_id,
            wait_time=2,
        ):
            if not isinstance(reply, Message):
                continue

            text = reply.text or reply.message or ""
            if not text.strip():
                continue

            # Extract user info from the sender
            sender = await _get_sender_info(client, reply)

            # Ensure user exists in Supabase
            if sender["telegram_user_id"]:
                user_uuid = supabase_writer.upsert_user(sender)
            else:
                user_uuid = None

            comment = {
                "post_id":              post_uuid,
                "channel_id":           channel_uuid,
                "user_id":              user_uuid,
                "telegram_message_id":  reply.id,
                "reply_to_message_id":  reply.reply_to.reply_to_msg_id if reply.reply_to else message_id,
                "text":                 text[:4096],
                "telegram_user_id":     sender["telegram_user_id"],
                "posted_at":            reply.date.isoformat(),
                "is_processed":         False,
                "neo4j_synced":         False,
            }
            new_comments.append(comment)
            comments_found += 1

            # Batch write every 50 comments
            if len(new_comments) >= 50:
                supabase_writer.upsert_comments(new_comments)
                new_comments = []

    except MsgIdInvalidError:
        logger.warning(f"Post {message_id} has no linked discussion group — skipping")
    except FloodWaitError as e:
        logger.warning(f"FloodWait while fetching comments — sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds + 5)
    except ChannelPrivateError:
        logger.warning(f"Discussion group for post {message_id} is private — skipping")
    except Exception as e:
        logger.error(f"Error fetching comments for post {message_id}: {e}")

    # Write remaining comments
    if new_comments:
        supabase_writer.upsert_comments(new_comments)

    # Mark post's comments as scraped
    if comments_found > 0:
        supabase_writer.mark_post_comments_scraped(post_uuid, comments_found)

    return comments_found


async def _get_sender_info(client: TelegramClient, message: Message) -> dict:
    """
    Extract sender information from a message.
    Returns a dict ready to upsert into telegram_users.
    """
    try:
        sender = await message.get_sender()
        if sender is None:
            return {"telegram_user_id": None, "username": None, "first_name": None, "last_name": None, "is_bot": False}

        return {
            "telegram_user_id": sender.id,
            "username":         getattr(sender, "username", None),
            "first_name":       getattr(sender, "first_name", None),
            "last_name":        getattr(sender, "last_name", None),
            "bio":              None,   # Bio requires a separate GetFullUser call (done in enricher)
            "is_bot":           getattr(sender, "bot", False),
        }
    except Exception:
        return {"telegram_user_id": None, "username": None, "first_name": None, "last_name": None, "is_bot": False}
