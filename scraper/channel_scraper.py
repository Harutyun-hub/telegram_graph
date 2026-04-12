"""
channel_scraper.py — Scrapes content from Telegram broadcast channels and supergroups.

Broadcast channels are ingested as posts and may later fetch linked-discussion comments.
Supergroups are ingested as thread anchors plus group messages so they can reuse the
existing comment/user AI pipeline and Neo4j post-bundle sync.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from loguru import logger
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError
from telethon.tl.types import InputPeerChannel
from telethon.tl.types import Channel, Message

import config
from api.source_resolution import build_pending_source_payload, ensure_resolution_job
from scraper.channel_metadata import (
    channel_peer_ref_from_entity,
    minimal_source_metadata_from_entity,
    resolve_source_metadata,
    source_type_from_entity,
)
from scraper.comment_scraper import get_sender_info


def _disable_unsupported_source(supabase_writer, channel_uuid: str, username: str, reason: str) -> None:
    """Pause a source that resolves to a non-channel Telegram peer."""
    try:
        supabase_writer.update_channel(
            channel_uuid,
            {
                "is_active": False,
                "source_type": "pending",
                "resolution_status": "error",
                "last_resolution_error": reason[:500],
            },
        )
        logger.warning(f"[{username}] Source auto-paused: {reason}")
    except Exception as exc:
        logger.error(f"[{username}] Failed to auto-pause unsupported source: {exc}")


def _parse_iso_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        import re

        fixed = re.sub(
            r"\.(\d{1,5})([+-])",
            lambda m: f'.{m.group(1).ljust(6, "0")}{m.group(2)}',
            value.replace("Z", "+00:00"),
        )
        try:
            parsed = datetime.fromisoformat(fixed)
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _cutoff_date(channel_record: dict) -> datetime:
    depth_days = int(channel_record.get("scrape_depth_days", 30) or 30)
    cutoff = datetime.now(timezone.utc) - timedelta(days=depth_days)
    last_scraped_at = _parse_iso_datetime(channel_record.get("last_scraped_at"))
    if last_scraped_at is not None:
        cutoff = max(cutoff, last_scraped_at)
    return cutoff


def _message_date(message: Message) -> datetime:
    if message.date.tzinfo is None:
        return message.date.replace(tzinfo=timezone.utc)
    return message.date.astimezone(timezone.utc)


def _message_text(message: Message) -> str:
    return (message.text or message.message or "").strip()


def _message_media_type(message: Message) -> str | None:
    if message.photo:
        return "photo"
    if message.video:
        return "video"
    if message.document:
        return "document"
    if message.audio:
        return "audio"
    return None


def _message_reactions(message: Message) -> dict | None:
    reactions: dict[str, int] = {}
    if message.reactions and message.reactions.results:
        for reaction in message.reactions.results:
            emoji = getattr(reaction.reaction, "emoticon", str(reaction.reaction))
            reactions[emoji] = reaction.count
    return reactions if reactions else None


def _reply_to_message_id(message: Message) -> int | None:
    reply = getattr(message, "reply_to", None)
    if reply is None:
        return None
    value = getattr(reply, "reply_to_msg_id", None)
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _thread_top_message_id(message: Message) -> int:
    reply = getattr(message, "reply_to", None)
    if reply is not None:
        for attr in ("reply_to_top_id", "reply_to_msg_id"):
            value = getattr(reply, attr, None)
            try:
                if value is not None:
                    return int(value)
            except Exception:
                pass
    return int(message.id)


def _peer_ref_to_input_peer(peer_ref: dict | None) -> InputPeerChannel | None:
    if not peer_ref:
        return None
    try:
        peer_id = int(peer_ref.get("peer_id"))
        access_hash = int(peer_ref.get("access_hash"))
    except Exception:
        return None
    return InputPeerChannel(channel_id=peer_id, access_hash=access_hash)


def _bump_scrape_diagnostic(diagnostics: dict | None, key: str, amount: int = 1) -> None:
    if diagnostics is None:
        return
    diagnostics[key] = int(diagnostics.get(key, 0) or 0) + amount


async def prepare_source_for_scrape(
    client: TelegramClient,
    channel_record: dict,
    supabase_writer,
    *,
    diagnostics: dict | None = None,
) -> tuple[dict | None, Channel | None]:
    """
    Resolve source metadata and peer type using the shared scheduler client.
    """
    username = channel_record["channel_username"]
    channel_uuid = channel_record["id"]
    entity_lookup = username
    used_peer_ref = False
    if config.FEATURE_SOURCE_PEER_REF_LOOKUP:
        peer_ref = supabase_writer.get_channel_peer_ref(channel_uuid, "primary")
        input_peer = _peer_ref_to_input_peer(peer_ref)
        if input_peer is not None:
            entity_lookup = input_peer
            used_peer_ref = True
            _bump_scrape_diagnostic(diagnostics, "peer_ref_channels")
        elif config.FEATURE_SOURCE_RESOLUTION_QUEUE:
            _bump_scrape_diagnostic(diagnostics, "pending_resolution_channels")
            ensure_resolution_job(supabase_writer, channel_record, priority=20)
            logger.info(f"[{username}] Missing peer ref; queued source resolution and skipping scrape cycle")
            return supabase_writer.get_channel_by_id(channel_uuid), None
    if not used_peer_ref and entity_lookup == username:
        _bump_scrape_diagnostic(diagnostics, "username_fallback_channels")

    try:
        entity = await client.get_entity(entity_lookup)
    except FloodWaitError:
        _bump_scrape_diagnostic(diagnostics, "resolve_flood_wait_count")
        raise
    except (ValueError, ChannelPrivateError) as exc:
        if used_peer_ref and config.FEATURE_SOURCE_RESOLUTION_QUEUE:
            _bump_scrape_diagnostic(diagnostics, "pending_resolution_channels")
            supabase_writer.update_channel(
                channel_uuid,
                build_pending_source_payload(
                    channel_title=(channel_record.get("channel_title") or username or "").strip() or username,
                    error_message=str(exc)[:500],
                ),
            )
            refreshed = supabase_writer.get_channel_by_id(channel_uuid) or channel_record
            ensure_resolution_job(supabase_writer, refreshed, priority=10 if refreshed.get("is_active") else 30)
            logger.warning(f"[{username}] Peer ref is stale or inaccessible; queued source re-resolution")
            return refreshed, None
        supabase_writer.update_channel(
            channel_uuid,
            {
                "source_type": "pending",
                "resolution_status": "error",
                "last_resolution_error": str(exc)[:500],
            },
        )
        logger.error(f"[{username}] Cannot access source: {exc}")
        return supabase_writer.get_channel_by_id(channel_uuid), None

    if not isinstance(entity, Channel):
        peer_type = type(entity).__name__
        reason = f"resolved peer is {peer_type}, not a Telegram channel/supergroup"
        logger.warning(f"[{username}] Skipping unsupported source: {reason}")
        _disable_unsupported_source(supabase_writer, channel_uuid, username, reason)
        return supabase_writer.get_channel_by_id(channel_uuid), None

    try:
        metadata, resolved_entity = await resolve_source_metadata(client, username=username, entity=entity)
        if config.FEATURE_SOURCE_RESOLUTION_QUEUE or config.FEATURE_SOURCE_PEER_REF_LOOKUP:
            supabase_writer.upsert_channel_peer_ref(
                channel_uuid,
                "primary",
                channel_peer_ref_from_entity(resolved_entity, username=username),
            )
        supabase_writer.update_channel(channel_uuid, metadata)
        refreshed = supabase_writer.get_channel_by_id(channel_uuid) or {**channel_record, **metadata}
        return refreshed, resolved_entity
    except Exception as exc:
        logger.warning(f"[{username}] Full source resolution failed, falling back to entity metadata: {exc}")
        metadata = minimal_source_metadata_from_entity(
            entity,
            username=username,
            fallback_title=channel_record.get("channel_title"),
        )
        supabase_writer.update_channel(channel_uuid, metadata)
        refreshed = supabase_writer.get_channel_by_id(channel_uuid) or {**channel_record, **metadata}
        return refreshed, entity


async def scrape_channel(
    client: TelegramClient,
    channel_record: dict,
    supabase_writer,
    *,
    entity: Channel | None = None,
) -> dict:
    """
    Scrape content for a single source row.

    Returns:
        {
            "posts_found": int,
            "comments_found": int,
            "source_type": "channel|supergroup|pending",
        }
    """
    if entity is None:
        channel_record, entity = await prepare_source_for_scrape(client, channel_record, supabase_writer)
        if not channel_record or entity is None:
            return {"posts_found": 0, "comments_found": 0, "source_type": "pending"}

    source_type = str(channel_record.get("source_type") or "").strip().lower()
    if source_type not in {"channel", "supergroup"}:
        source_type = source_type_from_entity(entity)

    if source_type == "supergroup":
        return await _scrape_supergroup(client, channel_record, entity, supabase_writer)
    return await _scrape_broadcast_channel(client, channel_record, entity, supabase_writer)


async def _scrape_broadcast_channel(
    client: TelegramClient,
    channel_record: dict,
    entity: Channel,
    supabase_writer,
) -> dict:
    username = channel_record["channel_username"]
    channel_uuid = channel_record["id"]
    cutoff = _cutoff_date(channel_record)

    logger.info(f"[{username}] Scraping broadcast posts since {cutoff.isoformat()}")

    posts_found = 0
    new_posts: list[dict] = []
    max_posts = max(0, int(getattr(config, "SCRAPE_MAX_POSTS_PER_SOURCE_PER_CYCLE", 0) or 0))
    cap_hit = False
    last_scraped_post_at: datetime | None = None

    try:
        async for message in client.iter_messages(
            entity,
            wait_time=2,
            reverse=False,
        ):
            if not isinstance(message, Message):
                continue

            msg_date = _message_date(message)
            if msg_date <= cutoff:
                logger.debug(f"[{username}] Reached cutoff date, stopping.")
                break

            text = _message_text(message)
            if not text:
                continue

            last_scraped_post_at = msg_date
            post = {
                "channel_id": channel_uuid,
                "telegram_message_id": int(message.id),
                "text": text[:4096],
                "media_type": _message_media_type(message),
                "views": int(message.views or 0),
                "forwards": int(message.forwards or 0),
                "reactions": _message_reactions(message),
                "has_comments": bool(message.replies and message.replies.replies > 0),
                "comment_count": int(message.replies.replies if message.replies else 0),
                "posted_at": msg_date.isoformat(),
                "is_processed": False,
                "neo4j_synced": False,
                "entry_kind": "broadcast_post",
            }
            new_posts.append(post)
            posts_found += 1

            if len(new_posts) >= 50:
                supabase_writer.upsert_posts(new_posts)
                logger.info(f"[{username}] Written {posts_found} broadcast posts so far...")
                new_posts = []

            if max_posts > 0 and posts_found >= max_posts:
                cap_hit = True
                logger.info(f"[{username}] Reached per-cycle broadcast post cap ({max_posts})")
                break

    except FloodWaitError as exc:
        logger.warning(f"[{username}] FloodWait — sleeping {exc.seconds}s")
        await asyncio.sleep(exc.seconds + 5)
    except Exception as exc:
        logger.error(f"[{username}] Unexpected broadcast scrape error: {exc}")

    if new_posts:
        supabase_writer.upsert_posts(new_posts)

    if cap_hit and last_scraped_post_at is not None:
        supabase_writer.update_channel_last_scraped_at(channel_uuid, last_scraped_post_at.isoformat())
    else:
        supabase_writer.update_channel_last_scraped(channel_uuid)
    logger.success(f"[{username}] Done — {posts_found} broadcast posts collected")
    return {"posts_found": posts_found, "comments_found": 0, "source_type": "channel"}


async def _fetch_missing_roots(client: TelegramClient, entity: Channel, thread_top_ids: list[int], root_messages: dict[int, Message]) -> dict[int, Message]:
    missing_ids = [top_id for top_id in thread_top_ids if top_id not in root_messages]
    if not missing_ids:
        return {}

    try:
        fetched = await client.get_messages(entity, ids=missing_ids)
    except Exception as exc:
        logger.warning(f"[{getattr(entity, 'username', getattr(entity, 'id', 'unknown'))}] Root fetch failed: {exc}")
        return {}

    items = fetched if isinstance(fetched, list) else [fetched]
    result: dict[int, Message] = {}
    for item in items:
        if isinstance(item, Message):
            result[int(item.id)] = item
    return result


async def _scrape_supergroup(
    client: TelegramClient,
    channel_record: dict,
    entity: Channel,
    supabase_writer,
) -> dict:
    username = channel_record["channel_username"]
    channel_uuid = channel_record["id"]
    cutoff = _cutoff_date(channel_record)
    max_messages = max(1, int(config.GROUP_MAX_MESSAGES_PER_SOURCE_PER_CYCLE))
    max_threads = max(1, int(config.GROUP_MAX_THREAD_ANCHORS_PER_SOURCE_PER_CYCLE))

    logger.info(
        f"[{username}] Scraping supergroup messages since {cutoff.isoformat()} "
        f"(max_messages={max_messages}, max_threads={max_threads})"
    )

    captured_messages: list[Message] = []
    seen_thread_ids: set[int] = set()

    try:
        async for message in client.iter_messages(
            entity,
            wait_time=2,
            reverse=False,
        ):
            if not isinstance(message, Message):
                continue

            msg_date = _message_date(message)
            if msg_date <= cutoff:
                logger.debug(f"[{username}] Reached cutoff date, stopping.")
                break

            text = _message_text(message)
            if not text:
                continue

            top_id = _thread_top_message_id(message)
            if top_id not in seen_thread_ids and len(seen_thread_ids) >= max_threads:
                continue

            seen_thread_ids.add(top_id)
            captured_messages.append(message)
            if len(captured_messages) >= max_messages:
                logger.info(f"[{username}] Reached per-cycle message cap ({max_messages})")
                break

    except FloodWaitError as exc:
        logger.warning(f"[{username}] FloodWait — sleeping {exc.seconds}s")
        await asyncio.sleep(exc.seconds + 5)
    except Exception as exc:
        logger.error(f"[{username}] Unexpected supergroup scrape error: {exc}")

    if not captured_messages:
        supabase_writer.update_channel_last_scraped(channel_uuid)
        logger.success(f"[{username}] Done — no new supergroup messages collected")
        return {"posts_found": 0, "comments_found": 0, "source_type": "supergroup"}

    thread_top_ids = sorted(seen_thread_ids)
    root_messages: dict[int, Message] = {
        int(message.id): message
        for message in captured_messages
        if int(message.id) in seen_thread_ids
    }
    root_messages.update(await _fetch_missing_roots(client, entity, thread_top_ids, root_messages))

    existing_anchors = supabase_writer.get_posts_by_message_ids(channel_uuid, thread_top_ids)
    anchor_ids: dict[int, str] = {}
    initial_anchor_payloads: list[dict] = []
    anchor_base: dict[int, dict] = {}
    now_iso = datetime.now(timezone.utc).isoformat()

    for top_id in thread_top_ids:
        existing = existing_anchors.get(top_id)
        anchor_id = str(existing.get("id")) if existing and existing.get("id") else str(uuid4())
        anchor_ids[top_id] = anchor_id

        root_message = root_messages.get(top_id)
        fallback_message = next(
            (message for message in captured_messages if _thread_top_message_id(message) == top_id),
            root_message,
        )
        source_message = root_message or fallback_message
        if source_message is None:
            continue

        payload = {
            "id": anchor_id,
            "channel_id": channel_uuid,
            "telegram_message_id": int(top_id),
            "text": _message_text(source_message)[:4096],
            "media_type": _message_media_type(source_message),
            "views": 0,
            "forwards": 0,
            "reactions": _message_reactions(source_message),
            "has_comments": False,
            "comment_count": 0,
            "posted_at": _message_date(source_message).isoformat(),
            "comments_scraped_at": now_iso,
            "is_processed": False,
            "neo4j_synced": False,
            "entry_kind": "thread_anchor",
        }
        anchor_base[top_id] = payload
        initial_anchor_payloads.append(payload)

    if initial_anchor_payloads:
        supabase_writer.upsert_posts(initial_anchor_payloads)

    deduped_messages: dict[int, Message] = {int(message.id): message for message in captured_messages}
    for top_id, root_message in root_messages.items():
        deduped_messages.setdefault(int(top_id), root_message)

    sender_cache: dict[int, dict] = {}
    user_uuid_cache: dict[int, str | None] = {}
    comment_payloads: list[dict] = []

    for message in sorted(deduped_messages.values(), key=_message_date):
        text = _message_text(message)
        if not text:
            continue

        top_id = _thread_top_message_id(message)
        post_id = anchor_ids.get(top_id)
        if not post_id:
            continue

        sender_key = getattr(message, "sender_id", None)
        sender = sender_cache.get(int(sender_key)) if sender_key is not None else None
        if sender is None:
            sender = await get_sender_info(client, message)
            telegram_user_id = sender.get("telegram_user_id")
            if telegram_user_id is not None:
                sender_cache[int(telegram_user_id)] = sender

        telegram_user_id = sender.get("telegram_user_id")
        if telegram_user_id is not None and int(telegram_user_id) not in user_uuid_cache:
            user_uuid_cache[int(telegram_user_id)] = supabase_writer.upsert_user(sender)

        comment_payloads.append(
            {
                "post_id": post_id,
                "channel_id": channel_uuid,
                "user_id": user_uuid_cache.get(int(telegram_user_id)) if telegram_user_id is not None else None,
                "telegram_message_id": int(message.id),
                "reply_to_message_id": _reply_to_message_id(message),
                "text": text[:4096],
                "telegram_user_id": telegram_user_id,
                "posted_at": _message_date(message).isoformat(),
                "message_kind": "group_message",
                "is_thread_root": int(message.id) == int(top_id),
                "thread_top_message_id": int(top_id),
                "is_processed": False,
                "neo4j_synced": False,
            }
        )

    for start in range(0, len(comment_payloads), 100):
        supabase_writer.upsert_comments(comment_payloads[start:start + 100])

    thread_stats = supabase_writer.get_comment_thread_stats(list(anchor_ids.values()))
    final_anchor_payloads: list[dict] = []
    for top_id, anchor_id in anchor_ids.items():
        base_payload = dict(anchor_base.get(top_id) or {})
        if not base_payload:
            continue
        stats = thread_stats.get(anchor_id, {})
        comment_count = int(stats.get("comment_count") or 0)
        base_payload.update(
            {
                "comment_count": comment_count,
                "has_comments": comment_count > 0,
                "thread_message_count": int(stats.get("message_count") or 0),
                "thread_participant_count": int(stats.get("thread_participant_count") or 0),
                "last_activity_at": stats.get("last_activity_at") or base_payload.get("posted_at"),
                "comments_scraped_at": now_iso,
            }
        )
        final_anchor_payloads.append(base_payload)

    if final_anchor_payloads:
        supabase_writer.upsert_posts(final_anchor_payloads)

    supabase_writer.update_channel_last_scraped(channel_uuid)
    total_group_messages = len(comment_payloads)
    logger.success(
        f"[{username}] Done — {len(anchor_ids)} thread anchors and {total_group_messages} group messages collected"
    )
    return {
        "posts_found": len(anchor_ids),
        "comments_found": total_group_messages,
        "source_type": "supergroup",
    }
