"""
channel_metadata.py — Resolves full Telegram channel metadata.
"""
from __future__ import annotations

from typing import Optional

from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import Channel


def _canonical_channel_username(value: Optional[str]) -> Optional[str]:
    normalized = (value or "").strip().lower().lstrip("@")
    return f"@{normalized}" if normalized else None


def source_type_from_entity(entity) -> str:
    if bool(getattr(entity, "megagroup", False)) and not bool(getattr(entity, "broadcast", False)):
        return "supergroup"
    return "channel"


def peer_flags_from_entity(entity) -> dict[str, bool]:
    return {
        "broadcast": bool(getattr(entity, "broadcast", False)),
        "megagroup": bool(getattr(entity, "megagroup", False)),
        "gigagroup": bool(getattr(entity, "gigagroup", False)),
        "forum": bool(getattr(entity, "forum", False)),
    }


def minimal_source_metadata_from_entity(
    entity: Channel,
    *,
    username: Optional[str] = None,
    fallback_title: Optional[str] = None,
) -> dict:
    resolved_username = _canonical_channel_username(getattr(entity, "username", None))
    if not resolved_username:
        resolved_username = _canonical_channel_username(username)

    channel_title = getattr(entity, "title", None) or fallback_title
    participants_count = getattr(entity, "participants_count", None)

    return {
        "telegram_channel_id": getattr(entity, "id", None),
        "channel_title": channel_title,
        "channel_username": resolved_username,
        "member_count": participants_count,
        "source_type": source_type_from_entity(entity),
        "resolution_status": "resolved",
        "last_resolution_error": None,
        "telegram_peer_flags": peer_flags_from_entity(entity),
    }


def channel_peer_ref_from_entity(
    entity: Channel,
    *,
    username: Optional[str] = None,
) -> dict:
    resolved_username = _canonical_channel_username(getattr(entity, "username", None))
    if not resolved_username:
        resolved_username = _canonical_channel_username(username)
    return {
        "peer_id": getattr(entity, "id", None),
        "access_hash": getattr(entity, "access_hash", None),
        "resolved_username": resolved_username,
    }


async def get_full_channel_metadata(
    client: TelegramClient,
    *,
    username: Optional[str] = None,
    entity=None,
) -> dict:
    """
    Fetch complete metadata for a Telegram channel/supergroup.

    Returns a dict suitable for SupabaseWriter.update_channel_metadata().
    """
    if entity is None:
        if not username:
            raise ValueError("Either username or entity must be provided")
        entity = await client.get_entity(username)

    full = await client(GetFullChannelRequest(entity))
    full_chat = getattr(full, "full_chat", None)

    about = getattr(full_chat, "about", None)
    if isinstance(about, str):
        about = about.strip() or None

    participants_count = getattr(full_chat, "participants_count", None)
    if participants_count is None:
        participants_count = getattr(entity, "participants_count", None)

    resolved_username = _canonical_channel_username(getattr(entity, "username", None))
    if not resolved_username:
        resolved_username = _canonical_channel_username(username)

    return {
        "telegram_channel_id": getattr(entity, "id", None),
        "channel_title": getattr(entity, "title", None),
        "channel_username": resolved_username,
        "description": about,
        "member_count": participants_count,
    }


async def resolve_source_metadata(
    client: TelegramClient,
    *,
    username: Optional[str] = None,
    entity=None,
) -> tuple[dict, Channel]:
    """
    Resolve source metadata and classify it as a broadcast channel or supergroup.
    """
    if entity is None:
        if not username:
            raise ValueError("Either username or entity must be provided")
        entity = await client.get_entity(username)

    if not isinstance(entity, Channel):
        raise ValueError(f"Unsupported Telegram peer type: {type(entity).__name__}")

    metadata = minimal_source_metadata_from_entity(entity, username=username)
    metadata.update(await get_full_channel_metadata(client, username=username, entity=entity))
    return metadata, entity
