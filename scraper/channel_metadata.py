"""
channel_metadata.py — Resolves full Telegram channel metadata.
"""
from __future__ import annotations

from typing import Optional

from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest


def _canonical_channel_username(value: Optional[str]) -> Optional[str]:
    normalized = (value or "").strip().lower().lstrip("@")
    return f"@{normalized}" if normalized else None


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
