from __future__ import annotations

import re

SOURCE_USERNAME_RE = re.compile(r"^[a-z][a-z0-9_]{4,31}$")
SOURCE_USERNAME_VALIDATION_MESSAGE = (
    "Invalid Telegram source. Use @name, t.me/name, t.me/name/123, "
    "or t.me/c/public_name/123; private numeric t.me/c links are not supported. "
    "Username must be 5-32 chars, letters/digits/underscore, and start with a letter."
)


def normalize_channel_username(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""

    value = re.sub(r"^https?://", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^www\.", "", value, flags=re.IGNORECASE)
    lowered = value.lower()
    if lowered.startswith("t.me/"):
        value = value[5:]
    elif lowered.startswith("telegram.me/"):
        value = value[12:]

    value = value.split("?", 1)[0].split("#", 1)[0].strip()
    if value.startswith("@"):
        value = value[1:]

    segments = [segment.strip() for segment in value.split("/") if segment.strip()]
    if not segments:
        return ""

    candidate = segments[0]
    if candidate.lower() == "c":
        candidate = segments[1] if len(segments) > 1 else ""

    candidate = candidate.strip().lower().lstrip("@")
    if not SOURCE_USERNAME_RE.match(candidate):
        return ""
    return candidate


def canonical_channel_username(handle: str) -> str:
    normalized = (handle or "").strip().lower().lstrip("@")
    return f"@{normalized}" if normalized else ""


def is_valid_channel_username(username: str) -> bool:
    return bool(SOURCE_USERNAME_RE.match((username or "").strip().lower().lstrip("@")))
