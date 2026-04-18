from __future__ import annotations

import hashlib
import json
from typing import Any

SUPPORTED_SOCIAL_PLATFORMS = ("facebook", "instagram", "google", "tiktok")
SUPPORTED_PROVIDER_KEYS = ("scrapecreators",)
SUPPORTED_TARGET_TYPES = ("page_id", "handle", "domain")
SUPPORTED_CONTENT_TYPES = ("ad", "post", "video")

DEFAULT_PROVIDER_KEY = "scrapecreators"

DEFAULT_TARGET_TYPE_BY_PLATFORM = {
    "facebook": "page_id",
    "instagram": "handle",
    "google": "domain",
    "tiktok": "handle",
}

DEFAULT_CONTENT_TYPES_BY_PLATFORM = {
    "facebook": ["ad"],
    "instagram": ["post"],
    "google": ["ad"],
    "tiktok": ["video"],
}

IDENTIFIER_FIELD_BY_TARGET_TYPE = {
    "page_id": "account_external_id",
    "handle": "account_handle",
    "domain": "domain",
}


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def clean_optional(value: Any) -> str | None:
    text = _trimmed(value)
    return text or None


def normalize_provider_key(value: Any) -> str:
    provider_key = _trimmed(value).lower() or DEFAULT_PROVIDER_KEY
    if provider_key not in SUPPORTED_PROVIDER_KEYS:
        raise ValueError(f"Unsupported social provider: {value}")
    return provider_key


def normalize_platform(value: Any) -> str:
    platform = _trimmed(value).lower()
    if platform not in SUPPORTED_SOCIAL_PLATFORMS:
        raise ValueError(f"Unsupported social platform: {value}")
    return platform


def default_target_type_for_platform(platform: Any) -> str:
    normalized_platform = normalize_platform(platform)
    return DEFAULT_TARGET_TYPE_BY_PLATFORM[normalized_platform]


def normalize_target_type(value: Any, *, platform: Any | None = None) -> str:
    target_type = _trimmed(value).lower()
    if not target_type and platform is not None:
        target_type = default_target_type_for_platform(platform)
    if target_type not in SUPPORTED_TARGET_TYPES:
        raise ValueError(f"Unsupported social target type: {value}")
    return target_type


def normalize_identifier(identifier: Any, *, target_type: Any) -> str:
    normalized_target_type = normalize_target_type(target_type)
    text = _trimmed(identifier)
    if not text:
        raise ValueError("A non-empty social source identifier is required")
    if normalized_target_type == "domain":
        text = text.lower()
        if text.startswith("http://"):
            text = text[len("http://"):]
        elif text.startswith("https://"):
            text = text[len("https://"):]
        return text.rstrip("/")
    if normalized_target_type == "handle":
        return text.lstrip("@").lower()
    return text


def identifier_from_source(source: dict[str, Any]) -> str | None:
    target_type = normalize_target_type(source.get("target_type"), platform=source.get("platform"))
    field = IDENTIFIER_FIELD_BY_TARGET_TYPE[target_type]
    value = clean_optional(source.get(field))
    if value:
        return normalize_identifier(value, target_type=target_type)
    identifier = clean_optional(source.get("identifier"))
    if identifier:
        return normalize_identifier(identifier, target_type=target_type)
    return None


def source_identifier_fields(
    *,
    target_type: Any,
    identifier: Any,
) -> dict[str, str | None]:
    normalized_target_type = normalize_target_type(target_type)
    normalized_identifier = normalize_identifier(identifier, target_type=normalized_target_type)
    payload = {
        "account_handle": None,
        "account_external_id": None,
        "domain": None,
    }
    payload[IDENTIFIER_FIELD_BY_TARGET_TYPE[normalized_target_type]] = normalized_identifier
    return payload


def normalize_content_types(value: Any, *, platform: Any | None = None) -> list[str]:
    if isinstance(value, str):
        candidates = [_trimmed(value).lower()]
    elif isinstance(value, list):
        candidates = [_trimmed(item).lower() for item in value]
    else:
        candidates = []
    normalized = [item for item in candidates if item]
    if not normalized and platform is not None:
        normalized = list(DEFAULT_CONTENT_TYPES_BY_PLATFORM[normalize_platform(platform)])
    invalid = [item for item in normalized if item not in SUPPORTED_CONTENT_TYPES]
    if invalid:
        raise ValueError(f"Unsupported social content types: {invalid}")
    deduped: list[str] = []
    for item in normalized:
        if item not in deduped:
            deduped.append(item)
    return deduped


def build_source_key(
    *,
    provider_key: Any,
    platform: Any,
    target_type: Any,
    identifier: Any,
) -> str:
    return ":".join(
        [
            normalize_provider_key(provider_key),
            normalize_platform(platform),
            normalize_target_type(target_type, platform=platform),
            normalize_identifier(identifier, target_type=target_type),
        ]
    )


def activity_identity_tuple(
    *,
    provider_key: Any,
    platform: Any,
    source_key: Any,
    provider_item_id: Any,
    source_kind: Any,
) -> dict[str, str]:
    return {
        "provider_key": normalize_provider_key(provider_key),
        "platform": normalize_platform(platform),
        "source_key": _trimmed(source_key),
        "provider_item_id": _trimmed(provider_item_id),
        "source_kind": _trimmed(source_kind).lower() or "post",
    }


def build_activity_uid(
    *,
    provider_key: Any,
    platform: Any,
    source_key: Any,
    provider_item_id: Any,
    source_kind: Any,
) -> str:
    identity = activity_identity_tuple(
        provider_key=provider_key,
        platform=platform,
        source_key=source_key,
        provider_item_id=provider_item_id,
        source_kind=source_kind,
    )
    if not identity["source_key"] or not identity["provider_item_id"]:
        raise ValueError("Canonical activity identity requires both source_key and provider_item_id")
    compact = json.dumps(identity, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return f"social:{hashlib.sha256(compact.encode('utf-8')).hexdigest()}"


def normalize_engagement_metrics(raw: dict[str, Any] | None) -> dict[str, int]:
    payload = raw if isinstance(raw, dict) else {}
    metrics = {
        "likes": payload.get("likes", payload.get("like_count")),
        "comments": payload.get("comments", payload.get("comment_count")),
        "shares": payload.get("shares", payload.get("share_count")),
        "views": payload.get("views", payload.get("view_count")),
        "plays": payload.get("plays", payload.get("play_count")),
        "impressions": payload.get("impressions", payload.get("impression_count")),
        "reactions": payload.get("reactions", payload.get("reaction_count")),
    }
    normalized: dict[str, int] = {}
    for key, value in metrics.items():
        try:
            normalized[key] = max(0, int(value or 0))
        except Exception:
            normalized[key] = 0
    return normalized
