from __future__ import annotations

import ast
import json
import re
from typing import Any


READABLE_TEXT_FIELDS = (
    "text",
    "caption",
    "message",
    "description",
    "ad_text",
    "body",
    "title",
    "link_description",
    "snapshot",
    "snapshot_data",
)


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def looks_like_raw_social_payload(value: Any) -> bool:
    text = _trimmed(value)
    if not text:
        return False
    starts_structured = (text.startswith("{") and text.endswith("}")) or (text.startswith("[") and text.endswith("]"))
    if starts_structured and (": " in text or "\":" in text or "':" in text):
        return True
    return any(marker in text[:500] for marker in ("'strong_id__'", '"strong_id__"', "'provider_item_id'", '"provider_item_id"'))


def _parse_structured_text(value: str) -> Any | None:
    if not looks_like_raw_social_payload(value):
        return None
    for parser in (json.loads, ast.literal_eval):
        try:
            return parser(value)
        except Exception:
            continue
    return None


def extract_readable_social_text(value: Any, *, _depth: int = 0) -> str:
    if value is None or _depth > 6:
        return ""

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        parsed = _parse_structured_text(text)
        if parsed is not None:
            return extract_readable_social_text(parsed, _depth=_depth + 1)
        if looks_like_raw_social_payload(text):
            return ""
        return _normalize_text(text)

    if isinstance(value, dict):
        for key in READABLE_TEXT_FIELDS:
            text = extract_readable_social_text(value.get(key), _depth=_depth + 1)
            if text:
                return text
        return ""

    if isinstance(value, list):
        for item in value[:8]:
            text = extract_readable_social_text(item, _depth=_depth + 1)
            if text:
                return text
        return ""

    return ""


def clean_social_text_content(
    text_content: Any,
    *,
    provider_payload: Any | None = None,
    analysis: dict[str, Any] | None = None,
) -> str | None:
    text = extract_readable_social_text(text_content)
    if text:
        return text

    text = extract_readable_social_text(provider_payload)
    if text:
        return text

    analysis_payload = analysis.get("analysis_payload") if isinstance(analysis, dict) else None
    if isinstance(analysis_payload, dict):
        text = extract_readable_social_text(analysis_payload.get("summary"))
        if text:
            return text

    if isinstance(analysis, dict):
        text = extract_readable_social_text(analysis.get("summary"))
        if text:
            return text

    return None


def clean_social_activity_row(row: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(row)
    cleaned["text_content"] = clean_social_text_content(
        cleaned.get("text_content"),
        provider_payload=cleaned.get("provider_payload"),
        analysis=cleaned.get("analysis") if isinstance(cleaned.get("analysis"), dict) else None,
    )
    return cleaned
