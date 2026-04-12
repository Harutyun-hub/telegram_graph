from __future__ import annotations

import time
from typing import Any

from loguru import logger

import config


def _usage_int(value: Any, field: str, default: int = 0) -> int:
    if value is None:
        return default
    raw = getattr(value, field, None)
    if raw is None and isinstance(value, dict):
        raw = value.get(field)
    try:
        return int(raw)
    except Exception:
        return default


def _response_id(response: Any) -> str:
    value = getattr(response, "id", None)
    if value is None and isinstance(response, dict):
        value = response.get("id")
    return str(value or "").strip()


def log_openai_usage(
    *,
    feature: str,
    model: str,
    response: Any,
    started_at: float | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    if not bool(getattr(config, "AI_USAGE_TELEMETRY_ENABLED", False)):
        return

    usage = getattr(response, "usage", None)
    if usage is None and isinstance(response, dict):
        usage = response.get("usage")
    if usage is None:
        return

    prompt_tokens = _usage_int(usage, "prompt_tokens")
    completion_tokens = _usage_int(usage, "completion_tokens")
    total_tokens = _usage_int(usage, "total_tokens")
    prompt_details = getattr(usage, "prompt_tokens_details", None)
    if prompt_details is None and isinstance(usage, dict):
        prompt_details = usage.get("prompt_tokens_details")
    cached_tokens = _usage_int(prompt_details, "cached_tokens")

    parts = [
        "AI_USAGE",
        f"feature={str(feature or 'unknown').strip()}",
        f"model={str(model or 'unknown').strip()}",
        f"prompt={prompt_tokens}",
        f"completion={completion_tokens}",
        f"total={total_tokens}",
        f"cached={cached_tokens}",
    ]

    if started_at is not None:
        latency_ms = max(0, int(round((time.perf_counter() - started_at) * 1000)))
        parts.append(f"latency_ms={latency_ms}")

    response_id = _response_id(response)
    if response_id:
        parts.append(f"response_id={response_id}")

    for key, value in (extra or {}).items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parts.append(f"{key}={text}")

    logger.info(" | ".join(parts))
