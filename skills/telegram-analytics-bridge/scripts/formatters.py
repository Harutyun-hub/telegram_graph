from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from models import SCHEMA_VERSION


MAX_TELEGRAM_BULLETS = 6
MAX_TELEGRAM_CHARS = 900


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _trim_text(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split()).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def build_telegram_text(summary: str, bullets: list[str], caveat: str | None = None) -> str:
    lines: list[str] = []
    if summary:
        lines.append(_trim_text(summary, 220))
    for bullet in bullets[:MAX_TELEGRAM_BULLETS]:
        clean = _trim_text(bullet, 180)
        if clean:
            lines.append(f"- {clean}")
    if caveat:
        lines.append(f"Caveat: {_trim_text(caveat, 180)}")
    text = "\n".join(lines).strip()
    return _trim_text(text, MAX_TELEGRAM_CHARS)


def build_success(
    *,
    action: str,
    window: str | None,
    summary: str,
    confidence: str,
    bullets: list[str],
    items: list[dict[str, Any]],
    source_endpoints: list[str],
    caveat: str | None = None,
    suggested_follow_up: str | None = None,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "ok": True,
        "action": action,
        "window": window,
        "summary": _trim_text(summary, 280),
        "confidence": confidence,
        "bullets": [_trim_text(item, 180) for item in bullets[:MAX_TELEGRAM_BULLETS]],
        "items": items,
        "telegram_text": build_telegram_text(summary, bullets, caveat=caveat),
        "source_endpoints": source_endpoints,
    }
    if caveat:
        response["caveat"] = _trim_text(caveat, 220)
    if suggested_follow_up:
        response["suggested_follow_up"] = _trim_text(suggested_follow_up, 180)
    return response


def build_error(
    *,
    action: str,
    message: str,
    error_type: str,
    window: str | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now_iso(),
        "ok": False,
        "action": action,
        "window": window,
        "error_type": error_type,
        "message": _trim_text(message, 240),
    }
