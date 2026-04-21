from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


RESOLUTION_JOB_KIND = "resolve_metadata"
SESSION_SLOT_PRIMARY = "primary"


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def build_pending_source_payload(
    *,
    channel_title: str,
    error_code: str | None = None,
    error_message: str | None = None,
    retry_after_at: datetime | None = None,
    attempt_count: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source_type": "pending",
        "resolution_status": "pending",
        "channel_title": channel_title,
        "last_resolution_error": error_message,
        "resolution_error_code": error_code,
        "resolution_retry_after_at": _iso(retry_after_at),
    }
    if attempt_count is not None:
        payload["resolution_attempt_count"] = int(attempt_count)
    return payload


def ensure_resolution_job(
    writer,
    channel_row: dict[str, Any],
    *,
    job_kind: str = RESOLUTION_JOB_KIND,
    priority: int | None = None,
) -> dict[str, Any] | None:
    if not channel_row:
        return None
    enqueue = getattr(writer, "enqueue_source_resolution_job", None)
    if not callable(enqueue):
        return None
    return enqueue(
        str(channel_row.get("id") or ""),
        job_kind=job_kind,
        priority=int(priority if priority is not None else 20),
    )


def enqueue_missing_peer_ref_backfill(
    writer,
    *,
    session_slot: str = SESSION_SLOT_PRIMARY,
    active_only: bool = True,
    limit: int | None = None,
) -> int:
    del session_slot
    list_missing = getattr(writer, "list_channels_missing_peer_refs", None)
    if not callable(list_missing):
        return 0
    queued = 0
    for channel in list_missing(active_only=active_only, limit=limit):
        if ensure_resolution_job(writer, channel, priority=20):
            queued += 1
    return queued


async def run_source_resolution_cycle(
    *,
    client,
    writer,
    session_slot: str = SESSION_SLOT_PRIMARY,
) -> dict[str, Any]:
    del client, writer
    return {
        "status": "disabled",
        "processed": 0,
        "queued": 0,
        "session_slot": session_slot,
    }
