from __future__ import annotations

from typing import Any


def build_pending_source_payload(*, channel_title: str) -> dict[str, Any]:
    return {
        "source_type": "pending",
        "resolution_status": "pending",
        "last_resolution_error": None,
        "channel_title": str(channel_title or "").strip(),
    }


def ensure_resolution_job(writer: Any, channel: dict[str, Any], *, job_kind: str = "resolve_metadata", priority: int = 30) -> dict | None:
    if writer is None or not hasattr(writer, "enqueue_source_resolution_job"):
        return None
    channel_id = str((channel or {}).get("id") or "").strip()
    if not channel_id:
        return None
    return writer.enqueue_source_resolution_job(
        channel_id,
        job_kind=job_kind,
        priority=priority,
    )


def enqueue_missing_peer_ref_backfill(
    writer: Any,
    *,
    session_slot: str = "primary",
    active_only: bool = True,
    limit: int = 100,
    priority: int = 20,
) -> int:
    if writer is None or not hasattr(writer, "list_channels_missing_peer_refs"):
        return 0
    missing = writer.list_channels_missing_peer_refs(
        session_slot=session_slot,
        active_only=active_only,
        limit=limit,
    )
    queued = 0
    for channel in missing:
        created = ensure_resolution_job(
            writer,
            channel,
            job_kind="resolve_metadata",
            priority=priority,
        )
        if created is not None:
            queued += 1
    return queued


async def run_source_resolution_cycle(
    *,
    client: Any,
    writer: Any,
    session_slot: str = "primary",
    max_jobs: int = 10,
    min_interval_seconds: int = 5,
) -> dict[str, Any]:
    del client, session_slot
    lease_seconds = max(30, int(min_interval_seconds) * 2)
    claimed = []
    if writer is not None and hasattr(writer, "claim_due_source_resolution_jobs"):
        claimed = writer.claim_due_source_resolution_jobs(
            limit=max_jobs,
            lease_seconds=lease_seconds,
        )
    return {
        "jobsClaimed": len(claimed or []),
        "jobsCompleted": 0,
        "jobsFailed": 0,
    }
