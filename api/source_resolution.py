from __future__ import annotations

import asyncio
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger
from telethon.errors import ChannelPrivateError, FloodWaitError
from telethon.tl.types import Channel

import config
from scraper.channel_metadata import channel_peer_ref_from_entity, resolve_source_metadata

RESOLUTION_JOB_KIND = "resolve_metadata"
RESOLUTION_STATUS_PENDING = "pending"
RESOLUTION_STATUS_RESOLVED = "resolved"
RESOLUTION_STATUS_ERROR = "error"
JOB_STATUS_PENDING = "pending"
JOB_STATUS_LEASED = "leased"
JOB_STATUS_DONE = "done"
JOB_STATUS_DEAD_LETTER = "dead_letter"
SESSION_SLOT_PRIMARY = "primary"

ERROR_CODE_FLOOD_WAIT = "flood_wait"
ERROR_CODE_USERNAME_MISSING = "username_missing"
ERROR_CODE_USERNAME_UNACCEPTABLE = "username_unacceptable"
ERROR_CODE_CHANNEL_PRIVATE = "channel_private"
ERROR_CODE_UNSUPPORTED_PEER = "unsupported_peer"
ERROR_CODE_TRANSIENT = "transient_error"

_USERNAME_MISSING_RE = re.compile(r'^No user has ".+" as username\.?$', re.IGNORECASE)
_USERNAME_UNACCEPTABLE_RE = re.compile(
    r"^Nobody is using this username, or the username is unacceptable\.",
    re.IGNORECASE,
)
_ENTITY_NOT_FOUND_RE = re.compile(r"cannot find any entity corresponding", re.IGNORECASE)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def resolution_job_priority(channel_row: dict[str, Any], *, job_kind: str = RESOLUTION_JOB_KIND) -> int:
    del job_kind
    is_active = bool(channel_row.get("is_active", False))
    resolution_status = str(channel_row.get("resolution_status") or "").strip().lower()
    if is_active and resolution_status == RESOLUTION_STATUS_PENDING:
        return 10
    if is_active:
        return 20
    return 30


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
        "resolution_status": RESOLUTION_STATUS_PENDING,
        "last_resolution_error": (error_message or None),
        "resolution_error_code": (error_code or None),
        "resolution_retry_after_at": _iso(retry_after_at),
        "channel_title": channel_title,
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
    writer.ensure_source_resolution_slot(
        SESSION_SLOT_PRIMARY,
        min_resolve_interval_seconds=config.SOURCE_RESOLUTION_MIN_INTERVAL_SECONDS,
        max_concurrent_resolves=1,
    )
    return writer.enqueue_source_resolution_job(
        str(channel_row.get("id") or ""),
        job_kind=job_kind,
        priority=int(priority if priority is not None else resolution_job_priority(channel_row, job_kind=job_kind)),
    )


def enqueue_missing_peer_ref_backfill(
    writer,
    *,
    session_slot: str = SESSION_SLOT_PRIMARY,
    active_only: bool = True,
    limit: int | None = None,
) -> int:
    count = 0
    for channel in writer.list_channels_missing_peer_refs(
        session_slot=session_slot,
        active_only=active_only,
        limit=limit or config.SOURCE_RESOLUTION_MAX_JOBS_PER_RUN,
    ):
        job = ensure_resolution_job(writer, channel, priority=20)
        if job:
            count += 1
    return count


def classify_resolution_exception(exc: Exception) -> dict[str, Any]:
    text = str(exc).strip()
    if isinstance(exc, FloodWaitError):
        return {
            "kind": "flood_wait",
            "retryable": True,
            "code": ERROR_CODE_FLOOD_WAIT,
            "message": text,
            "seconds": int(getattr(exc, "seconds", 0) or 0),
            "auto_pause": False,
        }
    if isinstance(exc, ChannelPrivateError):
        return {
            "kind": "permanent",
            "retryable": False,
            "code": ERROR_CODE_CHANNEL_PRIVATE,
            "message": text,
            "seconds": None,
            "auto_pause": True,
        }
    if _USERNAME_MISSING_RE.match(text) or _ENTITY_NOT_FOUND_RE.search(text):
        return {
            "kind": "permanent",
            "retryable": False,
            "code": ERROR_CODE_USERNAME_MISSING,
            "message": text,
            "seconds": None,
            "auto_pause": False,
        }
    if _USERNAME_UNACCEPTABLE_RE.match(text):
        return {
            "kind": "permanent",
            "retryable": False,
            "code": ERROR_CODE_USERNAME_UNACCEPTABLE,
            "message": text,
            "seconds": None,
            "auto_pause": False,
        }
    if "resolved peer is" in text.lower() or "unsupported telegram peer type" in text.lower():
        return {
            "kind": "permanent",
            "retryable": False,
            "code": ERROR_CODE_UNSUPPORTED_PEER,
            "message": text,
            "seconds": None,
            "auto_pause": True,
        }
    return {
        "kind": "transient",
        "retryable": True,
        "code": ERROR_CODE_TRANSIENT,
        "message": text,
        "seconds": None,
        "auto_pause": False,
    }


def compute_retry_at(*, attempt_count: int, flood_wait_seconds: int | None = None) -> datetime:
    now = utc_now()
    if flood_wait_seconds is not None and flood_wait_seconds > 0:
        return now + timedelta(seconds=int(flood_wait_seconds) + random.randint(5, 30))
    base_seconds = min(
        int(config.SOURCE_RESOLUTION_RETRY_MAX_SECONDS),
        int(60 * (2 ** min(max(1, int(attempt_count)), 8))),
    )
    return now + timedelta(seconds=base_seconds + random.randint(10, 60))


async def resolve_source_job(
    *,
    client,
    writer,
    channel_row: dict[str, Any],
    job_row: dict[str, Any],
    session_slot: str = SESSION_SLOT_PRIMARY,
) -> dict[str, Any]:
    channel_id = str(channel_row.get("id") or "").strip()
    username = str(channel_row.get("channel_username") or "").strip()
    fallback_title = (channel_row.get("channel_title") or username or "").strip()
    attempt_count = int(job_row.get("attempt_count") or 0) + 1
    now = utc_now()
    now_iso = now.isoformat()

    try:
        entity = await client.get_entity(username)
        if not isinstance(entity, Channel):
            raise ValueError(f"resolved peer is {type(entity).__name__}, not a Telegram channel/supergroup")

        metadata, resolved_entity = await resolve_source_metadata(client, username=username, entity=entity)
        if not metadata.get("channel_title") and fallback_title:
            metadata["channel_title"] = fallback_title

        writer.upsert_channel_peer_ref(
            channel_id,
            session_slot,
            {
                **channel_peer_ref_from_entity(resolved_entity, username=username),
                "resolved_at": now_iso,
                "last_verified_at": now_iso,
            },
        )
        metadata.update(
            {
                "resolution_status": RESOLUTION_STATUS_RESOLVED,
                "last_resolution_error": None,
                "resolution_error_code": None,
                "resolution_last_attempt_at": now_iso,
                "resolution_attempt_count": attempt_count,
                "resolution_retry_after_at": None,
            }
        )
        writer.update_channel(channel_id, metadata)
        writer.complete_source_resolution_job(str(job_row.get("id") or ""), attempt_count=attempt_count)
        writer.update_source_resolution_slot(
            session_slot,
            {
                "last_dispatch_at": now_iso,
                "last_success_at": now_iso,
                "cooldown_until": None,
                "last_flood_wait_seconds": None,
            },
        )
        return {
            "status": "resolved",
            "channel_id": channel_id,
            "attempt_count": attempt_count,
            "job_status": JOB_STATUS_DONE,
        }
    except Exception as exc:
        classified = classify_resolution_exception(exc)
        error_message = classified["message"][:500]

        if classified["code"] == ERROR_CODE_FLOOD_WAIT:
            retry_at = compute_retry_at(attempt_count=attempt_count, flood_wait_seconds=classified["seconds"])
            writer.update_channel(
                channel_id,
                {
                    "resolution_status": RESOLUTION_STATUS_PENDING,
                    "last_resolution_error": error_message,
                    "resolution_error_code": ERROR_CODE_FLOOD_WAIT,
                    "resolution_last_attempt_at": now_iso,
                    "resolution_attempt_count": attempt_count,
                    "resolution_retry_after_at": retry_at.isoformat(),
                },
            )
            writer.requeue_source_resolution_job(
                str(job_row.get("id") or ""),
                attempt_count=attempt_count,
                next_attempt_at=retry_at.isoformat(),
                last_error_code=ERROR_CODE_FLOOD_WAIT,
                last_error_message=error_message,
            )
            writer.update_source_resolution_slot(
                session_slot,
                {
                    "last_dispatch_at": now_iso,
                    "cooldown_until": retry_at.isoformat(),
                    "last_flood_wait_seconds": int(classified["seconds"] or 0),
                    "last_error_at": now_iso,
                },
            )
            return {
                "status": "flood_wait",
                "channel_id": channel_id,
                "attempt_count": attempt_count,
                "retry_at": retry_at.isoformat(),
                "job_status": JOB_STATUS_PENDING,
                "error_code": ERROR_CODE_FLOOD_WAIT,
            }

        if not classified["retryable"]:
            payload = {
                "resolution_status": RESOLUTION_STATUS_ERROR,
                "last_resolution_error": error_message,
                "resolution_error_code": classified["code"],
                "resolution_last_attempt_at": now_iso,
                "resolution_attempt_count": attempt_count,
                "resolution_retry_after_at": None,
            }
            if classified["code"] == ERROR_CODE_UNSUPPORTED_PEER:
                payload["source_type"] = "pending"
            if classified["auto_pause"]:
                payload["is_active"] = False
            writer.update_channel(channel_id, payload)
            writer.dead_letter_source_resolution_job(
                str(job_row.get("id") or ""),
                attempt_count=attempt_count,
                last_error_code=classified["code"],
                last_error_message=error_message,
            )
            writer.update_source_resolution_slot(
                session_slot,
                {
                    "last_dispatch_at": now_iso,
                    "last_error_at": now_iso,
                },
            )
            return {
                "status": "dead_letter",
                "channel_id": channel_id,
                "attempt_count": attempt_count,
                "job_status": JOB_STATUS_DEAD_LETTER,
                "error_code": classified["code"],
            }

        retry_at = compute_retry_at(attempt_count=attempt_count)
        writer.update_channel(
            channel_id,
            {
                "resolution_status": RESOLUTION_STATUS_PENDING,
                "last_resolution_error": error_message,
                "resolution_error_code": classified["code"],
                "resolution_last_attempt_at": now_iso,
                "resolution_attempt_count": attempt_count,
                "resolution_retry_after_at": retry_at.isoformat(),
            },
        )
        writer.requeue_source_resolution_job(
            str(job_row.get("id") or ""),
            attempt_count=attempt_count,
            next_attempt_at=retry_at.isoformat(),
            last_error_code=classified["code"],
            last_error_message=error_message,
        )
        writer.update_source_resolution_slot(
            session_slot,
            {
                "last_dispatch_at": now_iso,
                "last_error_at": now_iso,
            },
        )
        return {
            "status": "requeued",
            "channel_id": channel_id,
            "attempt_count": attempt_count,
            "retry_at": retry_at.isoformat(),
            "job_status": JOB_STATUS_PENDING,
            "error_code": classified["code"],
        }


async def run_source_resolution_cycle(
    *,
    client,
    writer,
    session_slot: str = SESSION_SLOT_PRIMARY,
    max_jobs: int | None = None,
    min_interval_seconds: int | None = None,
) -> dict[str, Any]:
    slot = writer.ensure_source_resolution_slot(
        session_slot,
        min_resolve_interval_seconds=min_interval_seconds or config.SOURCE_RESOLUTION_MIN_INTERVAL_SECONDS,
        max_concurrent_resolves=1,
    )
    slot = writer.get_source_resolution_slot(session_slot) or slot or {"slot_key": session_slot}

    now = utc_now()
    cooldown_until = slot.get("cooldown_until")
    cooldown_dt = None
    if cooldown_until:
        try:
            cooldown_dt = datetime.fromisoformat(str(cooldown_until).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            cooldown_dt = None
    if cooldown_dt and cooldown_dt > now:
        return {
            "status": "cooldown",
            "slot_key": session_slot,
            "cooldown_until": cooldown_dt.isoformat(),
            "jobs_claimed": 0,
            "jobs_processed": 0,
            "jobs_resolved": 0,
            "jobs_requeued": 0,
            "jobs_dead_lettered": 0,
        }

    claimed_jobs = writer.claim_due_source_resolution_jobs(
        limit=max(1, int(max_jobs or config.SOURCE_RESOLUTION_MAX_JOBS_PER_RUN)),
        lease_seconds=max(30, int(config.SOURCE_RESOLUTION_LEASE_SECONDS)),
    )
    if not claimed_jobs:
        return {
            "status": "idle",
            "slot_key": session_slot,
            "cooldown_until": None,
            "jobs_claimed": 0,
            "jobs_processed": 0,
            "jobs_resolved": 0,
            "jobs_requeued": 0,
            "jobs_dead_lettered": 0,
        }

    channels = writer.get_channels_by_ids([str(job.get("channel_id") or "") for job in claimed_jobs])
    processed = 0
    resolved = 0
    requeued = 0
    dead_lettered = 0
    halted_for_cooldown = False
    min_interval = max(
        1,
        int(min_interval_seconds or slot.get("min_resolve_interval_seconds") or config.SOURCE_RESOLUTION_MIN_INTERVAL_SECONDS),
    )
    results: list[dict[str, Any]] = []

    for index, job in enumerate(claimed_jobs):
        if index > 0:
            await asyncio.sleep(min_interval)

        channel_id = str(job.get("channel_id") or "").strip()
        channel_row = channels.get(channel_id)
        if not channel_row:
            writer.complete_source_resolution_job(
                str(job.get("id") or ""),
                attempt_count=int(job.get("attempt_count") or 0),
            )
            continue

        outcome = await resolve_source_job(
            client=client,
            writer=writer,
            channel_row=channel_row,
            job_row=job,
            session_slot=session_slot,
        )
        processed += 1
        results.append(outcome)
        if outcome["status"] == "resolved":
            resolved += 1
        elif outcome["status"] == "dead_letter":
            dead_lettered += 1
        else:
            requeued += 1

        if outcome["status"] == "flood_wait":
            halted_for_cooldown = True
            logger.warning(
                "Source resolution cooldown engaged for slot {} until {}",
                session_slot,
                outcome.get("retry_at"),
            )
            break

    snapshot = writer.get_source_resolution_snapshot(session_slot=session_slot)
    return {
        "status": "cooldown" if halted_for_cooldown else "completed",
        "slot_key": session_slot,
        "cooldown_until": snapshot.get("cooldown_until"),
        "jobs_claimed": len(claimed_jobs),
        "jobs_processed": processed,
        "jobs_resolved": resolved,
        "jobs_requeued": requeued,
        "jobs_dead_lettered": dead_lettered,
        "halted_for_cooldown": halted_for_cooldown,
        "results": results,
        "snapshot": snapshot,
    }
