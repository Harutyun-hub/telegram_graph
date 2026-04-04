from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from telethon.errors import FloodWaitError

from api import freshness, server
from api.source_resolution import (
    ERROR_CODE_FLOOD_WAIT,
    ERROR_CODE_USERNAME_UNACCEPTABLE,
    classify_resolution_exception,
    run_source_resolution_cycle,
)


class _FakeResolutionChannel:
    def __init__(self, *, channel_id: int = 101, access_hash: int = 202, username: str = "resolved_source") -> None:
        self.id = channel_id
        self.access_hash = access_hash
        self.username = username
        self.title = "Resolved Source"
        self.participants_count = 42
        self.broadcast = True
        self.megagroup = False
        self.gigagroup = False
        self.forum = False


class _FakeServerWriter:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.by_handle: dict[str, dict] = {}
        self.queued_jobs: list[dict] = []
        self.slots: dict[str, dict] = {}
        self.created_rows: list[dict] = []
        self.missing_peer_ref_rows: list[dict] = []

    def get_channel_by_handle(self, handle: str):
        return self.by_handle.get(str(handle).strip().lower())

    def create_channel(self, payload: dict) -> dict:
        row = {"id": "chan-created", **payload}
        self.rows[row["id"]] = dict(row)
        self.by_handle[str(payload.get("channel_username") or "").strip().lower().lstrip("@")] = row
        self.created_rows.append(dict(row))
        return dict(row)

    def update_channel(self, channel_uuid: str, payload: dict):
        row = dict(self.rows.get(channel_uuid, {"id": channel_uuid}))
        row.update(payload)
        self.rows[channel_uuid] = row
        handle = str(row.get("channel_username") or "").strip().lower().lstrip("@")
        if handle:
            self.by_handle[handle] = row
        return dict(row)

    def get_channel_by_id(self, channel_uuid: str):
        return dict(self.rows.get(channel_uuid, {"id": channel_uuid}))

    def ensure_source_resolution_slot(self, slot_key="primary", **payload):
        row = {"slot_key": slot_key, **payload}
        self.slots[str(slot_key)] = row
        return row

    def enqueue_source_resolution_job(self, channel_uuid: str, **payload):
        job = {"id": f"job-{len(self.queued_jobs) + 1}", "channel_id": channel_uuid, **payload}
        self.queued_jobs.append(job)
        return job

    def get_source_resolution_job(self, channel_uuid: str, job_kind: str = "resolve_metadata"):
        for job in self.queued_jobs:
            if job["channel_id"] == channel_uuid and job.get("job_kind", "resolve_metadata") == job_kind:
                return dict(job)
        return None

    def list_channels_missing_peer_refs(self, *, session_slot="primary", active_only=True, limit=100):
        del session_slot, active_only
        return [dict(row) for row in self.missing_peer_ref_rows[:limit]]


class _WorkerWriter:
    def __init__(self) -> None:
        self.slot = {
            "slot_key": "primary",
            "is_active": True,
            "priority": 100,
            "cooldown_until": None,
            "min_resolve_interval_seconds": 1,
            "max_concurrent_resolves": 1,
        }
        self.job = {
            "id": "job-1",
            "channel_id": "chan-1",
            "attempt_count": 0,
            "status": "pending",
            "next_attempt_at": datetime.now(timezone.utc).isoformat(),
        }
        self.channel = {
            "id": "chan-1",
            "channel_username": "@flooded_source",
            "channel_title": "Flooded Source",
            "is_active": True,
            "resolution_status": "pending",
        }
        self.requeued: list[dict] = []
        self.completed: list[dict] = []
        self.dead_lettered: list[dict] = []
        self.channel_updates: list[tuple[str, dict]] = []
        self.slot_updates: list[dict] = []
        self.peer_refs: list[dict] = []

    def ensure_source_resolution_slot(self, slot_key="primary", **_payload):
        return dict(self.slot)

    def get_source_resolution_slot(self, slot_key="primary"):
        return dict(self.slot) if slot_key == "primary" else None

    def claim_due_source_resolution_jobs(self, *, limit: int, lease_seconds: int):
        del limit, lease_seconds
        return [dict(self.job)]

    def get_channels_by_ids(self, channel_ids: list[str]):
        return {self.channel["id"]: dict(self.channel)} if self.channel["id"] in channel_ids else {}

    def upsert_channel_peer_ref(self, channel_uuid: str, session_slot: str, payload: dict):
        row = {"channel_id": channel_uuid, "session_slot": session_slot, **payload}
        self.peer_refs.append(row)
        return row

    def update_channel(self, channel_uuid: str, payload: dict):
        self.channel_updates.append((channel_uuid, dict(payload)))
        if channel_uuid == self.channel["id"]:
            self.channel.update(payload)
        return dict(self.channel)

    def complete_source_resolution_job(self, job_id: str, *, attempt_count: int | None = None):
        self.completed.append({"job_id": job_id, "attempt_count": attempt_count})
        return {"id": job_id, "status": "done"}

    def requeue_source_resolution_job(self, job_id: str, **payload):
        row = {"job_id": job_id, **payload}
        self.requeued.append(row)
        return row

    def dead_letter_source_resolution_job(self, job_id: str, **payload):
        row = {"job_id": job_id, **payload}
        self.dead_lettered.append(row)
        return row

    def update_source_resolution_slot(self, slot_key: str, payload: dict):
        self.slot.update(payload)
        self.slot_updates.append(dict(payload))
        return dict(self.slot)

    def get_source_resolution_snapshot(self, *, session_slot: str = "primary"):
        del session_slot
        return {
            "slot_key": "primary",
            "due_jobs": 0,
            "leased_jobs": 0,
            "dead_letter_jobs": len(self.dead_lettered),
            "cooldown_slots": 1 if self.slot.get("cooldown_until") else 0,
            "cooldown_until": self.slot.get("cooldown_until"),
            "oldest_due_age_seconds": None,
            "active_pending_sources": 1,
            "active_missing_peer_refs": 0,
        }


class _FakeFloodClient:
    async def get_entity(self, _username):
        raise FloodWaitError(request=None, capture=120)


class SourceResolutionServerTests(unittest.TestCase):
    def test_create_channel_source_enqueues_without_inline_resolution_when_queue_enabled(self) -> None:
        writer = _FakeServerWriter()
        payload = server.ChannelSourceCreateRequest(channel_username="@queued_source", channel_title="Queued Source")

        with patch.object(server, "get_supabase_writer", return_value=writer), patch.object(
            server.config, "FEATURE_SOURCE_RESOLUTION_QUEUE", True
        ), patch.object(server, "_try_enrich_channel_metadata", AsyncMock()) as inline_resolve:
            result = asyncio.run(server.create_channel_source(payload))

        self.assertEqual(result["action"], "created")
        self.assertEqual(len(writer.queued_jobs), 1)
        self.assertEqual(writer.queued_jobs[0]["channel_id"], "chan-created")
        inline_resolve.assert_not_awaited()

    def test_update_channel_source_does_not_requeue_resolved_source(self) -> None:
        writer = _FakeServerWriter()
        writer.rows["chan-1"] = {
            "id": "chan-1",
            "channel_username": "@resolved_source",
            "channel_title": "Resolved Source",
            "is_active": False,
            "resolution_status": "resolved",
        }
        payload = server.ChannelSourceUpdateRequest(is_active=True)

        with patch.object(server, "get_supabase_writer", return_value=writer), patch.object(
            server.config, "FEATURE_SOURCE_RESOLUTION_QUEUE", True
        ), patch.object(server, "_try_enrich_channel_metadata", AsyncMock()) as inline_resolve:
            result = asyncio.run(server.update_channel_source("chan-1", payload))

        self.assertTrue(result["item"]["is_active"])
        self.assertEqual(writer.queued_jobs, [])
        inline_resolve.assert_not_awaited()

    def test_create_channel_source_reactivates_unresolved_existing_source_and_queues(self) -> None:
        writer = _FakeServerWriter()
        writer.rows["chan-existing"] = {
            "id": "chan-existing",
            "channel_username": "@queued_existing",
            "channel_title": "Queued Existing",
            "is_active": False,
            "resolution_status": "error",
            "resolution_error_code": "username_missing",
            "last_resolution_error": "old error",
        }
        writer.by_handle["queued_existing"] = dict(writer.rows["chan-existing"])
        payload = server.ChannelSourceCreateRequest(channel_username="@queued_existing", channel_title="Queued Existing")

        with patch.object(server, "get_supabase_writer", return_value=writer), patch.object(
            server.config, "FEATURE_SOURCE_RESOLUTION_QUEUE", True
        ), patch.object(server, "_try_enrich_channel_metadata", AsyncMock()) as inline_resolve:
            result = asyncio.run(server.create_channel_source(payload))

        self.assertEqual(result["action"], "reactivated")
        self.assertTrue(result["item"]["is_active"])
        self.assertEqual(result["item"]["resolution_status"], "pending")
        self.assertIsNone(result["item"].get("resolution_error_code"))
        self.assertEqual(len(writer.queued_jobs), 1)
        self.assertEqual(writer.queued_jobs[0]["channel_id"], "chan-existing")
        inline_resolve.assert_not_awaited()

    def test_backfill_peer_ref_endpoint_queues_missing_sources(self) -> None:
        writer = _FakeServerWriter()
        writer.missing_peer_ref_rows = [
            {
                "id": "chan-2",
                "channel_username": "@needs_peer_ref",
                "channel_title": "Needs Peer Ref",
                "is_active": True,
                "resolution_status": "resolved",
            }
        ]

        class _Scheduler:
            def status(self):
                return {"resolution": {"snapshot": {"active_missing_peer_refs": 1}}}

        with patch.object(server, "get_supabase_writer", return_value=writer), patch.object(
            server, "get_scraper_scheduler", return_value=_Scheduler()
        ):
            result = asyncio.run(server.backfill_source_peer_refs())

        self.assertEqual(result["queued"], 1)
        self.assertEqual(len(writer.queued_jobs), 1)
        self.assertEqual(writer.queued_jobs[0]["channel_id"], "chan-2")


class SourceResolutionWorkerTests(unittest.TestCase):
    def test_classify_resolution_exception_handles_unacceptable_username(self) -> None:
        classified = classify_resolution_exception(
            ValueError(
                'Nobody is using this username, or the username is unacceptable. If the latter, it must match r"[a-zA-Z][\\w\\d]{3,30}[a-zA-Z\\d]" (caused by ResolveUsernameRequest)'
            )
        )

        self.assertEqual(classified["code"], ERROR_CODE_USERNAME_UNACCEPTABLE)
        self.assertFalse(classified["retryable"])

    def test_run_source_resolution_cycle_requeues_flood_wait_with_cooldown(self) -> None:
        writer = _WorkerWriter()
        client = _FakeFloodClient()

        result = asyncio.run(
            run_source_resolution_cycle(
                client=client,
                writer=writer,
                session_slot="primary",
                max_jobs=1,
                min_interval_seconds=1,
            )
        )

        self.assertEqual(result["status"], "cooldown")
        self.assertEqual(result["jobs_requeued"], 1)
        self.assertEqual(writer.requeued[0]["last_error_code"], ERROR_CODE_FLOOD_WAIT)
        self.assertEqual(writer.channel_updates[-1][1]["resolution_error_code"], ERROR_CODE_FLOOD_WAIT)
        self.assertIsNotNone(writer.slot.get("cooldown_until"))


class FreshnessResolutionMetricsTests(unittest.TestCase):
    def test_freshness_snapshot_includes_resolution_metrics(self) -> None:
        class _Writer:
            def get_pipeline_freshness_snapshot(self):
                return {
                    "active_channels": 2,
                    "active_channels_never_scraped": 0,
                    "last_scrape_at": datetime.now(timezone.utc).isoformat(),
                    "last_post_at": datetime.now(timezone.utc).isoformat(),
                    "last_process_at": datetime.now(timezone.utc).isoformat(),
                    "last_graph_sync_at": datetime.now(timezone.utc).isoformat(),
                    "unprocessed_posts": 0,
                    "unprocessed_comments": 0,
                    "unsynced_posts": 0,
                    "unsynced_analysis": 0,
                    "dead_letter_scopes": 0,
                    "retry_blocked_scopes": 0,
                }

            def get_source_resolution_snapshot(self, *, session_slot="primary"):
                del session_slot
                return {
                    "slot_key": "primary",
                    "due_jobs": 7,
                    "leased_jobs": 1,
                    "dead_letter_jobs": 2,
                    "cooldown_slots": 1,
                    "cooldown_until": datetime.now(timezone.utc).isoformat(),
                    "oldest_due_age_seconds": 45,
                    "active_pending_sources": 3,
                    "active_missing_peer_refs": 2,
                }

            def get_recent_pipeline_snapshot(self):
                return {
                    "window_days": 15,
                    "window_start_at": datetime.now(timezone.utc).isoformat(),
                    "recent_posts": 0,
                    "recent_comments": 0,
                    "recent_unsynced_posts": 0,
                    "recent_last_post_at": datetime.now(timezone.utc).isoformat(),
                    "recent_last_graph_sync_post_at": datetime.now(timezone.utc).isoformat(),
                }

        writer = _Writer()

        with patch.object(
            freshness,
            "_neo4j_snapshot",
            return_value={"recent_post_count": 0, "channel_count": 0, "topic_count": 0},
        ):
            snapshot = freshness.get_freshness_snapshot(
                writer,
                scheduler_status={"is_active": True, "interval_minutes": 15, "running_now": False, "run_history": []},
                force_refresh=True,
            )

        self.assertEqual(snapshot["backlog"]["resolution_due_jobs"], 7)
        self.assertEqual(snapshot["backlog"]["resolution_leased_jobs"], 1)
        self.assertEqual(snapshot["backlog"]["resolution_dead_letter_jobs"], 2)
        self.assertEqual(snapshot["backlog"]["active_pending_sources"], 3)
        self.assertEqual(snapshot["resolution"]["cooldown_slots"], 1)


if __name__ == "__main__":
    unittest.main()
