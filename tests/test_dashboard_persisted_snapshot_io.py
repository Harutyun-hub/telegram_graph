from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from api import server
from api.dashboard_dates import build_dashboard_date_context


class DashboardPersistedSnapshotIoTests(unittest.TestCase):
    def test_load_persisted_dashboard_snapshot_reads_runtime_json(self) -> None:
        ctx = build_dashboard_date_context("2026-03-08", "2026-03-22")
        payload = {
            "from": ctx.from_date.isoformat(),
            "to": ctx.to_date.isoformat(),
            "trustedEndDate": ctx.to_date.isoformat(),
            "snapshotBuiltAt": "2026-03-22T00:00:00+00:00",
            "snapshot": {"communityHealth": {"score": 72}},
            "meta": {"snapshotBuiltAt": "2026-03-22T00:00:00+00:00", "cacheStatus": "refresh_success"},
        }
        writer = SimpleNamespace(
            read_runtime_json=lambda path, prefer_signed_read=False, timeout_seconds=1.5: {
                "status": "ok",
                "payload": payload,
                "elapsed_ms": 12.5,
            }
        )

        with patch.object(server, "get_supabase_writer", return_value=writer):
            loaded = server._load_persisted_dashboard_snapshot("dashboard/snapshots/test.json")

        self.assertEqual(loaded["status"], "hit")
        self.assertEqual(loaded["readMs"], 12.5)
        self.assertEqual(loaded["ctx"].cache_key, ctx.cache_key)
        self.assertEqual(loaded["snapshot"]["communityHealth"]["score"], 72)

    def test_persist_dashboard_snapshot_async_writes_primary_and_alias(self) -> None:
        writes: list[tuple[str, dict]] = []
        writer = SimpleNamespace(
            save_runtime_json_fast=lambda path, payload: writes.append((path, payload)) or True
        )
        ctx = build_dashboard_date_context("2026-03-08", "2026-03-22")
        meta = {"snapshotBuiltAt": "2026-03-22T00:00:00+00:00", "cacheStatus": "refresh_success"}
        snapshot = {"communityHealth": {"score": 72}}

        class _ImmediateThread:
            def __init__(self, *, target=None, name=None, daemon=None):
                self._target = target
                self.name = name
                self.daemon = daemon

            def start(self) -> None:
                if self._target is not None:
                    self._target()

        with patch.object(server, "get_supabase_writer", return_value=writer), \
             patch.object(server.threading, "Thread", _ImmediateThread):
            server._persist_dashboard_snapshot_async(
                ctx,
                snapshot,
                meta,
                trusted_end_date=ctx.to_date.isoformat(),
                write_default_alias=True,
            )

        self.assertEqual(len(writes), 2)
        self.assertEqual(writes[0][0], "dashboard/snapshots/2026-03-08:2026-03-22.json")
        self.assertEqual(writes[1][0], "dashboard/snapshots/default.json")
        self.assertEqual(writes[0][1]["snapshot"]["communityHealth"]["score"], 72)


if __name__ == "__main__":
    unittest.main()
