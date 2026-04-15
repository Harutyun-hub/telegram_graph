from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from api import server


class FreshnessApiFlowTests(unittest.TestCase):
    def test_resolve_freshness_snapshot_uses_fresh_memory_cache(self) -> None:
        cached_snapshot = {"generated_at": "2026-04-13T10:00:00+00:00", "health": {"status": "healthy"}}
        cached_at = datetime.now(timezone.utc) - timedelta(seconds=5)

        with patch.object(server, "get_cached_freshness_snapshot", return_value=(cached_snapshot, cached_at)), \
             patch.object(server, "freshness_cache_ttl_seconds", return_value=300), \
             patch.object(server, "_enqueue_default_dashboard_refresh_if_needed", return_value={"enqueued": True}) as enqueue_mock, \
             patch.object(server.asyncio, "get_running_loop", side_effect=AssertionError("executor should not run")):
            snapshot = asyncio.run(server._resolve_freshness_snapshot(force_refresh=False))

        self.assertEqual(snapshot, cached_snapshot)
        enqueue_mock.assert_called_once_with(cached_snapshot, reason="freshness_memory_hit")

    def test_load_current_freshness_snapshot_prefers_shared_snapshot_for_normal_reads(self) -> None:
        writer = object()
        scheduler_status = {"is_active": True}

        with patch.object(server, "get_supabase_writer", return_value=writer), \
             patch.object(server, "get_current_scraper_scheduler_status", return_value=scheduler_status), \
             patch.object(server, "get_freshness_snapshot", return_value={"generated_at": "2026-04-13T10:00:00+00:00"}) as snapshot_mock:
            snapshot = server._load_current_freshness_snapshot(force_refresh=False)

        self.assertEqual(snapshot["generated_at"], "2026-04-13T10:00:00+00:00")
        snapshot_mock.assert_called_once_with(
            writer,
            scheduler_status=scheduler_status,
            force_refresh=False,
            prefer_shared_snapshot=True,
        )

    def test_load_current_freshness_snapshot_force_refresh_bypasses_shared_snapshot(self) -> None:
        writer = object()
        scheduler_status = {"is_active": True}

        with patch.object(server, "get_supabase_writer", return_value=writer), \
             patch.object(server, "get_current_scraper_scheduler_status", return_value=scheduler_status), \
             patch.object(server, "get_freshness_snapshot", return_value={"generated_at": "2026-04-13T10:00:00+00:00"}) as snapshot_mock:
            snapshot = server._load_current_freshness_snapshot(force_refresh=True)

        self.assertEqual(snapshot["generated_at"], "2026-04-13T10:00:00+00:00")
        snapshot_mock.assert_called_once_with(
            writer,
            scheduler_status=scheduler_status,
            force_refresh=True,
            prefer_shared_snapshot=False,
        )

    def test_resolve_freshness_snapshot_offloads_live_work_when_cache_misses(self) -> None:
        fake_loop = SimpleNamespace(
            run_in_executor=AsyncMock(return_value={"generated_at": "2026-04-13T10:00:00+00:00"})
        )

        with patch.object(server, "get_cached_freshness_snapshot", return_value=(None, None)), \
             patch.object(server.asyncio, "get_running_loop", return_value=fake_loop), \
             patch.object(server, "_load_current_freshness_snapshot", return_value={"generated_at": "2026-04-13T10:00:00+00:00"}), \
             patch.object(server, "_enqueue_default_dashboard_refresh_if_needed", return_value={"enqueued": True}) as enqueue_mock:
            snapshot = asyncio.run(server._resolve_freshness_snapshot(force_refresh=False))

        self.assertEqual(snapshot["generated_at"], "2026-04-13T10:00:00+00:00")
        fake_loop.run_in_executor.assert_awaited_once()
        enqueue_mock.assert_called_once_with(snapshot, reason="freshness_resolved")

    def test_resolve_freshness_snapshot_force_refresh_ignores_memory_cache(self) -> None:
        cached_snapshot = {"generated_at": "2026-04-13T10:00:00+00:00", "health": {"status": "healthy"}}
        cached_at = datetime.now(timezone.utc) - timedelta(seconds=5)
        fake_loop = SimpleNamespace(
            run_in_executor=AsyncMock(return_value={"generated_at": "2026-04-13T10:05:00+00:00"})
        )

        with patch.object(server, "get_cached_freshness_snapshot", return_value=(cached_snapshot, cached_at)), \
             patch.object(server, "freshness_cache_ttl_seconds", return_value=300), \
             patch.object(server.asyncio, "get_running_loop", return_value=fake_loop), \
             patch.object(server, "_load_current_freshness_snapshot", return_value={"generated_at": "2026-04-13T10:05:00+00:00"}), \
             patch.object(server, "_enqueue_default_dashboard_refresh_if_needed") as enqueue_mock:
            snapshot = asyncio.run(server._resolve_freshness_snapshot(force_refresh=True))

        self.assertEqual(snapshot["generated_at"], "2026-04-13T10:05:00+00:00")
        fake_loop.run_in_executor.assert_awaited_once()
        enqueue_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
