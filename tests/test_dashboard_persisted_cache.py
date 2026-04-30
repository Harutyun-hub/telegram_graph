from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from api import server


class DashboardPersistedCacheTests(unittest.TestCase):
    def _ctx(self) -> SimpleNamespace:
        return SimpleNamespace(
            from_date=datetime(2026, 3, 8, tzinfo=timezone.utc).date(),
            to_date=datetime(2026, 3, 22, tzinfo=timezone.utc).date(),
            days=15,
            is_operational=False,
            range_label="2026-03-08..2026-03-22",
            cache_key="2026-03-08:2026-03-22",
        )

    def _meta(self, *, cache_status: str = "refresh_success", is_stale: bool = False) -> dict:
        return {
            "cacheStatus": cache_status,
            "degradedTiers": [],
            "suppressedDegradedTiers": [],
            "tierTimes": {},
            "snapshotBuiltAt": "2026-03-22T00:00:00+00:00",
            "isStale": is_stale,
            "buildElapsedSeconds": 0.25,
            "buildMode": "test",
            "refreshFailureCount": 0,
        }

    def _snapshot(self) -> dict:
        return {"communityHealth": {"score": 72}}

    def test_default_dashboard_uses_matching_persisted_alias_after_freshness_resolution(self) -> None:
        ctx = self._ctx()
        alias_snapshot = {
            "status": "hit",
            "readMs": 23.4,
            "snapshot": self._snapshot(),
            "meta": self._meta(),
            "ctx": ctx,
            "snapshotBuiltAt": datetime(2026, 3, 22, tzinfo=timezone.utc),
            "trustedEndDate": "2026-03-22",
            "cacheKey": ctx.cache_key,
        }
        live_freshness = {
            "snapshot": {"generated_at": "2026-03-22T00:00:00+00:00"},
            "source": "live",
        }

        with patch.object(server, "_cached_freshness_resolution", side_effect=[
            {"snapshot": None, "source": None},
            live_freshness,
        ]), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=ctx.to_date), \
             patch.object(server, "_dashboard_context_from_trusted_end", return_value=ctx), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot", side_effect=[
                 {"status": "miss", "readMs": 5.2},
                 alias_snapshot,
             ]), \
             patch.object(server, "_is_persisted_snapshot_usable", return_value=True), \
             patch.object(server, "_is_persisted_snapshot_fresh", return_value=True), \
             patch.object(server, "prime_dashboard_snapshot") as prime_mock:
            payload = server._build_dashboard_response_payload(None, None)

        self.assertEqual(payload["meta"]["cacheSource"], "persisted")
        self.assertEqual(payload["meta"]["cacheStatus"], "persisted_fresh")
        self.assertEqual(payload["meta"]["defaultResolutionPath"], "persisted_alias")
        self.assertEqual(payload["meta"]["persistedReadStatus"], "hit")
        self.assertEqual(payload["meta"]["trustedEndDate"], "2026-03-22")
        self.assertEqual(payload["meta"]["persistedReadMs"], 28.6)
        prime_mock.assert_called_once()

    def test_default_dashboard_rejects_alias_mismatch_and_returns_warming(self) -> None:
        current_ctx = self._ctx()
        alias_ctx = SimpleNamespace(
            from_date=datetime(2026, 3, 7, tzinfo=timezone.utc).date(),
            to_date=datetime(2026, 3, 21, tzinfo=timezone.utc).date(),
            days=15,
            is_operational=False,
            range_label="2026-03-07..2026-03-21",
            cache_key="2026-03-07:2026-03-21",
        )
        live_freshness = {
            "snapshot": {"generated_at": "2026-03-22T00:00:00+00:00"},
            "source": "live",
        }
        alias_snapshot = {
            "status": "hit",
            "readMs": 18.6,
            "snapshot": self._snapshot(),
            "meta": self._meta(is_stale=True),
            "ctx": alias_ctx,
            "snapshotBuiltAt": datetime(2026, 3, 21, tzinfo=timezone.utc),
            "trustedEndDate": "2026-03-21",
            "cacheKey": alias_ctx.cache_key,
        }

        with patch.object(server, "_cached_freshness_resolution", side_effect=[
            {"snapshot": None, "source": None},
            live_freshness,
        ]), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=current_ctx.to_date), \
             patch.object(server, "_dashboard_context_from_trusted_end", return_value=current_ctx), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot", side_effect=[
                 {"status": "miss", "readMs": 5.2},
                 alias_snapshot,
             ]), \
             patch.object(server, "schedule_dashboard_snapshot_refresh", return_value={"started": True, "inflight": False, "suppressed": False, "failureCount": 0}) as refresh_mock, \
             patch.object(server, "prime_dashboard_snapshot") as prime_mock:
            with self.assertRaises(server.DashboardWarmingError):
                server._build_dashboard_response_payload(None, None)

        refresh_mock.assert_called_once_with(current_ctx)
        prime_mock.assert_not_called()

    def test_explicit_range_serves_persisted_stale_and_triggers_refresh(self) -> None:
        ctx = self._ctx()
        persisted = {
            "status": "hit",
            "readMs": 11.2,
            "snapshot": self._snapshot(),
            "meta": self._meta(),
            "ctx": ctx,
            "snapshotBuiltAt": datetime(2026, 3, 22, tzinfo=timezone.utc),
            "trustedEndDate": "2026-03-22",
        }

        with patch.object(server, "_cached_freshness_resolution", return_value={"snapshot": None, "source": None}), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value=persisted), \
             patch.object(server, "_is_persisted_snapshot_fresh", return_value=False), \
             patch.object(server, "_is_persisted_snapshot_usable", return_value=True), \
             patch.object(server, "schedule_dashboard_snapshot_refresh", return_value={"started": True, "inflight": False, "suppressed": False, "failureCount": 0}) as refresh_mock, \
             patch.object(server, "prime_dashboard_snapshot") as prime_mock:
            payload = server._build_dashboard_response_payload("2026-03-08", "2026-03-22")

        self.assertEqual(payload["meta"]["cacheSource"], "persisted")
        self.assertTrue(payload["meta"]["isStale"])
        self.assertEqual(payload["meta"]["cacheStatus"], "persisted_stale_while_revalidate")
        self.assertEqual(payload["meta"]["fallbackReason"], "exact_stale_snapshot")
        self.assertFalse(payload["meta"]["refreshSuppressed"])
        refresh_mock.assert_called_once()
        prime_mock.assert_called_once()

    def test_missing_snapshot_returns_warming_and_background_refresh_only(self) -> None:
        ctx = self._ctx()

        with patch.object(server, "_cached_freshness_resolution", return_value={"snapshot": None, "source": None}), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value={"status": "miss", "readMs": 4.5}), \
             patch.object(server, "build_dashboard_date_context", return_value=ctx), \
             patch.object(server, "schedule_dashboard_snapshot_refresh", return_value={"started": True, "inflight": False, "suppressed": False, "failureCount": 0}) as refresh_mock, \
             patch.object(server, "get_dashboard_snapshot") as rebuild_mock, \
             patch.object(server, "_persist_dashboard_snapshot_async") as persist_mock:
            with self.assertRaises(server.DashboardWarmingError):
                server._build_dashboard_response_payload("2026-03-08", "2026-03-22")

        refresh_mock.assert_called_once_with(ctx)
        rebuild_mock.assert_not_called()
        persist_mock.assert_not_called()

    def test_missing_historical_snapshot_returns_warming_without_fastpath_build(self) -> None:
        ctx = self._ctx()
        fast_meta = self._meta(cache_status="historical_fastpath_uncached")
        fast_meta["degradedTiers"] = ["network", "comparative"]
        fast_meta["skippedTiers"] = ["comparative", "network"]

        with patch.object(server, "_cached_freshness_resolution", return_value={"snapshot": None, "source": None}), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value={"status": "miss", "readMs": 4.5}), \
             patch.object(server, "build_dashboard_date_context", return_value=ctx), \
             patch.object(server, "_should_use_historical_fastpath", return_value=True), \
             patch.object(server, "build_dashboard_snapshot_once", return_value=(self._snapshot(), fast_meta)) as fastpath_mock, \
             patch.object(server, "schedule_dashboard_snapshot_refresh", return_value={"started": True, "inflight": False, "suppressed": False, "failureCount": 0}) as refresh_mock, \
             patch.object(server, "get_dashboard_snapshot") as rebuild_mock:
            with self.assertRaises(server.DashboardWarmingError):
                server._build_dashboard_response_payload("2026-03-08", "2026-03-22")

        fastpath_mock.assert_not_called()
        refresh_mock.assert_called_once_with(ctx)
        rebuild_mock.assert_not_called()

    def test_default_dashboard_cold_miss_returns_warming_when_exact_and_alias_are_missing(self) -> None:
        ctx = self._ctx()
        live_freshness = {
            "snapshot": {"generated_at": "2026-03-22T00:00:00+00:00"},
            "source": "live",
        }

        with patch.object(server, "_cached_freshness_resolution", side_effect=[
            {"snapshot": None, "source": None},
            live_freshness,
        ]), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=ctx.to_date), \
             patch.object(server, "_dashboard_context_from_trusted_end", return_value=ctx), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot", side_effect=[
                 {"status": "miss", "readMs": 5.2},
                 {"status": "miss", "readMs": 4.5},
             ]), \
             patch.object(server, "schedule_dashboard_snapshot_refresh", return_value={"started": True, "inflight": False, "suppressed": False, "failureCount": 0}) as refresh_mock, \
             patch.object(server, "get_dashboard_snapshot") as rebuild_mock, \
             patch.object(server, "_persist_dashboard_snapshot_async") as persist_mock:
            with self.assertRaises(server.DashboardWarmingError):
                server._build_dashboard_response_payload(None, None)

        refresh_mock.assert_called_once_with(ctx)
        rebuild_mock.assert_not_called()
        persist_mock.assert_not_called()

    def test_default_memory_hit_persists_exact_snapshot_and_default_alias_async(self) -> None:
        ctx = self._ctx()
        freshness = {
            "snapshot": {"generated_at": "2026-03-22T00:00:00+00:00"},
            "source": "memory",
        }
        memory_meta = self._meta(cache_status="memory_fresh")

        with patch.object(server, "_cached_freshness_resolution", return_value=freshness), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=ctx.to_date), \
             patch.object(server, "_dashboard_context_from_trusted_end", return_value=ctx), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(self._snapshot(), memory_meta, "fresh")), \
             patch.object(server, "_persist_dashboard_snapshot_async") as persist_mock:
            payload = server._build_dashboard_response_payload(None, None)

        self.assertEqual(payload["meta"]["cacheSource"], "memory")
        self.assertEqual(payload["meta"]["cacheStatus"], "memory_fresh")
        persist_mock.assert_called_once_with(
            ctx,
            self._snapshot(),
            memory_meta,
            trusted_end_date="2026-03-22",
            write_default_alias=True,
        )

    def test_load_persisted_dashboard_snapshot_fails_safe_on_timeout(self) -> None:
        writer = SimpleNamespace(
            read_runtime_json=lambda *args, **kwargs: {"status": "timeout", "payload": {}, "elapsed_ms": 341.2},
        )

        with patch.object(server, "get_supabase_writer", return_value=writer):
            loaded = server._load_persisted_dashboard_snapshot("dashboard/snapshots/default.json")

        self.assertEqual(loaded["status"], "timeout")
        self.assertEqual(loaded["readMs"], 341.2)


if __name__ == "__main__":
    unittest.main()
