from __future__ import annotations

import json
from datetime import date, datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from api import server


class _FakeRuntimeWriter:
    def __init__(self) -> None:
        self.payloads: dict[str, dict] = {}

    def save_runtime_json(self, path: str, payload: dict) -> bool:
        self.payloads[path] = json.loads(json.dumps(payload))
        return True

    def read_runtime_json(
        self,
        path: str,
        *,
        prefer_signed_read: bool = True,
        timeout_seconds: float | None = None,
    ) -> dict:
        del prefer_signed_read, timeout_seconds
        if path not in self.payloads:
            return {"status": "missing", "payload": {}, "error": "missing", "elapsed_ms": 1.25}
        return {"status": "ok", "payload": self.payloads[path], "error": "", "elapsed_ms": 1.25}


class DashboardPersistedCacheTests(unittest.TestCase):
    def _ctx(self, *, end: date | None = None) -> SimpleNamespace:
        end_date = end or date(2026, 3, 22)
        start_date = date.fromordinal(end_date.toordinal() - 14)
        return SimpleNamespace(
            from_date=start_date,
            to_date=end_date,
            days=15,
            is_operational=False,
            range_label=f"{start_date.isoformat()}..{end_date.isoformat()}",
            cache_key=f"{start_date.isoformat()}:{end_date.isoformat()}",
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

    def test_canonical_default_persistence_gate_allows_strategic_degradation_but_keeps_pulse_required(self) -> None:
        ctx = self._ctx()
        strategic_only = self._meta()
        strategic_only["degradedTiers"] = ["strategic"]
        pulse_only = self._meta()
        pulse_only["degradedTiers"] = ["pulse"]

        self.assertTrue(
            server._should_persist_dashboard_snapshot_for_context(
                strategic_only,
                ctx=ctx,
                trusted_end_date=ctx.to_date.isoformat(),
            )
        )
        self.assertFalse(
            server._should_persist_dashboard_snapshot_for_context(
                pulse_only,
                ctx=ctx,
                trusted_end_date=ctx.to_date.isoformat(),
            )
        )

    def test_default_dashboard_uses_persisted_alias_after_exact_default_resolution(self) -> None:
        ctx = self._ctx()
        persisted = {
            "status": "hit",
            "readMs": 23.4,
            "snapshot": self._snapshot(),
            "meta": self._meta(),
            "ctx": ctx,
            "cacheKey": ctx.cache_key,
            "snapshotBuiltAt": datetime(2026, 3, 22, tzinfo=timezone.utc),
            "trustedEndDate": "2026-03-22",
        }

        with patch.object(server, "_cached_freshness_resolution", side_effect=[
            {"snapshot": None, "source": None},
            {"snapshot": {"generated_at": "2026-03-22T00:00:00+00:00"}, "source": "live"},
        ]), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=ctx.to_date), \
             patch.object(server, "_dashboard_context_from_trusted_end", return_value=ctx), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot", side_effect=[persisted]) as load_mock, \
             patch.object(server, "_is_persisted_snapshot_fresh", return_value=True), \
             patch.object(server, "prime_dashboard_snapshot") as prime_mock:
            payload = server._build_dashboard_response_payload(None, None)

        self.assertEqual(payload["meta"]["cacheSource"], "persisted")
        self.assertEqual(payload["meta"]["cacheStatus"], "persisted_fresh")
        self.assertEqual(payload["meta"]["defaultResolutionPath"], "persisted_alias")
        self.assertEqual(payload["meta"]["persistedReadStatus"], "hit")
        self.assertEqual(payload["meta"]["trustedEndDate"], "2026-03-22")
        load_mock.assert_called_once_with(server._DASHBOARD_DEFAULT_ALIAS_PATH)
        prime_mock.assert_called_once()

    def test_default_dashboard_uses_exact_default_artifact_when_alias_cache_key_mismatches(self) -> None:
        current_ctx = self._ctx()
        previous_ctx = self._ctx(end=date(2026, 3, 21))
        alias_snapshot = {
            "status": "hit",
            "readMs": 11.0,
            "snapshot": self._snapshot(),
            "meta": self._meta(),
            "ctx": previous_ctx,
            "cacheKey": previous_ctx.cache_key,
            "snapshotBuiltAt": datetime(2026, 3, 21, tzinfo=timezone.utc),
            "trustedEndDate": "2026-03-21",
        }
        exact_snapshot = {
            "status": "hit",
            "readMs": 7.5,
            "snapshot": self._snapshot(),
            "meta": self._meta(),
            "ctx": current_ctx,
            "cacheKey": current_ctx.cache_key,
            "snapshotBuiltAt": datetime(2026, 3, 22, tzinfo=timezone.utc),
            "trustedEndDate": "2026-03-22",
        }

        with patch.object(server, "_cached_freshness_resolution", return_value={"snapshot": {"generated_at": "2026-03-22T00:00:00+00:00"}, "source": "memory"}), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=current_ctx.to_date), \
             patch.object(server, "_dashboard_context_from_trusted_end", return_value=current_ctx), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot", side_effect=[alias_snapshot, exact_snapshot]), \
             patch.object(server, "_is_persisted_snapshot_fresh", return_value=True), \
             patch.object(server, "prime_dashboard_snapshot") as prime_mock:
            payload = server._build_dashboard_response_payload(None, None)

        self.assertEqual(payload["meta"]["cacheSource"], "persisted")
        self.assertEqual(payload["meta"]["defaultResolutionPath"], "persisted_exact_default")
        self.assertEqual(payload["meta"]["persistedReadStatus"], "hit")
        self.assertEqual(payload["meta"]["persistedReadMs"], 7.5)
        prime_mock.assert_called_once()

    def test_default_dashboard_alias_cache_key_mismatch_returns_warming_when_exact_default_is_missing(self) -> None:
        current_ctx = self._ctx()
        previous_ctx = self._ctx(end=date(2026, 3, 21))
        alias_snapshot = {
            "status": "hit",
            "readMs": 9.1,
            "snapshot": self._snapshot(),
            "meta": self._meta(),
            "ctx": previous_ctx,
            "cacheKey": previous_ctx.cache_key,
            "snapshotBuiltAt": datetime(2026, 3, 21, tzinfo=timezone.utc),
            "trustedEndDate": "2026-03-21",
        }

        with patch.object(server, "_cached_freshness_resolution", return_value={"snapshot": {"generated_at": "2026-03-22T00:00:00+00:00"}, "source": "memory"}), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=current_ctx.to_date), \
             patch.object(server, "_dashboard_context_from_trusted_end", return_value=current_ctx), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot", side_effect=[alias_snapshot, {"status": "miss", "readMs": 4.5}]), \
             patch.object(server, "schedule_dashboard_snapshot_refresh", return_value={"started": True, "inflight": False, "suppressed": False, "failureCount": 0}) as refresh_mock, \
             patch.object(server, "prime_dashboard_snapshot") as prime_mock:
            with self.assertRaises(server.DashboardWarmingError):
                server._build_dashboard_response_payload(None, None)

        refresh_mock.assert_called_once_with(current_ctx)
        prime_mock.assert_not_called()

    def test_explicit_range_does_not_use_durable_default_artifact(self) -> None:
        ctx = self._ctx()

        with patch.object(server, "_cached_freshness_resolution", return_value={"snapshot": None, "source": None}), \
             patch.object(server, "build_dashboard_date_context", return_value=ctx), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot") as load_mock, \
             patch.object(server, "schedule_dashboard_snapshot_refresh", return_value={"started": True, "inflight": False, "suppressed": False, "failureCount": 0}) as refresh_mock:
            with self.assertRaises(server.DashboardWarmingError):
                server._build_dashboard_response_payload("2026-03-08", "2026-03-22")

        load_mock.assert_not_called()
        refresh_mock.assert_called_once_with(ctx)

    def test_load_persisted_dashboard_snapshot_round_trips_runtime_json(self) -> None:
        writer = _FakeRuntimeWriter()
        ctx = self._ctx()
        payload = server._dashboard_artifact_payload(
            ctx,
            self._snapshot(),
            self._meta(),
            trusted_end_date=ctx.to_date.isoformat(),
        )
        writer.save_runtime_json(server._DASHBOARD_DEFAULT_ALIAS_PATH, payload)

        with patch.object(server, "get_supabase_writer", return_value=writer):
            loaded = server._load_persisted_dashboard_snapshot(server._DASHBOARD_DEFAULT_ALIAS_PATH)

        self.assertEqual(loaded["status"], "hit")
        self.assertEqual(loaded["cacheKey"], ctx.cache_key)
        self.assertEqual(loaded["trustedEndDate"], ctx.to_date.isoformat())
        self.assertEqual(loaded["snapshot"]["communityHealth"]["score"], 72)

    def test_persist_dashboard_snapshot_sync_writes_exact_and_alias_for_current_default(self) -> None:
        writer = _FakeRuntimeWriter()
        ctx = self._ctx()

        with patch.object(server, "get_supabase_writer", return_value=writer), \
             patch.object(server, "_is_canonical_default_context", return_value=True):
            result = server._persist_dashboard_snapshot_sync(
                ctx,
                self._snapshot(),
                self._meta(),
                trusted_end_date=ctx.to_date.isoformat(),
                write_default_alias=True,
            )

        self.assertTrue(result["exactSaved"])
        self.assertTrue(result["aliasSaved"])
        self.assertIn(server._dashboard_snapshot_storage_path(ctx.cache_key), writer.payloads)
        self.assertIn(server._DASHBOARD_DEFAULT_ALIAS_PATH, writer.payloads)

    def test_persist_dashboard_snapshot_sync_writes_canonical_default_with_truthful_strategic_degradation(self) -> None:
        writer = _FakeRuntimeWriter()
        ctx = self._ctx()
        meta = self._meta()
        meta["degradedTiers"] = ["strategic"]
        meta["tierTimes"] = {"pulse": 1.0, "strategic": None}

        with patch.object(server, "get_supabase_writer", return_value=writer), \
             patch.object(server, "_is_canonical_default_context", return_value=True):
            result = server._persist_dashboard_snapshot_sync(
                ctx,
                self._snapshot(),
                meta,
                trusted_end_date=ctx.to_date.isoformat(),
                write_default_alias=True,
            )

        self.assertTrue(result["exactSaved"])
        self.assertTrue(result["aliasSaved"])
        alias_payload = writer.payloads[server._DASHBOARD_DEFAULT_ALIAS_PATH]
        self.assertEqual(alias_payload["dashboardMeta"]["degradedTiers"], ["strategic"])


if __name__ == "__main__":
    unittest.main()
