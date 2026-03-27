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

    def test_default_dashboard_uses_persisted_alias_before_live_freshness(self) -> None:
        ctx = self._ctx()
        persisted = {
            "status": "hit",
            "readMs": 23.4,
            "snapshot": self._snapshot(),
            "meta": self._meta(),
            "ctx": ctx,
            "snapshotBuiltAt": datetime(2026, 3, 22, tzinfo=timezone.utc),
            "trustedEndDate": "2026-03-22",
        }

        with patch.object(server, "_cached_freshness_resolution", return_value={"snapshot": None, "source": None}), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value=persisted), \
             patch.object(server, "_is_persisted_snapshot_usable", return_value=True), \
             patch.object(server, "_is_persisted_snapshot_fresh", return_value=True), \
             patch.object(server, "prime_dashboard_snapshot") as prime_mock, \
             patch.object(server, "_ensure_background_freshness_refresh", return_value=True) as freshness_refresh_mock, \
             patch.object(server, "get_dashboard_snapshot") as rebuild_mock:
            payload = server._build_dashboard_response_payload(None, None)

        self.assertEqual(payload["meta"]["cacheSource"], "persisted")
        self.assertEqual(payload["meta"]["cacheStatus"], "persisted_fresh")
        self.assertEqual(payload["meta"]["persistedReadStatus"], "hit")
        self.assertEqual(payload["meta"]["trustedEndDate"], "2026-03-22")
        prime_mock.assert_called_once()
        freshness_refresh_mock.assert_called_once()
        rebuild_mock.assert_not_called()

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
             patch.object(server, "_ensure_background_dashboard_refresh", return_value=True) as refresh_mock, \
             patch.object(server, "prime_dashboard_snapshot") as prime_mock:
            payload = server._build_dashboard_response_payload("2026-03-08", "2026-03-22")

        self.assertEqual(payload["meta"]["cacheSource"], "persisted")
        self.assertTrue(payload["meta"]["isStale"])
        self.assertEqual(payload["meta"]["cacheStatus"], "persisted_stale_while_revalidate")
        refresh_mock.assert_called_once()
        prime_mock.assert_called_once()

    def test_missing_snapshot_falls_back_to_blocking_rebuild_and_async_persist(self) -> None:
        ctx = self._ctx()
        rebuilt = (self._snapshot(), self._meta(cache_status="refresh_success"))

        with patch.object(server, "_cached_freshness_resolution", return_value={"snapshot": None, "source": None}), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value={"status": "miss", "readMs": 4.5}), \
             patch.object(server, "build_dashboard_date_context", return_value=ctx), \
             patch.object(server, "get_dashboard_snapshot", return_value=rebuilt) as rebuild_mock, \
             patch.object(server, "_persist_dashboard_snapshot_async") as persist_mock:
            payload = server._build_dashboard_response_payload("2026-03-08", "2026-03-22")

        self.assertEqual(payload["meta"]["cacheSource"], "rebuild")
        self.assertEqual(payload["meta"]["persistedReadStatus"], "miss")
        rebuild_mock.assert_called_once_with(ctx, force_refresh=True)
        persist_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
