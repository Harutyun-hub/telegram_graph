from __future__ import annotations

import threading
import time
import unittest
from unittest.mock import patch

from api import aggregator
from api.dashboard_dates import build_dashboard_date_context


class DashboardRefreshWaitTests(unittest.TestCase):
    def setUp(self) -> None:
        with aggregator._cache_lock:
            aggregator._cache_entries.clear()
        with aggregator._refresh_state_lock:
            aggregator._refresh_states.clear()

    def tearDown(self) -> None:
        with aggregator._cache_lock:
            aggregator._cache_entries.clear()
        with aggregator._refresh_state_lock:
            aggregator._refresh_states.clear()

    def test_follower_waits_longer_when_no_stale_snapshot_exists(self) -> None:
        ctx = build_dashboard_date_context("2026-03-10", "2026-03-24")
        state = aggregator.DashboardRefreshState(inflight=True)
        state.event.clear()
        with aggregator._refresh_state_lock:
            aggregator._refresh_states[ctx.cache_key] = state

        snapshot = {"communityHealth": {"score": 72}}
        meta = {
            "cacheStatus": "refresh_success",
            "degradedTiers": [],
            "suppressedDegradedTiers": [],
            "tierTimes": {},
            "snapshotBuiltAt": "2026-03-24T00:00:00+00:00",
            "isStale": False,
            "buildElapsedSeconds": 11.7,
            "buildMode": "parallel",
            "refreshFailureCount": 0,
        }

        def _finish_refresh() -> None:
            with aggregator._cache_lock:
                aggregator._cache_entries[ctx.cache_key] = (time.time(), snapshot, meta)
            with aggregator._refresh_state_lock:
                state.inflight = False
                state.event.set()

        timer = threading.Timer(0.05, _finish_refresh)
        timer.start()
        try:
            with patch.object(aggregator, "WAIT_FOR_REFRESH_SECONDS", 0.01), \
                 patch.object(aggregator, "WAIT_FOR_EMPTY_REFRESH_SECONDS", 0.2):
                payload, runtime_meta = aggregator.get_dashboard_snapshot(ctx)
        finally:
            timer.join(timeout=1.0)

        self.assertEqual(payload, snapshot)
        self.assertEqual(runtime_meta.get("cacheStatus"), "refresh_success")
        self.assertEqual(runtime_meta.get("refreshFailureCount"), 0)

    def test_one_off_snapshot_can_skip_optional_tiers_without_priming_cache(self) -> None:
        ctx = build_dashboard_date_context("2026-03-10", "2026-03-24")

        with patch.object(aggregator, "_build_snapshot_with_timeout") as build_mock:
            build_mock.return_value = (
                {"communityHealth": {"score": 72}, "communityChannels": [], "weeklyShifts": []},
                {"pulse": 1.2, "network": None, "comparative": None, "derived": 0.0},
                1.8,
                "parallel",
            )
            payload, runtime_meta = aggregator.build_dashboard_snapshot_once(
                ctx,
                skipped_tiers={"network", "comparative"},
                cache_status="historical_fastpath_uncached",
            )

        self.assertEqual(payload["communityHealth"]["score"], 72)
        self.assertEqual(runtime_meta.get("cacheStatus"), "historical_fastpath_uncached")
        self.assertEqual(sorted(runtime_meta.get("skippedTiers") or []), ["comparative", "network"])
        self.assertEqual(sorted(runtime_meta.get("degradedTiers") or []), ["comparative", "network"])
        with aggregator._cache_lock:
            self.assertNotIn(ctx.cache_key, aggregator._cache_entries)

    def test_critical_degraded_rebuild_becomes_short_lived_stale_snapshot(self) -> None:
        ctx = build_dashboard_date_context("2026-03-10", "2026-03-24")
        tier_times = {
            "pulse": 1.2,
            "strategic": None,
            "behavioral": 2.1,
            "network": 0.8,
            "psychographic": 0.5,
            "predictive": 1.0,
            "actionable": 0.9,
            "comparative": 1.1,
            "derived": 0.0,
        }
        snapshot = {"communityHealth": {"score": 68}}

        with patch.object(
            aggregator,
            "_build_snapshot_with_timeout",
            return_value=(snapshot, tier_times, 12.4, "parallel"),
        ):
            payload, runtime_meta = aggregator.get_dashboard_snapshot(ctx, force_refresh=True)

        self.assertEqual(payload, snapshot)
        self.assertEqual(runtime_meta.get("cacheStatus"), "refresh_success_uncached_degraded")
        self.assertTrue(runtime_meta.get("isStale"))

        with patch.object(aggregator, "_ensure_background_refresh", return_value=True) as refresh_mock, \
             patch.object(aggregator, "_build_snapshot_with_timeout", side_effect=AssertionError("unexpected rebuild")):
            second_payload, second_meta = aggregator.get_dashboard_snapshot(ctx)

        self.assertEqual(second_payload, snapshot)
        self.assertEqual(second_meta.get("cacheStatus"), "stale_while_revalidate")
        self.assertTrue(second_meta.get("isStale"))
        refresh_mock.assert_called_once_with(ctx.cache_key, ctx)


if __name__ == "__main__":
    unittest.main()
