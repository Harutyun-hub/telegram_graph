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


if __name__ == "__main__":
    unittest.main()
