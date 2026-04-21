from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api import aggregator
from api import runtime_coordinator
from api import server
from api.dashboard_dates import build_dashboard_date_context


class DashboardCacheFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        aggregator.invalidate_cache()
        aggregator._refresh_states.clear()

    def test_build_dashboard_snapshot_once_tracks_skipped_tiers(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")
        tier_times = {
            "pulse": 1.0,
            "strategic": 1.0,
            "behavioral": 1.0,
            "network": None,
            "psychographic": 1.0,
            "predictive": None,
            "actionable": 1.0,
            "comparative": 1.0,
            "derived": 0.0,
        }
        with patch.object(
            aggregator,
            "_build_snapshot_with_timeout",
            return_value=({"communityHealth": {"score": 52}}, tier_times, 4.2, "parallel"),
        ) as build_mock:
            _snapshot, meta = aggregator.build_dashboard_snapshot_once(
                ctx,
                skipped_tiers={"network", "predictive"},
                cache_status="historical_fastpath_uncached",
            )

        build_mock.assert_called_once_with(ctx, skipped_tiers={"network", "predictive"})
        self.assertEqual(meta["cacheStatus"], "historical_fastpath_uncached")
        self.assertEqual(meta["skippedTiers"], ["network", "predictive"])

    def test_critical_degraded_rebuild_seeds_short_lived_stale_snapshot(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")
        tier_times = {
            "pulse": 1.2,
            "strategic": None,
            "behavioral": 2.3,
            "network": 1.1,
            "psychographic": 0.8,
            "predictive": 1.5,
            "actionable": 1.0,
            "comparative": 1.4,
            "derived": 0.0,
        }
        snapshot = {"communityHealth": {"score": 50}}

        with patch.object(
            aggregator,
            "_build_snapshot_with_timeout",
            return_value=(snapshot, tier_times, 12.0, "parallel"),
        ) as build_mock:
            first_snapshot, first_meta = aggregator.get_dashboard_snapshot(ctx, force_refresh=True)

        self.assertEqual(first_snapshot, snapshot)
        self.assertEqual(first_meta["cacheStatus"], "refresh_success_uncached_degraded")
        self.assertTrue(first_meta["isStale"])
        self.assertIn(ctx.cache_key, aggregator._cache_entries)
        build_mock.assert_called_once()

        with patch.object(
            aggregator,
            "_ensure_background_refresh",
            return_value=True,
        ) as refresh_mock, \
             patch.object(aggregator, "_build_snapshot_with_timeout", side_effect=AssertionError("unexpected rebuild")):
            second_snapshot, second_meta = aggregator.get_dashboard_snapshot(ctx)

        self.assertEqual(second_snapshot, snapshot)
        self.assertEqual(second_meta["cacheStatus"], "stale_while_revalidate")
        self.assertTrue(second_meta["isStale"])
        refresh_mock.assert_called_once_with(ctx.cache_key, ctx)

    def test_schedule_dashboard_snapshot_refresh_is_single_flight_per_key(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")
        thread_starts: list[str] = []

        class DummyThread:
            def __init__(self, *, name: str | None = None, target=None, args=(), daemon: bool | None = None):
                self.name = name or "dummy"
                self._target = target
                self._args = args
                self.daemon = daemon

            def start(self) -> None:
                thread_starts.append(self.name)

        with patch.object(aggregator.threading, "Thread", DummyThread):
            results = [aggregator.schedule_dashboard_snapshot_refresh(ctx) for _ in range(8)]

        self.assertEqual(sum(1 for result in results if result["started"]), 1)
        self.assertEqual(sum(1 for result in results if result["inflight"]), 7)
        self.assertTrue(all(not result["suppressed"] for result in results))
        self.assertEqual(len(thread_starts), 1)
        aggregator._release_refresh_slot(ctx.cache_key)


class DashboardApiAvailabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._startup_handlers = list(server.app.router.on_startup)
        cls._shutdown_handlers = list(server.app.router.on_shutdown)
        server.app.router.on_startup = []
        server.app.router.on_shutdown = []
        cls.client = TestClient(server.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        server.app.router.on_startup = cls._startup_handlers
        server.app.router.on_shutdown = cls._shutdown_handlers

    def setUp(self) -> None:
        server._analytics_rate_limit_buckets.clear()
        runtime_coordinator._LOCAL_COUNTERS.clear()
        runtime_coordinator._LOCAL_LOCKS.clear()

    def test_dashboard_exact_stale_snapshot_returns_200_with_truthful_meta(self) -> None:
        stale_snapshot = {
            "communityHealth": {"score": 52},
            "communityBrief": {
                "postsAnalyzed24h": 10,
                "commentScopesAnalyzed24h": 15,
                "totalAnalyses24h": 25,
                "refreshedMinutesAgo": 4,
                "windowDays": 7,
            },
        }
        stale_meta = {
            "cacheStatus": "memory_stale",
            "cacheSource": "memory",
            "degradedTiers": [],
            "suppressedDegradedTiers": [],
            "tierTimes": {"pulse": 0.5, "derived": 0.0},
            "snapshotBuiltAt": "2026-04-06T00:00:00Z",
            "isStale": True,
            "buildElapsedSeconds": 0.5,
            "buildMode": "parallel",
            "refreshFailureCount": 1,
        }
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(
                 server,
                 "_cached_freshness_resolution",
                 return_value={"snapshot": None, "source": None, "snapshotBuiltAt": None, "persistedReadStatus": None, "persistedReadMs": None},
             ), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value={"status": "miss", "readMs": 0.0}), \
             patch.object(
                 server,
                 "peek_dashboard_snapshot",
                 return_value=(stale_snapshot, stale_meta, "stale"),
             ), \
             patch.object(
                 server,
                 "schedule_dashboard_snapshot_refresh",
                 return_value={"started": True, "inflight": False, "suppressed": False, "failureCount": 1},
             ) as schedule_mock:
            response = self.client.get("/api/dashboard?from=2026-03-31&to=2026-04-06")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["meta"]["cacheSource"], "memory")
        self.assertEqual(payload["meta"]["cacheStatus"], "memory_stale_while_revalidate")
        self.assertTrue(payload["meta"]["isStale"])
        self.assertEqual(payload["meta"]["fallbackReason"], "exact_stale_snapshot")
        self.assertFalse(payload["meta"]["refreshSuppressed"])
        schedule_mock.assert_called_once()

    def test_dashboard_exact_miss_rebuilds_without_name_error(self) -> None:
        rebuild_snapshot = {
            "communityHealth": {"score": 55},
            "communityBrief": {
                "postsAnalyzed24h": 11,
                "commentScopesAnalyzed24h": 17,
                "totalAnalyses24h": 28,
                "refreshedMinutesAgo": 6,
                "windowDays": 7,
            },
        }
        rebuild_meta = {
            "cacheStatus": "refresh_success",
            "cacheSource": "rebuild",
            "degradedTiers": [],
            "suppressedDegradedTiers": [],
            "tierTimes": {"pulse": 0.5, "derived": 0.0},
            "snapshotBuiltAt": "2026-04-06T00:00:00Z",
            "isStale": False,
            "buildElapsedSeconds": 0.5,
            "buildMode": "parallel",
            "refreshFailureCount": 0,
        }
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(
                 server,
                 "_cached_freshness_resolution",
                 return_value={"snapshot": None, "source": None, "snapshotBuiltAt": None, "persistedReadStatus": None, "persistedReadMs": None},
             ), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value={"status": "miss", "readMs": 0.0}), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "get_dashboard_snapshot", return_value=(rebuild_snapshot, rebuild_meta)) as rebuild_mock, \
             patch.object(server, "_should_persist_dashboard_snapshot", return_value=False), \
             patch.object(server, "_persist_dashboard_snapshot_async") as persist_mock:
            response = self.client.get("/api/dashboard?from=2026-03-31&to=2026-04-06")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["meta"]["cacheSource"], "rebuild")
        self.assertEqual(payload["meta"]["cacheStatus"], "refresh_success")
        rebuild_mock.assert_called_once()
        persist_mock.assert_not_called()

    def test_dashboard_placeholder_snapshot_returns_warming_503(self) -> None:
        placeholder_meta = {
            "cacheStatus": "refresh_success",
            "cacheSource": "rebuild",
            "degradedTiers": [],
            "suppressedDegradedTiers": [],
            "tierTimes": {"pulse": 0.4, "derived": 0.0},
            "snapshotBuiltAt": "2026-04-06T00:00:00Z",
            "isStale": False,
            "buildElapsedSeconds": 0.4,
            "buildMode": "parallel",
            "refreshFailureCount": 0,
        }
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(
                 server,
                 "_cached_freshness_resolution",
                 return_value={"snapshot": None, "source": None, "snapshotBuiltAt": None, "persistedReadStatus": None, "persistedReadMs": None},
             ), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value={"status": "miss", "readMs": 0.0}), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "get_dashboard_snapshot", return_value=({"communityHealth": {"score": 0}, "communityBrief": {}}, placeholder_meta)):
            response = self.client.get("/api/dashboard?from=2026-03-31&to=2026-04-06")

        self.assertEqual(response.status_code, 503)
        self.assertIn("warming this date range", response.text.lower())

    def test_dashboard_exact_miss_timeout_returns_warming_503(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(
                 server,
                 "_cached_freshness_resolution",
                 return_value={"snapshot": None, "source": None, "snapshotBuiltAt": None, "persistedReadStatus": None, "persistedReadMs": None},
             ), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value={"status": "miss", "readMs": 0.0}), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "get_dashboard_snapshot", side_effect=TimeoutError("dashboard rebuild timed out")):
            response = self.client.get("/api/dashboard?from=2026-03-31&to=2026-04-06")

        self.assertEqual(response.status_code, 503)
        self.assertIn("warming this date range", response.text.lower())


if __name__ == "__main__":
    unittest.main()
