from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading
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

    def test_tier_pulse_uses_shared_snapshot_builder_once(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")
        snapshot = {
            "communityHealth": {"score": 64},
            "trendingTopics": [{"name": "Housing"}],
            "trendingNewTopics": [{"name": "Jobs"}],
            "communityBrief": {"postsAnalyzed24h": 42},
        }

        with patch.object(aggregator.pulse, "get_pulse_snapshot", return_value=snapshot) as pulse_mock:
            payload = aggregator._tier_pulse(ctx)

        pulse_mock.assert_called_once_with(ctx)
        self.assertEqual(payload, snapshot)

    def test_build_snapshot_parallel_uses_extended_timeout_for_critical_tiers(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")
        captured_timeouts: dict[str, float] = {}

        class DummyExecutor:
            pass

        class DummyFuture:
            def __init__(self, name: str, payload: dict) -> None:
                self.name = name
                self.payload = payload

            def result(self, timeout=None):
                captured_timeouts[self.name] = timeout
                return self.payload, 0.1

        ordered = [
            ("pulse", lambda: {"communityHealth": {"score": 61}}),
            ("network", lambda: {"communityChannels": []}),
        ]
        futures = {
            "pulse": DummyFuture("pulse", {"communityHealth": {"score": 61}}),
            "network": DummyFuture("network", {"communityChannels": []}),
        }

        with patch.object(aggregator, "_ordered_tiers", return_value=ordered), \
             patch.object(aggregator, "_submit_tier_futures", return_value=(DummyExecutor(), futures)):
            aggregator._build_snapshot_parallel(ctx, use_timeouts=True)

        self.assertEqual(captured_timeouts["pulse"], aggregator.CRITICAL_TIER_TIMEOUT_SECONDS)
        self.assertEqual(captured_timeouts["network"], aggregator.TIER_TIMEOUT_SECONDS)

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

    def test_build_dashboard_snapshot_once_can_force_sequential_mode(self) -> None:
        ctx = build_dashboard_date_context("2026-04-09", "2026-04-15")
        tier_times = {
            "pulse": 3.8,
            "strategic": None,
            "behavioral": None,
            "network": None,
            "psychographic": None,
            "predictive": None,
            "actionable": None,
            "comparative": None,
            "derived": 0.0,
        }

        with patch.object(
            aggregator,
            "_build_snapshot_with_timeout",
            return_value=({"communityHealth": {"score": 55}}, tier_times, 3.8, "sequential"),
        ) as build_mock:
            _snapshot, meta = aggregator.build_dashboard_snapshot_once(
                ctx,
                skipped_tiers={"strategic", "behavioral"},
                cache_status="sync_exact_fastpath",
                parallel_enabled=False,
            )

        build_mock.assert_called_once_with(
            ctx,
            skipped_tiers={"strategic", "behavioral"},
            parallel_enabled=False,
        )
        self.assertEqual(meta["cacheStatus"], "sync_exact_fastpath")
        self.assertEqual(meta["buildMode"], "sequential")

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

    def test_same_key_empty_pulse_rebuild_preserves_richer_previous_snapshot(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")
        stale_snapshot = {
            "communityBrief": {"totalAnalyses24h": 128, "postsAnalyzed24h": 90},
            "communityHealth": {
                "score": 64,
                "components": [{"label": "Sentiment", "score": 64}],
            },
            "trendingTopics": [{"name": "Housing"}],
        }
        stale_meta = {
            "cacheStatus": "refresh_success",
            "degradedTiers": [],
            "suppressedDegradedTiers": [],
            "tierTimes": {"pulse": 1.0},
            "snapshotBuiltAt": "2026-04-06T00:00:00Z",
            "isStale": False,
            "buildElapsedSeconds": 2.1,
            "buildMode": "parallel",
            "refreshFailureCount": 0,
        }
        empty_pulse_snapshot = {
            "communityBrief": {
                "totalAnalyses24h": 0,
                "postsAnalyzed24h": 0,
                "commentScopesAnalyzed24h": 0,
            },
            "communityHealth": {"score": 50, "components": []},
            "trendingTopics": [],
        }
        tier_times = {
            "pulse": 1.2,
            "strategic": 1.0,
            "behavioral": 1.0,
            "network": 1.1,
            "psychographic": 1.0,
            "predictive": 1.0,
            "actionable": 1.0,
            "comparative": 1.0,
            "derived": 0.0,
        }
        with aggregator._cache_lock:
            aggregator._cache_entries[ctx.cache_key] = (aggregator.time.time(), stale_snapshot, stale_meta)

        with patch.object(
            aggregator,
            "_build_snapshot_with_timeout",
            return_value=(empty_pulse_snapshot, tier_times, 8.2, "parallel"),
        ):
            snapshot, meta = aggregator.get_dashboard_snapshot(ctx, force_refresh=True)

        self.assertEqual(snapshot, stale_snapshot)
        self.assertEqual(meta["cacheStatus"], "preserved_previous_on_empty_pulse")
        self.assertTrue(meta["isStale"])

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

    def test_seed_dashboard_snapshot_respects_refresh_suppression_by_default(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")
        state = aggregator._get_refresh_state(ctx.cache_key)
        state.suppressed_until = aggregator.time.time() + 60.0
        state.failure_count = 2

        result = aggregator.seed_dashboard_snapshot(ctx)

        self.assertFalse(result["started"])
        self.assertTrue(result["suppressed"])
        self.assertEqual(result["failureCount"], 2)

    def test_seed_dashboard_snapshot_uses_timeout_override_without_emitting_refresh_callback(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")
        snapshot = {"communityHealth": {"score": 55}}
        meta = {
            "cacheStatus": "refresh_success",
            "degradedTiers": [],
            "suppressedDegradedTiers": [],
            "tierTimes": {"pulse": 1.0},
            "snapshotBuiltAt": "2026-04-06T00:00:00Z",
            "isStale": False,
            "buildElapsedSeconds": 4.0,
            "buildMode": "parallel",
            "refreshFailureCount": 0,
        }

        with patch.object(
            aggregator,
            "_refresh_dashboard_snapshot",
            return_value=(snapshot, meta),
        ) as refresh_mock:
            result = aggregator.seed_dashboard_snapshot(
                ctx,
                timeout_seconds=45.0,
                force=True,
            )

        self.assertTrue(result["started"])
        self.assertEqual(result["snapshot"], snapshot)
        self.assertEqual(result["meta"], meta)
        refresh_mock.assert_called_once()
        _args, kwargs = refresh_mock.call_args
        self.assertEqual(kwargs["timeout_seconds"], 45.0)
        self.assertFalse(kwargs["emit_refresh_complete"])


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
             patch.object(
                 server,
                 "peek_dashboard_snapshot",
                 return_value=({"communityHealth": {"score": 52}}, stale_meta, "stale"),
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

    def test_dashboard_exact_miss_builds_synchronously_and_returns_200(self) -> None:
        built_snapshot = {
            "communityHealth": {"score": 52},
            "trendingTopics": [{"name": "Housing"}],
            "communityBrief": {"postsAnalyzed24h": 42},
        }
        built_meta = {
            "cacheStatus": "sync_exact_build",
            "cacheSource": "rebuild",
            "degradedTiers": [],
            "suppressedDegradedTiers": [],
            "tierTimes": {"pulse": 0.5, "derived": 0.0},
            "snapshotBuiltAt": "2026-04-06T00:00:00Z",
            "isStale": False,
            "buildElapsedSeconds": 0.5,
            "buildMode": "parallel",
            "refreshFailureCount": 0,
            "skippedTiers": [],
        }
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(
                 server,
                 "_cached_freshness_resolution",
                 return_value={"snapshot": {"generated_at": "2026-04-06T00:00:00Z"}, "source": "memory", "snapshotBuiltAt": None, "persistedReadStatus": None, "persistedReadMs": None},
             ), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=build_dashboard_date_context("2026-03-23", "2026-04-06").to_date), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(
                 server,
                 "_build_exact_range_snapshot_sync",
                 return_value=(built_snapshot, built_meta, "sync_exact_build"),
             ) as build_mock, \
             patch.object(server, "schedule_dashboard_snapshot_refresh") as schedule_mock:
            response = self.client.get("/api/dashboard?from=2026-03-31&to=2026-04-06")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["meta"]["from"], "2026-03-31")
        self.assertEqual(payload["meta"]["to"], "2026-04-06")
        self.assertEqual(payload["meta"]["rangeResolutionPath"], "sync_exact_build")
        self.assertEqual(payload["meta"]["cacheStatus"], "sync_exact_build")
        self.assertEqual(payload["data"]["communityHealth"]["score"], 52)
        self.assertEqual(payload["data"]["trendingTopics"][0]["name"], "Housing")
        build_mock.assert_called_once()
        schedule_mock.assert_not_called()

    def test_dashboard_historical_exact_range_returns_truthful_skipped_tiers(self) -> None:
        built_snapshot = {"communityHealth": {"score": 52}}
        built_meta = {
            "cacheStatus": "sync_exact_historical_fastpath",
            "cacheSource": "rebuild",
            "degradedTiers": [],
            "suppressedDegradedTiers": [],
            "tierTimes": {"pulse": 0.5, "derived": 0.0},
            "snapshotBuiltAt": "2026-04-06T00:00:00Z",
            "isStale": False,
            "buildElapsedSeconds": 0.5,
            "buildMode": "parallel",
            "refreshFailureCount": 0,
            "skippedTiers": ["comparative", "network", "predictive"],
        }
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(
                 server,
                 "_cached_freshness_resolution",
                 return_value={"snapshot": {"generated_at": "2026-04-15T00:00:00Z"}, "source": "memory", "snapshotBuiltAt": None, "persistedReadStatus": None, "persistedReadMs": None},
             ), \
             patch.object(server, "_trusted_end_date_from_freshness", return_value=build_dashboard_date_context("2026-04-01", "2026-04-15").to_date), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(
                 server,
                 "_build_exact_range_snapshot_sync",
                 return_value=(built_snapshot, built_meta, "sync_exact_historical_fastpath"),
             ) as build_mock:
            response = self.client.get("/api/dashboard?from=2025-12-01&to=2026-01-31")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["meta"]["rangeResolutionPath"], "sync_exact_historical_fastpath")
        self.assertEqual(payload["meta"]["skippedTiers"], ["comparative", "network", "predictive"])
        build_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
