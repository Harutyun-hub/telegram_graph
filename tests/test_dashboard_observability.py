from __future__ import annotations

import json
import time
import unittest
from contextlib import contextmanager
from typing import Any, Iterator
from unittest.mock import patch

from fastapi.testclient import TestClient
from loguru import logger

from api import aggregator, dashboard_observability as dashboard_obs, server


@contextmanager
def _capture_json_logs() -> Iterator[list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []

    def _sink(message) -> None:
        raw = message.record.get("message")
        if not isinstance(raw, str):
            return
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return
        if isinstance(payload, dict):
            records.append(payload)

    sink_id = logger.add(_sink, format="{message}")
    try:
        yield records
    finally:
        logger.remove(sink_id)


def _freshness_snapshot() -> dict[str, Any]:
    return {
        "generated_at": "2026-04-15T08:00:00+00:00",
        "pipeline": {
            "sync": {
                "status": "healthy",
                "age_minutes": 12,
                "last_graph_sync_at": "2026-04-15T08:00:00+00:00",
            },
            "process": {
                "status": "healthy",
                "age_minutes": 11,
            },
        },
    }


class DashboardProducerObservabilityApiTests(unittest.TestCase):
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
        aggregator.invalidate_cache()
        aggregator._refresh_states.clear()
        server._analytics_rate_limit_buckets.clear()

    def test_default_dashboard_warming_emits_correlated_request_and_refresh_events(self) -> None:
        freshness_resolution = {
            "snapshot": _freshness_snapshot(),
            "source": "memory",
            "snapshotBuiltAt": None,
            "persistedReadStatus": None,
            "persistedReadMs": None,
        }
        refresh_status = {
            "started": True,
            "inflight": False,
            "suppressed": False,
            "failureCount": 2,
            "buildId": "build-default-1",
            "globalInflightRefreshes": 1,
            "refreshSuppressedUntil": None,
        }

        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_cached_freshness_resolution", return_value=freshness_resolution), \
             patch.object(server, "_load_persisted_dashboard_snapshot", return_value={"status": "miss", "readMs": 0.0}), \
             patch.object(server, "_load_recent_default_dashboard_snapshot", return_value={"status": "miss", "readMs": 0.0}), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(server, "schedule_dashboard_snapshot_refresh", return_value=refresh_status), \
             patch.object(server, "_ensure_background_freshness_refresh", return_value=True), \
             _capture_json_logs() as logs:
            response = self.client.get("/api/dashboard")

        self.assertEqual(response.status_code, 503)
        request_id = response.headers.get("X-Request-ID")
        self.assertTrue(request_id)

        scheduled = [entry for entry in logs if entry.get("event") == "dashboard_default_refresh_scheduled"]
        self.assertEqual(len(scheduled), 1)
        scheduled_event = scheduled[0]
        self.assertEqual(scheduled_event["request_id"], request_id)
        self.assertEqual(scheduled_event["build_id"], "build-default-1")
        self.assertEqual(scheduled_event["reason"], "dashboard_request")
        self.assertTrue(scheduled_event["started"])
        self.assertFalse(scheduled_event["inflight"])
        self.assertFalse(scheduled_event["suppressed"])
        self.assertEqual(scheduled_event["failure_count"], 2)
        self.assertEqual(scheduled_event["global_inflight_refreshes"], 1)
        self.assertRegex(str(scheduled_event["cache_key"]), r"^\d{4}-\d{2}-\d{2}:\d{4}-\d{2}-\d{2}$")

        request_events = [entry for entry in logs if entry.get("event") == "dashboard_default_request"]
        self.assertEqual(len(request_events), 1)
        request_event = request_events[0]
        self.assertEqual(request_event["request_id"], request_id)
        self.assertEqual(request_event["request_status"], 503)
        self.assertEqual(request_event["cache_state_at_read"], "missing")
        self.assertEqual(request_event["cache_status"], "warming")
        self.assertEqual(request_event["trigger_reason"], "dashboard_request")
        self.assertRegex(str(request_event["default_cache_key"]), r"^\d{4}-\d{2}-\d{2}:\d{4}-\d{2}-\d{2}$")
        self.assertGreaterEqual(float(request_event["request_elapsed_ms"]), 0.0)

    def test_custom_range_warming_does_not_emit_default_producer_events(self) -> None:
        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(
                 server,
                 "_cached_freshness_resolution",
                 return_value={"snapshot": None, "source": None, "snapshotBuiltAt": None, "persistedReadStatus": None, "persistedReadMs": None},
             ), \
             patch.object(server, "peek_dashboard_snapshot", return_value=(None, None, "missing")), \
             patch.object(
                 server,
                 "schedule_dashboard_snapshot_refresh",
                 return_value={"started": True, "inflight": False, "suppressed": False, "failureCount": 0},
             ), \
             _capture_json_logs() as logs:
            response = self.client.get("/api/dashboard?from=2026-04-01&to=2026-04-15")

        self.assertEqual(response.status_code, 503)
        default_events = [entry for entry in logs if str(entry.get("event", "")).startswith("dashboard_default_")]
        self.assertEqual(default_events, [])


class DashboardProducerObservabilityBuildTests(unittest.TestCase):
    def setUp(self) -> None:
        aggregator.invalidate_cache()
        aggregator._refresh_states.clear()

    def _prepare_refresh_state(
        self,
        ctx,
        *,
        build_id: str | None,
        reason: str | None,
        request_id: str | None = None,
    ) -> None:
        state = aggregator.DashboardRefreshState(
            inflight=True,
            build_id=build_id,
            trigger_request_id=request_id,
            reason=reason,
            scheduled_at=time.time(),
        )
        state.event.clear()
        aggregator._refresh_states[ctx.cache_key] = state

    def test_background_default_build_emits_started_completed_and_query_summary(self) -> None:
        ctx = aggregator._default_dashboard_context()
        self._prepare_refresh_state(
            ctx,
            build_id="build-default-2",
            reason="dashboard_request",
            request_id="req-default-2",
        )

        def _fake_build(_ctx, *, skipped_tiers=None):
            self.assertIsNone(skipped_tiers)
            dashboard_obs.observe_query_family(
                "pulse.community_brief.analysis_rows",
                "supabase",
                lambda: [{"summary": "ok"}],
            )
            tier_times = {
                "pulse": 1.23,
                "strategic": None,
                "behavioral": None,
                "network": None,
                "psychographic": None,
                "predictive": None,
                "actionable": None,
                "comparative": None,
                "derived": 0.0,
            }
            snapshot = {
                "communityBrief": {"messagesAnalyzed": 3},
                "communityHealth": {"score": 55},
                "trendingTopics": [{"topic": "Radar"}],
            }
            return snapshot, tier_times, 1.23, "parallel"

        with patch.object(aggregator, "_build_snapshot_with_timeout", side_effect=_fake_build), \
             _capture_json_logs() as logs:
            aggregator._background_refresh_dashboard_snapshot(ctx.cache_key, ctx)

        started = [entry for entry in logs if entry.get("event") == "dashboard_default_build_started"]
        self.assertEqual(len(started), 1)
        self.assertEqual(started[0]["build_id"], "build-default-2")
        self.assertEqual(started[0]["trigger_request_id"], "req-default-2")

        query_events = [entry for entry in logs if entry.get("event") == "dashboard_default_query_family"]
        self.assertEqual(len(query_events), 1)
        self.assertEqual(query_events[0]["query_family"], "pulse.community_brief.analysis_rows")
        self.assertEqual(query_events[0]["backend"], "supabase")
        self.assertEqual(query_events[0]["status"], "ok")

        completed = [entry for entry in logs if entry.get("event") == "dashboard_default_build_completed"]
        self.assertEqual(len(completed), 1)
        completed_event = completed[0]
        self.assertEqual(completed_event["build_id"], "build-default-2")
        self.assertEqual(completed_event["reason"], "dashboard_request")
        self.assertEqual(completed_event["build_mode"], "parallel")
        self.assertTrue(completed_event["cache_written"])
        self.assertFalse(completed_event["cache_preserved"])
        self.assertFalse(completed_event["used_stale_fallback"])
        self.assertIn("tier_times", completed_event)
        self.assertIn("degraded_tiers", completed_event)

        summary_events = [entry for entry in logs if entry.get("event") == "dashboard_default_query_family_summary"]
        self.assertEqual(len(summary_events), 1)
        summary = summary_events[0]["query_families"]
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["query_family"], "pulse.community_brief.analysis_rows")
        self.assertEqual(summary[0]["backend"], "supabase")
        self.assertEqual(summary[0]["status"], "ok")
        self.assertEqual(summary[0]["attempts"], 1)

    def test_build_timeout_executor_preserves_default_build_context(self) -> None:
        ctx = aggregator._default_dashboard_context()
        build = dashboard_obs.DefaultProducerBuildContext(
            build_id="build-default-thread-1",
            cache_key=ctx.cache_key,
            reason="dashboard_request",
            trigger_request_id="req-thread-1",
        )
        seen: dict[str, Any] = {}

        def _fake_build(_ctx, _use_timeouts, *, skipped_tiers=None):
            self.assertIsNone(skipped_tiers)
            current = dashboard_obs.current_build_context()
            seen["build_id"] = current.build_id if current is not None else None
            seen["cache_key"] = current.cache_key if current is not None else None
            return {}, {"derived": 0.0}, 0.01, "parallel"

        with patch.object(aggregator, "_build_snapshot", side_effect=_fake_build), \
             dashboard_obs.bind_build_context(build):
            aggregator._build_snapshot_with_timeout(ctx)

        self.assertEqual(seen["build_id"], "build-default-thread-1")
        self.assertEqual(seen["cache_key"], ctx.cache_key)

    def test_non_default_refresh_context_emits_no_default_build_or_query_events(self) -> None:
        ctx = aggregator._default_dashboard_context()
        self._prepare_refresh_state(
            ctx,
            build_id=None,
            reason=None,
            request_id=None,
        )

        def _fake_build(_ctx, *, skipped_tiers=None):
            self.assertIsNone(skipped_tiers)
            dashboard_obs.observe_query_family(
                "pulse.community_brief.analysis_rows",
                "supabase",
                lambda: [{"summary": "ok"}],
            )
            tier_times = {
                "pulse": 1.0,
                "strategic": 1.0,
                "behavioral": 1.0,
                "network": 1.0,
                "psychographic": 1.0,
                "predictive": 1.0,
                "actionable": 1.0,
                "comparative": 1.0,
                "derived": 0.0,
            }
            snapshot = {
                "communityBrief": {"messagesAnalyzed": 3},
                "communityHealth": {"score": 55},
                "trendingTopics": [{"topic": "Radar"}],
            }
            return snapshot, tier_times, 1.0, "parallel"

        with patch.object(aggregator, "_build_snapshot_with_timeout", side_effect=_fake_build), \
             _capture_json_logs() as logs:
            aggregator._background_refresh_dashboard_snapshot(ctx.cache_key, ctx)

        default_events = [entry for entry in logs if str(entry.get("event", "")).startswith("dashboard_default_")]
        self.assertEqual(default_events, [])


if __name__ == "__main__":
    unittest.main()
