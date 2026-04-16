from __future__ import annotations

import time
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from loguru import logger
from neo4j.exceptions import ServiceUnavailable

from api import aggregator, dashboard_observability as dashboard_obs, db


@contextmanager
def _capture_log_messages():
    messages: list[str] = []

    def _sink(message) -> None:
        raw = message.record.get("message")
        if isinstance(raw, str):
            messages.append(raw)

    sink_id = logger.add(_sink, format="{message}")
    try:
        yield messages
    finally:
        logger.remove(sink_id)


class DetailCacheBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        aggregator.invalidate_cache()

    def tearDown(self) -> None:
        aggregator.invalidate_cache()

    def test_fresh_detail_cache_returns_without_builder(self) -> None:
        cache_key = "topics:test"
        aggregator._store_detail_cache_value(cache_key, [{"topic": "cached"}])

        def _boom():
            raise AssertionError("builder should not be called for fresh cache")

        payload = aggregator._get_cached_detail_value(cache_key, _boom)
        self.assertEqual(payload, [{"topic": "cached"}])

    def test_stale_detail_cache_serves_immediately_and_refreshes_once(self) -> None:
        cache_key = "topics:stale"
        aggregator._store_detail_cache_value(cache_key, [{"topic": "stale"}])
        with aggregator._detail_cache_lock:
            _ts, payload = aggregator._detail_cache[cache_key]
            aggregator._detail_cache[cache_key] = (
                time.time() - aggregator.DETAIL_CACHE_TTL_SECONDS - 5,
                payload,
            )

        refresh_calls = 0

        def _submit(fn):
            nonlocal refresh_calls
            refresh_calls += 1
            fn()

        with patch.object(aggregator, "submit_background", side_effect=_submit):
            payload = aggregator._get_cached_detail_value(
                cache_key,
                lambda: [{"topic": "fresh"}],
            )

        self.assertEqual(payload, [{"topic": "stale"}])
        self.assertEqual(refresh_calls, 1)
        fresh = aggregator._get_cached_detail_value(cache_key, lambda: [{"topic": "unused"}])
        self.assertEqual(fresh, [{"topic": "fresh"}])

    def test_expired_detail_cache_raises_when_refresh_fails(self) -> None:
        cache_key = "topics:expired"
        aggregator._store_detail_cache_value(cache_key, [{"topic": "expired"}])
        with aggregator._detail_cache_lock:
            _ts, payload = aggregator._detail_cache[cache_key]
            aggregator._detail_cache[cache_key] = (
                time.time() - aggregator.DETAIL_MAX_STALE_SECONDS - 5,
                payload,
            )

        with patch.object(aggregator, "_build_detail_with_timeout", side_effect=TimeoutError("boom")):
            with self.assertRaises(aggregator.DetailRefreshUnavailableError):
                aggregator._get_cached_detail_value(cache_key, lambda: [{"topic": "fresh"}])

    def test_topics_page_uses_extended_topics_cache_ttl(self) -> None:
        ctx = aggregator._default_dashboard_context()
        with patch.object(aggregator, "_get_cached_detail_value", return_value=[{"topic": "cached"}]) as cache_mock:
            payload = aggregator.get_topics_page(page=0, size=100, ctx=ctx)

        self.assertEqual(payload, [{"topic": "cached"}])
        self.assertEqual(cache_mock.call_args.kwargs["ttl_seconds"], aggregator.TOPICS_PAGE_CACHE_TTL_SECONDS)

    def test_topic_detail_injects_overview_fallback_when_backend_payload_has_none(self) -> None:
        ctx = aggregator._default_dashboard_context()
        raw_payload = {
            "name": "Armenian Government Performance",
            "category": "Government & Leadership",
            "mentionCount": 155,
            "currentMentions": 155,
            "previousMentions": 283,
            "growth7dPct": -45,
            "sentimentPositive": 3,
            "sentimentNegative": 83,
            "distinctUsers": 53,
            "distinctChannels": 10,
            "topChannels": ["Armenian Life", "Channel 2"],
            "evidence": [{"id": "msg-1", "timestamp": "2026-03-30T07:33:14Z"}],
        }

        with patch.object(aggregator, "_get_cached_detail_value", return_value=raw_payload):
            payload = aggregator.get_topic_detail("Armenian Government Performance", "Government & Leadership", ctx)

        self.assertIsNotNone(payload)
        self.assertEqual(payload["overview"]["status"], "fallback")
        self.assertEqual(payload["overview"]["windowStart"], ctx.from_date.isoformat())
        self.assertEqual(payload["overview"]["windowEnd"], ctx.to_date.isoformat())
        self.assertIn("Armenian Government Performance", payload["overview"]["summaryEn"])


class _DummySession:
    def __init__(self, *, value=None, error: Exception | None = None):
        self.value = value
        self.error = error

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute_read(self, work):
        if self.error is not None:
            raise self.error
        return work(object())

    def execute_write(self, work):
        if self.error is not None:
            raise self.error
        return work(object())


class _DummyDriver:
    def __init__(self, *, value=None, error: Exception | None = None):
        self.value = value
        self.error = error
        self.closed = False

    def session(self, **_kwargs):
        return _DummySession(value=self.value, error=self.error)

    def close(self):
        self.closed = True


class Neo4jDriverManagerTests(unittest.TestCase):
    def test_execute_read_retries_once_after_guarded_reset(self) -> None:
        manager = db.Neo4jDriverManager()
        bad_driver = _DummyDriver(error=ServiceUnavailable("defunct connection"))
        good_driver = _DummyDriver(value="ok")

        with patch.object(manager, "get_driver", side_effect=[bad_driver, good_driver]), \
             patch.object(manager, "reset_driver", return_value=True) as reset_mock:
            result = manager.execute_read(lambda _tx: "ok", op_name="unit-test")

        self.assertEqual(result, "ok")
        reset_mock.assert_called_once()

    def test_reset_driver_respects_cooldown(self) -> None:
        manager = db.Neo4jDriverManager()
        manager._drivers[db.REQUEST_DRIVER_KEY] = _DummyDriver()
        manager._reset_cooldowns[db.REQUEST_DRIVER_KEY] = time.monotonic()

        with patch.object(manager, "_create_driver") as create_mock:
            did_reset = manager.reset_driver(db.REQUEST_DRIVER_KEY, "boom")

        self.assertFalse(did_reset)
        create_mock.assert_not_called()

    def test_execute_read_logs_default_producer_context_on_failure_and_reset(self) -> None:
        manager = db.Neo4jDriverManager()
        manager._drivers[db.REQUEST_DRIVER_KEY] = _DummyDriver(error=ServiceUnavailable("defunct connection"))
        manager._reset_cooldowns[db.REQUEST_DRIVER_KEY] = (
            time.monotonic() - db.NEO4J_DRIVER_RESET_COOLDOWN_SECONDS - 1
        )

        with patch.object(manager, "_create_driver", return_value=_DummyDriver(value="ok")):
            build = dashboard_obs.DefaultProducerBuildContext(
                build_id="build-neo4j-1",
                cache_key="2026-04-01:2026-04-15",
                reason="dashboard_request",
                trigger_request_id="req-neo4j-1",
            )
            producer_context = build.producer_query_context(
                tier="network",
                query_family="network.key_voices.neo4j",
            )
            with _capture_log_messages() as messages:
                result = manager.execute_read(
                    lambda _tx: "ok",
                    op_name="network.key_voices.neo4j",
                    producer_context=producer_context,
                )

        self.assertEqual(result, "ok")
        joined = "\n".join(messages)
        self.assertIn("Neo4j read failed", joined)
        self.assertIn("Neo4j driver reset completed", joined)
        self.assertIn("buildId=build-neo4j-1", joined)
        self.assertIn("tier=network", joined)
        self.assertIn("queryFamily=network.key_voices.neo4j", joined)
        self.assertIn("cacheKey=2026-04-01:2026-04-15", joined)
        self.assertIn("triggerRequestId=req-neo4j-1", joined)
        self.assertIn("reason=dashboard_request", joined)


if __name__ == "__main__":
    unittest.main()
