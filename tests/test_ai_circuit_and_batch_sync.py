from __future__ import annotations

import json
import unittest
from unittest.mock import Mock, patch

from processor import intent_extractor
from scraper import scrape_orchestrator


class _CoordinatorStub:
    def __init__(self) -> None:
        self.json_values: dict[str, str] = {}
        self.locks: dict[str, str] = {}
        self.counters: dict[str, int] = {}

    def get_json(self, name: str):
        return self.json_values.get(name)

    def set_json(self, name: str, value: str, ttl_seconds: int) -> bool:
        del ttl_seconds
        self.json_values[name] = value
        return True

    def delete_json(self, name: str) -> bool:
        self.json_values.pop(name, None)
        return True

    def acquire_lock(self, name: str, ttl_seconds: int) -> str | None:
        del ttl_seconds
        if name in self.locks:
            return None
        token = f"{name}:{len(self.locks) + 1}"
        self.locks[name] = token
        return token

    def release_lock(self, name: str, token: str | None) -> None:
        if token and self.locks.get(name) == token:
            self.locks.pop(name, None)

    def increment_window_counter(self, name: str, window_seconds: int) -> int:
        del window_seconds
        value = int(self.counters.get(name, 0)) + 1
        self.counters[name] = value
        return value


class _QuotaError(Exception):
    def __init__(self) -> None:
        super().__init__("insufficient_quota")
        self.body = {"error": {"code": "insufficient_quota", "message": "quota exceeded"}}
        self.status_code = 429


class _ProviderError(Exception):
    def __init__(self) -> None:
        super().__init__("provider unavailable")
        self.body = {"error": {"code": "server_error", "message": "service unavailable"}}
        self.response = type("Response", (), {"status_code": 503})()


class OpenAICircuitBreakerTests(unittest.TestCase):
    def test_request_json_opens_quota_circuit_and_subsequent_call_is_blocked(self) -> None:
        coordinator = _CoordinatorStub()

        create_mock = Mock(side_effect=_QuotaError())
        fake_client = Mock()
        fake_client.chat.completions.create = create_mock

        with patch.object(intent_extractor, "client", fake_client), \
             patch.object(intent_extractor, "get_runtime_coordinator", return_value=coordinator), \
             patch.object(intent_extractor.config, "OPENAI_CIRCUIT_BREAKER_ENABLED", True), \
             patch.object(intent_extractor.config, "OPENAI_CIRCUIT_QUOTA_OPEN_SECONDS", 300), \
             patch.object(intent_extractor.config, "AI_REQUEST_MAX_RETRIES", 0):
            with self.assertRaises(intent_extractor.OpenAICircuitOpenError) as first_ctx:
                intent_extractor._request_json(
                    system_prompt="system",
                    user_context="user",
                    max_tokens=100,
                    request_label="quota-test",
                )

            self.assertEqual(first_ctx.exception.reason, "insufficient_quota")
            self.assertIn(intent_extractor._OPENAI_CIRCUIT_STATE_KEY, coordinator.json_values)
            self.assertEqual(create_mock.call_count, 1)

            with self.assertRaises(intent_extractor.OpenAICircuitOpenError) as second_ctx:
                intent_extractor._request_json(
                    system_prompt="system",
                    user_context="user",
                    max_tokens=100,
                    request_label="quota-test-second",
                )

            self.assertEqual(second_ctx.exception.reason, "insufficient_quota")
            self.assertEqual(create_mock.call_count, 1)

    def test_half_open_probe_success_closes_circuit(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.json_values[intent_extractor._OPENAI_CIRCUIT_STATE_KEY] = json.dumps(
            {
                "state": "open",
                "reason": "rate_limit",
                "open_until": "2026-04-10T00:00:00Z",
                "open_seconds": 60,
                "failure_count": 3,
            }
        )

        fake_response = Mock()
        fake_response.id = "resp-1"
        fake_response.choices = [Mock(message=Mock(content="{}"))]
        fake_client = Mock()
        fake_client.chat.completions.create = Mock(return_value=fake_response)

        with patch.object(intent_extractor, "client", fake_client), \
             patch.object(intent_extractor, "get_runtime_coordinator", return_value=coordinator), \
             patch.object(intent_extractor, "log_openai_usage"), \
             patch.object(intent_extractor.config, "OPENAI_CIRCUIT_BREAKER_ENABLED", True), \
             patch.object(intent_extractor.config, "AI_REQUEST_MAX_RETRIES", 0):
            parsed = intent_extractor._request_json(
                system_prompt="system",
                user_context="user",
                max_tokens=100,
                request_label="half-open-success",
            )

        self.assertEqual(parsed, {})
        self.assertNotIn(intent_extractor._OPENAI_CIRCUIT_STATE_KEY, coordinator.json_values)
        self.assertEqual(coordinator.locks, {})

    def test_half_open_probe_failure_reopens_circuit(self) -> None:
        coordinator = _CoordinatorStub()
        coordinator.json_values[intent_extractor._OPENAI_CIRCUIT_STATE_KEY] = json.dumps(
            {
                "state": "open",
                "reason": "provider_error",
                "open_until": "2026-04-10T00:00:00Z",
                "open_seconds": 60,
                "failure_count": 2,
            }
        )

        fake_client = Mock()
        fake_client.chat.completions.create = Mock(side_effect=_ProviderError())

        with patch.object(intent_extractor, "client", fake_client), \
             patch.object(intent_extractor, "get_runtime_coordinator", return_value=coordinator), \
             patch.object(intent_extractor.config, "OPENAI_CIRCUIT_BREAKER_ENABLED", True), \
             patch.object(intent_extractor.config, "AI_REQUEST_MAX_RETRIES", 0):
            with self.assertRaises(intent_extractor.OpenAICircuitOpenError) as ctx:
                intent_extractor._request_json(
                    system_prompt="system",
                    user_context="user",
                    max_tokens=100,
                    request_label="half-open-failure",
                )

        self.assertEqual(ctx.exception.reason, "provider_error")
        self.assertIn(intent_extractor._OPENAI_CIRCUIT_STATE_KEY, coordinator.json_values)
        self.assertEqual(coordinator.locks, {})


class Neo4jBatchSyncTests(unittest.TestCase):
    def test_orchestrator_uses_batch_sync_and_bulk_markers(self) -> None:
        class _Writer:
            def __init__(self) -> None:
                self.bulk_post_ids: list[str] = []
                self.bulk_analysis_ids: list[str] = []

            def auto_recover_transient_failures(self):
                return {}

            def get_unprocessed_comments(self, limit=200):
                del limit
                return []

            def get_unprocessed_posts(self, limit=100):
                del limit
                return []

            def get_unsynced_posts(self, limit=100):
                del limit
                return [
                    {"id": "post-1", "channel_id": "channel-1"},
                    {"id": "post-2", "channel_id": "channel-1"},
                ]

            def get_post_bundles_batch(self, posts):
                return [
                    {
                        "post": {"id": post["id"]},
                        "comments": [],
                        "analyses": {},
                        "analysis_records": [{"id": f"analysis-{post['id']}"}],
                    }
                    for post in posts
                ]

            def mark_posts_neo4j_synced(self, post_ids):
                self.bulk_post_ids.extend(post_ids)
                return len(post_ids)

            def mark_analyses_synced(self, analysis_ids):
                self.bulk_analysis_ids.extend(analysis_ids)
                return len(analysis_ids)

            def reconcile_post_analysis_sync(self, limit=300):
                del limit
                return 0

        writer = _Writer()
        batch_calls: list[list[str]] = []

        class _Neo4jWriter:
            def sync_post_batch(self, bundles):
                batch_calls.append([bundle["post"]["id"] for bundle in bundles])

            def sync_bundle(self, bundle):
                raise AssertionError(f"Unexpected single-post fallback: {bundle}")

        with patch.object(scrape_orchestrator, "_get_background_writer", return_value=_Neo4jWriter()), \
             patch.object(scrape_orchestrator.config, "NEO4J_SYNC_BATCH_CHUNK_SIZE", 20):
            result = scrape_orchestrator._run_ai_process_and_sync_blocking(
                writer,
                comment_limit=10,
                post_limit=10,
                sync_limit=10,
            )

        self.assertEqual(batch_calls, [["post-1", "post-2"]])
        self.assertEqual(writer.bulk_post_ids, ["post-1", "post-2"])
        self.assertEqual(writer.bulk_analysis_ids, ["analysis-post-1", "analysis-post-2"])
        self.assertEqual(result["posts_synced"], 2)
        self.assertEqual(result["sync_errors"], 0)


if __name__ == "__main__":
    unittest.main()
