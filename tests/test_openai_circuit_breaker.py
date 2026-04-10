from __future__ import annotations

import json
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from processor import intent_extractor
from processor.intent_extractor import OpenAICircuitOpenError


class _CoordinatorStub:
    def __init__(self) -> None:
        self.json_values: dict[str, str] = {}
        self.counters: dict[str, int] = {}
        self.locks: dict[str, str] = {}

    def get_json(self, name: str):
        return self.json_values.get(name)

    def set_json(self, name: str, value: str, ttl_seconds: int) -> bool:
        del ttl_seconds
        self.json_values[name] = value
        return True

    def delete_json(self, name: str) -> bool:
        return self.json_values.pop(name, None) is not None

    def increment_window_counter(self, name: str, window_seconds: int) -> int:
        del window_seconds
        self.counters[name] = self.counters.get(name, 0) + 1
        return self.counters[name]

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


class _WriterStub:
    def __init__(self) -> None:
        self.failures: list[dict] = []
        self.processed_posts: list[str] = []
        self.saved: list[dict] = []

    def record_processing_failure(self, **kwargs) -> None:
        self.failures.append(kwargs)

    def clear_processing_failure(self, scope_type: str, scope_key: str) -> None:
        del scope_type, scope_key

    def get_blocked_scopes(self, scope_type: str, scope_keys: list[str]) -> set[str]:
        del scope_type, scope_keys
        return set()

    def mark_post_processed(self, post_id: str) -> None:
        self.processed_posts.append(post_id)

    def save_analysis(self, analysis: dict) -> None:
        self.saved.append(analysis)

    def get_posts_by_ids(self, post_ids: list[str]) -> dict[str, dict]:
        del post_ids
        return {}


class OpenAICircuitBreakerTests(unittest.TestCase):
    def _config_stack(self, stack: ExitStack) -> None:
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_BREAKER_ENABLED", True))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_RATE_LIMIT_THRESHOLD", 3))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_RATE_LIMIT_WINDOW_SECONDS", 60))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_PROVIDER_ERROR_THRESHOLD", 3))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_PROVIDER_ERROR_WINDOW_SECONDS", 60))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_RATE_LIMIT_OPEN_SECONDS", 300))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_PROVIDER_ERROR_OPEN_SECONDS", 120))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_QUOTA_OPEN_SECONDS", 1800))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_HALF_OPEN_TTL_SECONDS", 30))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_REOPEN_MULTIPLIER", 2.0))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_CIRCUIT_MAX_OPEN_SECONDS", 7200))
        stack.enter_context(patch.object(intent_extractor.config, "AI_REQUEST_MAX_RETRIES", 0))
        stack.enter_context(patch.object(intent_extractor.config, "AI_REQUEST_RETRY_BACKOFF_SECONDS", 0.0))
        stack.enter_context(patch.object(intent_extractor.config, "AI_REQUEST_TIMEOUT_SECONDS", 30))
        stack.enter_context(patch.object(intent_extractor.config, "OPENAI_MODEL", "gpt-test"))
        stack.enter_context(patch.object(intent_extractor.config, "FEATURE_EXTRACTION_V2", False))
        stack.enter_context(patch.object(intent_extractor.config, "AI_POST_BATCH_SIZE", 10))
        stack.enter_context(patch.object(intent_extractor.config, "AI_POST_WORKERS", 1))
        stack.enter_context(patch.object(intent_extractor.config, "AI_MAX_INFLIGHT_REQUESTS", 1))

    def test_request_json_short_circuits_when_circuit_is_open(self) -> None:
        coordinator = _CoordinatorStub()
        open_until = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        coordinator.set_json(
            intent_extractor._OPENAI_CIRCUIT_STATE_KEY,
            json.dumps({"state": "open", "reason": "rate_limit", "open_until": open_until}),
            300,
        )
        create = Mock()

        with ExitStack() as stack:
            self._config_stack(stack)
            stack.enter_context(patch.object(intent_extractor, "get_runtime_coordinator", return_value=coordinator))
            stack.enter_context(patch.object(intent_extractor.client.chat.completions, "create", create))
            with self.assertRaises(OpenAICircuitOpenError) as ctx:
                intent_extractor._request_json(
                    system_prompt="system",
                    user_context="user",
                    max_tokens=100,
                    request_label="test-open",
                )

        self.assertEqual(ctx.exception.reason, "rate_limit")
        create.assert_not_called()

    def test_request_json_opens_circuit_on_quota_error(self) -> None:
        coordinator = _CoordinatorStub()

        with ExitStack() as stack:
            self._config_stack(stack)
            stack.enter_context(patch.object(intent_extractor, "get_runtime_coordinator", return_value=coordinator))
            stack.enter_context(
                patch.object(
                    intent_extractor.client.chat.completions,
                    "create",
                    side_effect=RuntimeError("insufficient_quota: quota exhausted"),
                )
            )
            with self.assertRaises(OpenAICircuitOpenError) as ctx:
                intent_extractor._request_json(
                    system_prompt="system",
                    user_context="user",
                    max_tokens=100,
                    request_label="test-quota",
                )

        self.assertEqual(ctx.exception.reason, "insufficient_quota")
        state = json.loads(coordinator.get_json(intent_extractor._OPENAI_CIRCUIT_STATE_KEY) or "{}")
        self.assertEqual(state.get("state"), "open")
        self.assertEqual(state.get("reason"), "insufficient_quota")
        self.assertEqual(state.get("open_seconds"), 1800)

    def test_request_json_closes_circuit_after_successful_half_open_probe(self) -> None:
        coordinator = _CoordinatorStub()
        open_until = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
        coordinator.set_json(
            intent_extractor._OPENAI_CIRCUIT_STATE_KEY,
            json.dumps(
                {
                    "state": "open",
                    "reason": "rate_limit",
                    "open_until": open_until,
                    "open_seconds": 300,
                    "failure_count": 3,
                }
            ),
            300,
        )
        response = SimpleNamespace(
            id="resp_123",
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"ok": true}'))],
        )

        with ExitStack() as stack:
            self._config_stack(stack)
            stack.enter_context(patch.object(intent_extractor, "get_runtime_coordinator", return_value=coordinator))
            stack.enter_context(patch.object(intent_extractor.client.chat.completions, "create", return_value=response))
            result = intent_extractor._request_json(
                system_prompt="system",
                user_context="user",
                max_tokens=100,
                request_label="test-half-open",
            )

        self.assertEqual(result, {"ok": True})
        self.assertIsNone(coordinator.get_json(intent_extractor._OPENAI_CIRCUIT_STATE_KEY))
        self.assertEqual(coordinator.locks, {})

    def test_extract_intents_skips_failure_record_when_circuit_is_open(self) -> None:
        writer = _WriterStub()
        comments = [
            {
                "id": "comment-1",
                "telegram_user_id": 7,
                "channel_id": "channel-1",
                "post_id": "post-1",
                "text": "This is long enough to analyze.",
                "posted_at": "2026-04-10T12:00:00Z",
            }
        ]

        with patch.object(
            intent_extractor,
            "_analyze_comment_group_payload",
            side_effect=OpenAICircuitOpenError(reason="rate_limit", open_until="2026-04-10T12:05:00Z"),
        ):
            stats = intent_extractor.extract_intents(comments, writer, include_stats=True)

        self.assertEqual(stats["blocked_groups"], 1)
        self.assertEqual(stats["failed_groups"], 0)
        self.assertEqual(writer.failures, [])

    def test_extract_post_intents_marks_chunk_blocked_when_circuit_is_open(self) -> None:
        writer = _WriterStub()
        posts = [
            {"id": "post-1", "channel_id": "channel-1", "text": "A" * 30},
            {"id": "post-2", "channel_id": "channel-1", "text": "B" * 30},
        ]

        with ExitStack() as stack:
            self._config_stack(stack)
            stack.enter_context(
                patch.object(
                    intent_extractor,
                    "_analyze_post_batch_payload",
                    side_effect=OpenAICircuitOpenError(reason="rate_limit", open_until="2026-04-10T12:05:00Z"),
                )
            )
            stats = intent_extractor.extract_post_intents(posts, writer, include_stats=True)

        self.assertEqual(stats["blocked_posts"], 2)
        self.assertEqual(stats["failed_posts"], 0)
        self.assertEqual(stats["batch_failures"], 0)
        self.assertEqual(writer.failures, [])


if __name__ == "__main__":
    unittest.main()
