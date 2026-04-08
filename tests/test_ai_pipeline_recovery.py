from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from api import freshness
from buffer.supabase_writer import (
    AI_FAILURE_CLASS_PERMANENT,
    AI_FAILURE_CLASS_TRANSIENT,
    SupabaseWriter,
    _classify_processing_error,
)
from scraper import scrape_orchestrator


class FailureClassificationTests(unittest.TestCase):
    def test_classify_transient_quota_error(self) -> None:
        result = _classify_processing_error(
            "Error code: 429 - {'error': {'code': 'insufficient_quota'}}"
        )
        self.assertEqual(result["failure_class"], AI_FAILURE_CLASS_TRANSIENT)
        self.assertEqual(result["error_code"], "openai_insufficient_quota")

    def test_classify_permanent_payload_error(self) -> None:
        result = _classify_processing_error("missing parsed payload for post id")
        self.assertEqual(result["failure_class"], AI_FAILURE_CLASS_PERMANENT)
        self.assertEqual(result["error_code"], "missing_parsed_payload")


class OrchestratorRecoveryTests(unittest.TestCase):
    def test_run_full_cycle_uses_runnable_backlog_for_backpressure(self) -> None:
        class _Writer:
            def get_backlog_counts(self):
                return {
                    "unprocessed_posts": 900,
                    "unprocessed_comments": 900,
                    "runnable_posts": 0,
                    "runnable_comment_groups": 0,
                }

        writer = _Writer()

        async def _run() -> dict:
            with patch.object(
                scrape_orchestrator,
                "run_scrape_cycle",
                AsyncMock(return_value={"channels_total": 1, "scrape_skipped": False}),
            ) as scrape_mock, patch.object(
                scrape_orchestrator,
                "run_ai_process_and_sync",
                AsyncMock(return_value={"ai_analysis_saved": 0, "posts_synced": 0}),
            ):
                result = await scrape_orchestrator.run_full_cycle(client=object(), supabase_writer=writer)
                self.assertEqual(scrape_mock.await_count, 1)
                return result

        result = asyncio.run(_run())
        self.assertFalse(result.get("scrape_skipped", False))

    def test_run_ai_process_and_sync_reconciles_even_without_unsynced_posts(self) -> None:
        class _Writer:
            def auto_recover_transient_failures(self):
                return {"post_retried": 3, "comment_group_retried": 4, "promoted_permanent": 1}

            def get_unprocessed_comments(self, limit=200):
                del limit
                return []

            def get_unprocessed_posts(self, limit=100):
                del limit
                return []

            def get_unsynced_posts(self, limit=100):
                del limit
                return []

            def reconcile_post_analysis_sync(self, limit=300):
                del limit
                return 7

        result = scrape_orchestrator._run_ai_process_and_sync_blocking(
            _Writer(),
            comment_limit=10,
            post_limit=10,
            sync_limit=10,
        )

        self.assertEqual(result["recovery_unlocked_posts"], 3)
        self.assertEqual(result["recovery_unlocked_comment_groups"], 4)
        self.assertEqual(result["recovery_promoted_permanent"], 1)
        self.assertEqual(result["post_analysis_reconciled"], 7)
        self.assertEqual(result["posts_pending_sync"], 0)


class FailureRetrySelectionTests(unittest.TestCase):
    def test_retry_processing_failures_resets_dead_letter_for_manual_retry(self) -> None:
        class _RetryQuery:
            def __init__(self) -> None:
                self.updated_payload: dict | None = None
                self.update_calls = 0

            def select(self, _columns: str):
                return self

            def update(self, payload: dict):
                self.updated_payload = dict(payload)
                self.update_calls += 1
                return self

            def eq(self, _field: str, _value):
                return self

            def in_(self, _field: str, _values):
                return self

            def execute(self):
                if self.update_calls == 0:
                    return type("Resp", (), {"data": [{"scope_key": "post-1"}]})()
                return type("Resp", (), {"data": [{"scope_key": "post-1"}]})()

        class _RetryClient:
            def __init__(self) -> None:
                self.query = _RetryQuery()

            def table(self, name: str):
                if name != "ai_processing_failures":
                    raise AssertionError(f"Unexpected table: {name}")
                return self.query

        writer = object.__new__(SupabaseWriter)
        writer.client = _RetryClient()
        writer._failure_table_warning_emitted = False
        writer._warn_failure_table_once = lambda error: (_ for _ in ()).throw(error)

        retried = writer.retry_processing_failures(scope_type="post", scope_keys=["post-1"])

        self.assertEqual(retried, 1)
        self.assertEqual(writer.client.query.updated_payload.get("is_dead_letter"), False)
        self.assertEqual(writer.client.query.updated_payload.get("attempt_count"), 0)
        self.assertIn("recovery_after_at", writer.client.query.updated_payload)


class FreshnessAIMetricsTests(unittest.TestCase):
    def test_snapshot_exposes_runnable_and_dead_letter_breakdown(self) -> None:
        class _Writer:
            def get_pipeline_freshness_snapshot(self):
                now = datetime.now(timezone.utc).isoformat()
                return {
                    "active_channels": 3,
                    "active_channels_never_scraped": 1,
                    "last_scrape_at": now,
                    "last_post_at": now,
                    "last_process_at": now,
                    "last_graph_sync_at": now,
                    "unprocessed_posts": 20,
                    "unprocessed_comments": 40,
                    "unsynced_posts": 0,
                    "unsynced_analysis": 12,
                    "dead_letter_scopes": 8,
                    "retry_blocked_scopes": 2,
                    "transient_dead_letter_scopes": 5,
                    "permanent_dead_letter_scopes": 3,
                    "recent_transient_failures": 4,
                    "recent_permanent_failures": 1,
                    "runnable_posts": 7,
                    "runnable_comment_groups": 6,
                    "blocked_dead_letter_posts": 4,
                    "blocked_dead_letter_comment_groups": 3,
                    "blocked_retry_posts": 1,
                    "blocked_retry_comment_groups": 1,
                }

            def get_source_resolution_snapshot(self, *, session_slot="primary"):
                del session_slot
                return {
                    "slot_key": "primary",
                    "due_jobs": 0,
                    "leased_jobs": 0,
                    "dead_letter_jobs": 0,
                    "stale_nonclaimable_jobs": 0,
                    "cooldown_slots": 0,
                    "cooldown_until": None,
                    "oldest_due_age_seconds": None,
                    "active_pending_sources": 0,
                    "active_missing_peer_refs": 0,
                }

            def get_recent_pipeline_snapshot(self):
                now = datetime.now(timezone.utc).isoformat()
                return {
                    "window_days": 15,
                    "window_start_at": now,
                    "recent_posts": 0,
                    "recent_comments": 0,
                    "recent_unsynced_posts": 0,
                    "recent_last_post_at": now,
                    "recent_last_graph_sync_post_at": now,
                }

        with patch.object(
            freshness,
            "_neo4j_snapshot",
            return_value={"recent_post_count": 0, "channel_count": 0, "topic_count": 0},
        ):
            snapshot = freshness.get_freshness_snapshot(
                _Writer(),
                scheduler_status={"is_active": True, "interval_minutes": 30, "running_now": False, "run_history": []},
                force_refresh=True,
            )

        self.assertEqual(snapshot["backlog"]["runnable_posts"], 7)
        self.assertEqual(snapshot["backlog"]["runnable_comment_groups"], 6)
        self.assertEqual(snapshot["backlog"]["transient_dead_letter_scopes"], 5)
        self.assertEqual(snapshot["pulse"]["queue"]["ai_items"], 13)
        self.assertEqual(snapshot["pulse"]["queue"]["ai_raw_items"], 60)


if __name__ == "__main__":
    unittest.main()
