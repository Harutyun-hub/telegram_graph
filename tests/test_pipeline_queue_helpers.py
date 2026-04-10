from __future__ import annotations

import unittest
from contextlib import contextmanager
from unittest.mock import patch

from buffer.supabase_writer import SupabaseWriter


class _FakeCursor:
    def __init__(self, results: list[object]) -> None:
        self._results = list(results)
        self.calls: list[tuple[str, tuple | None]] = []

    def execute(self, sql: str, params=None):
        self.calls.append((sql, params))
        return self

    def fetchall(self):
        if not self._results:
            return []
        value = self._results.pop(0)
        if isinstance(value, list):
            return value
        return []

    def fetchone(self):
        if not self._results:
            return None
        value = self._results.pop(0)
        if isinstance(value, list):
            return value[0] if value else None
        return value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    @contextmanager
    def transaction(self):
        yield self

    def cursor(self):
        return self._cursor


class PipelineQueueHelpersTests(unittest.TestCase):
    def test_repair_pipeline_stage_queues_returns_counts(self) -> None:
        writer = object.__new__(SupabaseWriter)
        writer.enqueue_ai_post_jobs = lambda limit=None: 2
        writer.enqueue_ai_comment_group_jobs = lambda limit=None: 3
        writer.enqueue_neo4j_sync_jobs = lambda limit=None: 4

        result = writer.repair_pipeline_stage_queues(limit=100)

        self.assertEqual(
            result,
            {
                "ai_post_jobs_enqueued": 2,
                "ai_comment_group_jobs_enqueued": 3,
                "neo4j_sync_jobs_enqueued": 4,
            },
        )

    def test_claim_ai_post_jobs_uses_worker_and_batch(self) -> None:
        writer = object.__new__(SupabaseWriter)
        writer._pipeline_queue_warning_emitted = set()
        writer._warn_pipeline_queue_once = lambda queue_name, error: (_ for _ in ()).throw(error)
        cursor = _FakeCursor([[{"id": "job-1", "lease_token": "token-1"}]])

        @contextmanager
        def fake_pipeline_connection():
            yield _FakeConnection(cursor)

        writer._pipeline_connection = fake_pipeline_connection

        claimed = writer.claim_ai_post_jobs(worker_id="worker-1", batch_size=5, lease_seconds=120)

        self.assertEqual(len(claimed), 1)
        sql, params = cursor.calls[0]
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertEqual(params, (5, "worker-1", 120))

    def test_nack_pipeline_job_dead_letters_at_max_attempts(self) -> None:
        writer = object.__new__(SupabaseWriter)
        writer._pipeline_queue_warning_emitted = set()
        writer._warn_pipeline_queue_once = lambda queue_name, error: (_ for _ in ()).throw(error)
        cursor = _FakeCursor(
            [
                {"id": "job-1", "attempt_count": 2},
                {"id": "job-1", "status": "dead_lettered", "attempt_count": 3},
            ]
        )

        @contextmanager
        def fake_pipeline_connection():
            yield _FakeConnection(cursor)

        writer._pipeline_connection = fake_pipeline_connection

        with patch("buffer.supabase_writer.config.PIPELINE_QUEUE_MAX_ATTEMPTS", 3):
            result = writer.nack_ai_post_job("job-1", "00000000-0000-0000-0000-000000000123", "boom")

        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "dead_lettered")
        self.assertEqual(result["attempt_count"], 3)
        update_sql, update_params = cursor.calls[1]
        self.assertIn("dead_lettered", update_params)
        self.assertIn("UPDATE public.ai_post_jobs", update_sql)

    def test_claim_neo4j_sync_jobs_enforces_processed_unsynced_guard(self) -> None:
        writer = object.__new__(SupabaseWriter)
        writer._pipeline_queue_warning_emitted = set()
        writer._warn_pipeline_queue_once = lambda queue_name, error: (_ for _ in ()).throw(error)
        cursor = _FakeCursor([[{"id": "job-2", "lease_token": "token-2"}]])

        @contextmanager
        def fake_pipeline_connection():
            yield _FakeConnection(cursor)

        writer._pipeline_connection = fake_pipeline_connection

        claimed = writer.claim_neo4j_sync_jobs(worker_id="worker-neo4j", batch_size=3, lease_seconds=180)

        self.assertEqual(len(claimed), 1)
        sql, params = cursor.calls[0]
        self.assertIn("JOIN public.telegram_posts AS post", sql)
        self.assertIn("post.is_processed = TRUE", sql)
        self.assertIn("post.neo4j_synced = FALSE", sql)
        self.assertEqual(params, (3, "worker-neo4j", 180))


if __name__ == "__main__":
    unittest.main()
