from __future__ import annotations

import unittest
from unittest.mock import patch

from buffer.supabase_writer import SupabaseWriter
from ingester.neo4j_writer import Neo4jWriter
from scraper import scrape_orchestrator


class _FilterQuery:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = [dict(row) for row in rows]
        self._eq_filters: list[tuple[str, object]] = []
        self._in_filters: list[tuple[str, tuple[object, ...]]] = []
        self._null_filters: list[tuple[str, bool]] = []
        self._order_field: str | None = None
        self._order_desc = False
        self._limit: int | None = None

    def select(self, _columns: str):
        return self

    def eq(self, field: str, value):
        self._eq_filters.append((field, value))
        return self

    def in_(self, field: str, values: list[object]):
        self._in_filters.append((field, tuple(values)))
        return self

    def is_(self, field: str, value: str):
        self._null_filters.append((field, str(value).lower() == "null"))
        return self

    def order(self, field: str, desc: bool = False):
        self._order_field = field
        self._order_desc = desc
        return self

    def limit(self, value: int):
        self._limit = int(value)
        return self

    def execute(self):
        rows = list(self._rows)
        for field, value in self._eq_filters:
            rows = [row for row in rows if row.get(field) == value]
        for field, values in self._in_filters:
            rows = [row for row in rows if row.get(field) in values]
        for field, should_be_null in self._null_filters:
            if should_be_null:
                rows = [row for row in rows if row.get(field) is None]
        if self._order_field:
            rows = sorted(rows, key=lambda row: str(row.get(self._order_field) or ""), reverse=self._order_desc)
        if self._limit is not None:
            rows = rows[:self._limit]
        return type("Resp", (), {"data": rows})()


class _BatchClient:
    def __init__(self, tables: dict[str, list[dict]]) -> None:
        self._tables = {name: [dict(row) for row in rows] for name, rows in tables.items()}

    def table(self, name: str):
        if name not in self._tables:
            raise AssertionError(f"Unexpected table: {name}")
        return _FilterQuery(self._tables[name])


class _FakeNeo4jResult:
    def consume(self):
        return None


class _FakeNeo4jTx:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def run(self, cypher: str, params: dict):
        self.calls.append((cypher, params))
        return _FakeNeo4jResult()


class BatchBundleAssemblyTests(unittest.TestCase):
    def test_get_post_bundles_batch_reuses_scoped_and_fallback_analyses(self) -> None:
        writer = object.__new__(SupabaseWriter)
        writer.client = _BatchClient(
            {
                "telegram_channels": [
                    {"id": "channel-1", "channel_username": "chan-1", "channel_title": "Channel 1"},
                ],
                "telegram_comments": [
                    {
                        "id": "comment-1",
                        "post_id": "post-1",
                        "channel_id": "channel-1",
                        "telegram_user_id": 10,
                        "telegram_message_id": 101,
                        "posted_at": "2026-04-09T10:00:00+00:00",
                        "text": "first",
                    },
                    {
                        "id": "comment-2",
                        "post_id": "post-1",
                        "channel_id": "channel-1",
                        "telegram_user_id": 20,
                        "telegram_message_id": 102,
                        "posted_at": "2026-04-09T10:05:00+00:00",
                        "text": "second",
                    },
                ],
                "ai_analysis": [
                    {
                        "id": "analysis-scoped",
                        "content_type": "batch",
                        "content_id": "post-1",
                        "channel_id": "channel-1",
                        "telegram_user_id": 10,
                        "created_at": "2026-04-09T10:10:00+00:00",
                        "raw_llm_response": {},
                    },
                    {
                        "id": "analysis-fallback",
                        "content_type": "batch",
                        "content_id": None,
                        "channel_id": "channel-1",
                        "telegram_user_id": 20,
                        "created_at": "2026-04-09T10:06:00+00:00",
                        "raw_llm_response": {},
                    },
                    {
                        "id": "analysis-post",
                        "content_type": "post",
                        "content_id": "post-1",
                        "created_at": "2026-04-09T10:15:00+00:00",
                        "raw_llm_response": {},
                    },
                ],
            }
        )

        bundles = writer.get_post_bundles_batch(
            [
                {
                    "id": "post-1",
                    "channel_id": "channel-1",
                    "posted_at": "2026-04-09T10:00:00+00:00",
                    "text": "post text",
                }
            ]
        )

        self.assertEqual(len(bundles), 1)
        bundle = bundles[0]
        self.assertEqual(bundle["channel"]["id"], "channel-1")
        self.assertEqual(bundle["analyses"]["10"]["id"], "analysis-scoped")
        self.assertEqual(bundle["analyses"]["20"]["id"], "analysis-fallback")
        self.assertEqual(bundle["post_analysis"]["id"], "analysis-post")
        self.assertEqual(bundle["reply_user_map"][101], 10)
        self.assertEqual(bundle["reply_user_map"][102], 20)


class Neo4jBatchWriterTests(unittest.TestCase):
    def test_sync_post_batch_uses_unwind_queries(self) -> None:
        calls: list[tuple[str, dict]] = []

        def fake_execute_write(work, *, driver_key, op_name):
            self.assertEqual(driver_key, "background")
            self.assertEqual(op_name, "writer.sync_post_batch")
            tx = _FakeNeo4jTx()
            work(tx)
            calls.extend(tx.calls)

        bundle = {
            "post": {
                "id": "post-1",
                "channel_id": "channel-1",
                "telegram_message_id": 77,
                "posted_at": "2026-04-09T10:00:00+00:00",
                "text": "post text",
                "comment_count": 1,
            },
            "channel": {
                "id": "channel-1",
                "channel_username": "chan-1",
                "channel_title": "Channel 1",
            },
            "comments": [
                {
                    "id": "comment-1",
                    "post_id": "post-1",
                    "telegram_message_id": 101,
                    "telegram_user_id": 10,
                    "posted_at": "2026-04-09T10:05:00+00:00",
                    "text": "comment text",
                }
            ],
            "analyses": {
                "10": {
                    "primary_intent": "Question",
                    "sentiment_score": 0.25,
                    "raw_llm_response": {},
                }
            },
            "post_analysis": {
                "id": "analysis-post",
                "topics": ["Housing Costs"],
                "raw_llm_response": {"sentiment": "negative"},
            },
            "reply_user_map": {},
        }

        with patch("ingester.neo4j_writer.db.execute_write", side_effect=fake_execute_write):
            Neo4jWriter().sync_post_batch([bundle])

        self.assertEqual(len(calls), 3)
        self.assertIn("UNWIND $rows AS row", calls[0][0])
        self.assertIn("UNWIND $rows AS row", calls[1][0])
        self.assertIn("UNWIND $rows AS row", calls[2][0])
        self.assertEqual(len(calls[0][1]["rows"]), 1)
        self.assertEqual(len(calls[1][1]["rows"]), 1)
        self.assertEqual(len(calls[2][1]["rows"]), 1)


class Neo4jBatchOrchestratorTests(unittest.TestCase):
    def test_run_ai_process_and_sync_uses_batch_helpers(self) -> None:
        class _FakeWriter:
            def __init__(self) -> None:
                self.batch_calls: list[list[dict]] = []
                self.single_calls = 0

            def sync_post_batch(self, bundles: list[dict]) -> None:
                self.batch_calls.append(list(bundles))

            def sync_bundle(self, bundle: dict) -> None:
                self.single_calls += 1

        class _FakeSupabase:
            def __init__(self) -> None:
                self.bulk_post_ids: list[str] = []
                self.bulk_analysis_ids: list[str] = []

            def get_unprocessed_comments(self, limit: int = 0):
                return []

            def get_unprocessed_posts(self, limit: int = 0):
                return []

            def get_unsynced_posts(self, limit: int = 0):
                return [
                    {"id": "post-1", "channel_id": "channel-1", "posted_at": "2026-04-09T10:00:00+00:00"},
                    {"id": "post-2", "channel_id": "channel-1", "posted_at": "2026-04-09T11:00:00+00:00"},
                ]

            def get_post_bundles_batch(self, posts: list[dict]):
                return [
                    {
                        "post": posts[0],
                        "channel": {"id": "channel-1"},
                        "comments": [],
                        "analyses": {},
                        "analysis_records": [{"id": "analysis-1"}],
                        "reply_user_map": {},
                    },
                    {
                        "post": posts[1],
                        "channel": {"id": "channel-1"},
                        "comments": [],
                        "analyses": {},
                        "analysis_records": [{"id": "analysis-2"}],
                        "reply_user_map": {},
                    },
                ]

            def mark_posts_neo4j_synced(self, post_ids: list[str]) -> int:
                self.bulk_post_ids.extend(post_ids)
                return len(post_ids)

            def mark_analyses_synced(self, analysis_ids: list[str]) -> int:
                self.bulk_analysis_ids.extend(analysis_ids)
                return len(analysis_ids)

            def reconcile_post_analysis_sync(self, limit: int = 0) -> int:
                return 0

        fake_writer = _FakeWriter()
        fake_supabase = _FakeSupabase()

        with patch("scraper.scrape_orchestrator._get_background_writer", return_value=fake_writer), \
             patch("scraper.scrape_orchestrator.config.NEO4J_SYNC_BATCH_CHUNK_SIZE", 2), \
             patch("scraper.scrape_orchestrator.extract_intents", return_value=0), \
             patch("scraper.scrape_orchestrator.extract_post_intents", return_value=0):
            result = scrape_orchestrator._run_ai_process_and_sync_blocking(
                fake_supabase,
                comment_limit=10,
                post_limit=10,
                sync_limit=10,
            )

        self.assertEqual(result["posts_synced"], 2)
        self.assertEqual(result["sync_errors"], 0)
        self.assertEqual(len(fake_writer.batch_calls), 1)
        self.assertEqual(fake_writer.single_calls, 0)
        self.assertEqual(fake_supabase.bulk_post_ids, ["post-1", "post-2"])
        self.assertEqual(fake_supabase.bulk_analysis_ids, ["analysis-1", "analysis-2"])


if __name__ == "__main__":
    unittest.main()
