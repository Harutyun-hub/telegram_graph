from __future__ import annotations

import unittest
from unittest.mock import patch

from buffer.supabase_writer import SupabaseWriter
from scraper import scrape_orchestrator


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class _FakeNotFilter:
    def __init__(self, query: "_FakeQuery") -> None:
        self.query = query

    def is_(self, field: str, value):
        normalized = None if value == "null" else value
        self.query.filters.append(("not_is", field, normalized))
        return self.query


class _FakeQuery:
    def __init__(self, client: "_FakeClient", table_name: str) -> None:
        self.client = client
        self.table_name = table_name
        self.filters: list[tuple[str, str, object]] = []
        self.operation = "select"
        self.payload = None
        self.order_field: str | None = None
        self.order_desc = False
        self.limit_n: int | None = None
        self.not_ = _FakeNotFilter(self)

    def select(self, _columns: str, count: str | None = None):
        del count
        self.operation = "select"
        return self

    def eq(self, field: str, value):
        self.filters.append(("eq", field, value))
        return self

    def in_(self, field: str, values):
        self.filters.append(("in", field, tuple(values)))
        return self

    def order(self, field: str, desc: bool = False):
        self.order_field = field
        self.order_desc = bool(desc)
        return self

    def limit(self, value: int):
        self.limit_n = int(value)
        return self

    def update(self, payload: dict):
        self.operation = "update"
        self.payload = dict(payload)
        return self

    def insert(self, payload):
        self.operation = "insert"
        self.payload = payload
        return self

    def _matches(self, row: dict) -> bool:
        for op, field, value in self.filters:
            row_value = row.get(field)
            if op == "eq":
                if row_value != value:
                    return False
            elif op == "in":
                if row_value not in value:
                    return False
            elif op == "not_is":
                if value is None:
                    if row_value is None:
                        return False
                elif row_value == value:
                    return False
            else:  # pragma: no cover - helper guard
                raise AssertionError(f"Unsupported filter op: {op}")
        return True

    def execute(self):
        rows = self.client.tables.setdefault(self.table_name, [])

        if self.operation == "select":
            matched = [dict(row) for row in rows if self._matches(row)]
            if self.order_field:
                matched.sort(key=lambda row: row.get(self.order_field) or "", reverse=self.order_desc)
            if self.limit_n is not None:
                matched = matched[:self.limit_n]
            return _FakeResponse(matched)

        if self.operation == "update":
            if self.table_name in self.client.fail_update_tables:
                raise RuntimeError(f"simulated update failure for {self.table_name}")
            updated: list[dict] = []
            for row in rows:
                if self._matches(row):
                    row.update(self.payload or {})
                    updated.append(dict(row))
            return _FakeResponse(updated)

        if self.operation == "insert":
            payloads = self.payload if isinstance(self.payload, list) else [self.payload]
            inserted: list[dict] = []
            for payload in payloads:
                row = dict(payload or {})
                row.setdefault("id", f"generated-{self.table_name}-{len(rows) + 1}")
                rows.append(row)
                inserted.append(dict(row))
            return _FakeResponse(inserted)

        raise AssertionError(f"Unsupported operation: {self.operation}")


class _FakeClient:
    def __init__(
        self,
        *,
        ai_analysis: list[dict] | None = None,
        telegram_posts: list[dict] | None = None,
        fail_update_tables: set[str] | None = None,
    ) -> None:
        self.tables = {
            "ai_analysis": [dict(row) for row in (ai_analysis or [])],
            "telegram_posts": [dict(row) for row in (telegram_posts or [])],
        }
        self.fail_update_tables = set(fail_update_tables or set())

    def table(self, name: str) -> _FakeQuery:
        return _FakeQuery(self, name)


class SaveAnalysisReplaySyncTests(unittest.TestCase):
    def _writer(
        self,
        *,
        ai_analysis: list[dict] | None = None,
        telegram_posts: list[dict] | None = None,
        fail_update_tables: set[str] | None = None,
    ) -> SupabaseWriter:
        writer = object.__new__(SupabaseWriter)
        writer.client = _FakeClient(
            ai_analysis=ai_analysis,
            telegram_posts=telegram_posts,
            fail_update_tables=fail_update_tables,
        )
        writer._register_topic_proposals_from_analysis = lambda *args, **kwargs: None
        return writer

    def test_inserting_post_analysis_marks_parent_post_graph_dirty(self) -> None:
        writer = self._writer(telegram_posts=[{"id": "post-1", "neo4j_synced": True}])

        inserted = writer.save_analysis(
            {
                "id": "analysis-1",
                "content_type": "post",
                "content_id": "post-1",
                "channel_id": "channel-1",
                "raw_llm_response": {"topics": []},
            }
        )

        self.assertEqual(inserted["id"], "analysis-1")
        self.assertFalse(writer.client.tables["telegram_posts"][0]["neo4j_synced"])
        self.assertFalse(writer.client.tables["ai_analysis"][0]["neo4j_synced"])

    def test_updating_post_analysis_marks_parent_post_graph_dirty(self) -> None:
        writer = self._writer(
            ai_analysis=[
                {
                    "id": "analysis-1",
                    "content_type": "post",
                    "content_id": "post-1",
                    "created_at": "2026-04-12T10:00:00+00:00",
                    "neo4j_synced": True,
                    "primary_intent": "Observation / Monitoring",
                    "raw_llm_response": {"topics": []},
                }
            ],
            telegram_posts=[{"id": "post-1", "neo4j_synced": True}],
        )

        writer.save_analysis(
            {
                "content_type": "post",
                "content_id": "post-1",
                "channel_id": "channel-1",
                "primary_intent": "Information Seeking",
                "raw_llm_response": {"topics": []},
            }
        )

        self.assertFalse(writer.client.tables["telegram_posts"][0]["neo4j_synced"])
        self.assertEqual(writer.client.tables["ai_analysis"][0]["primary_intent"], "Information Seeking")
        self.assertFalse(writer.client.tables["ai_analysis"][0]["neo4j_synced"])

    def test_inserting_scoped_batch_analysis_marks_parent_post_graph_dirty(self) -> None:
        writer = self._writer(telegram_posts=[{"id": "post-1", "neo4j_synced": True}])

        writer.save_analysis(
            {
                "id": "analysis-1",
                "content_type": "batch",
                "content_id": "post-1",
                "channel_id": "channel-1",
                "telegram_user_id": 123,
                "raw_llm_response": {"message_topics": []},
            }
        )

        self.assertFalse(writer.client.tables["telegram_posts"][0]["neo4j_synced"])
        self.assertFalse(writer.client.tables["ai_analysis"][0]["neo4j_synced"])

    def test_updating_scoped_batch_analysis_marks_parent_post_graph_dirty(self) -> None:
        writer = self._writer(
            ai_analysis=[
                {
                    "id": "analysis-1",
                    "content_type": "batch",
                    "content_id": "post-1",
                    "channel_id": "channel-1",
                    "telegram_user_id": 123,
                    "created_at": "2026-04-12T10:00:00+00:00",
                    "neo4j_synced": True,
                    "primary_intent": "Observation / Monitoring",
                    "raw_llm_response": {"message_topics": []},
                }
            ],
            telegram_posts=[{"id": "post-1", "neo4j_synced": True}],
        )

        writer.save_analysis(
            {
                "content_type": "batch",
                "content_id": "post-1",
                "channel_id": "channel-1",
                "telegram_user_id": 123,
                "primary_intent": "Coordination",
                "raw_llm_response": {"message_topics": []},
            }
        )

        self.assertFalse(writer.client.tables["telegram_posts"][0]["neo4j_synced"])
        self.assertEqual(writer.client.tables["ai_analysis"][0]["primary_intent"], "Coordination")
        self.assertFalse(writer.client.tables["ai_analysis"][0]["neo4j_synced"])

    def test_legacy_batch_without_content_id_does_not_dirty_any_parent_post(self) -> None:
        writer = self._writer(telegram_posts=[{"id": "post-1", "neo4j_synced": True}])

        inserted = writer.save_analysis(
            {
                "id": "analysis-1",
                "content_type": "batch",
                "channel_id": "channel-1",
                "telegram_user_id": 123,
                "raw_llm_response": {"message_topics": []},
            }
        )

        self.assertEqual(inserted["id"], "analysis-1")
        self.assertTrue(writer.client.tables["telegram_posts"][0]["neo4j_synced"])
        self.assertNotIn("neo4j_synced", writer.client.tables["ai_analysis"][0])

    def test_save_path_is_safe_when_parent_post_is_missing_or_non_updatable(self) -> None:
        cases = [
            {
                "label": "missing-parent",
                "telegram_posts": [],
                "fail_update_tables": set(),
            },
            {
                "label": "update-failure",
                "telegram_posts": [{"id": "post-1", "neo4j_synced": True}],
                "fail_update_tables": {"telegram_posts"},
            },
        ]

        for case in cases:
            with self.subTest(case["label"]):
                writer = self._writer(
                    telegram_posts=case["telegram_posts"],
                    fail_update_tables=case["fail_update_tables"],
                )

                inserted = writer.save_analysis(
                    {
                        "id": "analysis-1",
                        "content_type": "post",
                        "content_id": "post-1",
                        "channel_id": "channel-1",
                        "raw_llm_response": {"topics": []},
                    }
                )

                self.assertEqual(inserted["id"], "analysis-1")
                self.assertFalse(writer.client.tables["ai_analysis"][0]["neo4j_synced"])
                if case["telegram_posts"]:
                    self.assertTrue(writer.client.tables["telegram_posts"][0]["neo4j_synced"])
                else:
                    self.assertEqual(writer.client.tables["telegram_posts"], [])

    def test_reconcile_requeues_parent_posts_without_marking_analyses_synced(self) -> None:
        writer = self._writer(
            ai_analysis=[
                {
                    "id": "post-analysis-1",
                    "content_type": "post",
                    "content_id": "post-1",
                    "created_at": "2026-04-12T10:00:00+00:00",
                    "neo4j_synced": False,
                },
                {
                    "id": "batch-analysis-1",
                    "content_type": "batch",
                    "content_id": "post-2",
                    "created_at": "2026-04-12T10:01:00+00:00",
                    "neo4j_synced": False,
                },
            ],
            telegram_posts=[
                {"id": "post-1", "neo4j_synced": True},
                {"id": "post-2", "neo4j_synced": True},
            ],
        )

        reconciled = writer.reconcile_post_analysis_sync(limit=10)

        self.assertEqual(reconciled, 2)
        self.assertEqual(
            [row["neo4j_synced"] for row in writer.client.tables["telegram_posts"]],
            [False, False],
        )
        self.assertEqual(
            [row["neo4j_synced"] for row in writer.client.tables["ai_analysis"]],
            [False, False],
        )

    def test_reconcile_path_is_safe_when_parent_post_is_missing_or_non_updatable(self) -> None:
        cases = [
            {
                "label": "missing-parent",
                "telegram_posts": [],
                "fail_update_tables": set(),
                "expected_reconciled": 0,
            },
            {
                "label": "update-failure",
                "telegram_posts": [{"id": "post-1", "neo4j_synced": True}],
                "fail_update_tables": {"telegram_posts"},
                "expected_reconciled": 0,
            },
        ]

        for case in cases:
            with self.subTest(case["label"]):
                writer = self._writer(
                    ai_analysis=[
                        {
                            "id": "post-analysis-1",
                            "content_type": "post",
                            "content_id": "post-1",
                            "created_at": "2026-04-12T10:00:00+00:00",
                            "neo4j_synced": False,
                        }
                    ],
                    telegram_posts=case["telegram_posts"],
                    fail_update_tables=case["fail_update_tables"],
                )

                reconciled = writer.reconcile_post_analysis_sync(limit=10)

                self.assertEqual(reconciled, case["expected_reconciled"])
                self.assertFalse(writer.client.tables["ai_analysis"][0]["neo4j_synced"])
                if case["telegram_posts"]:
                    self.assertTrue(writer.client.tables["telegram_posts"][0]["neo4j_synced"])
                else:
                    self.assertEqual(writer.client.tables["telegram_posts"], [])


class ReplaySyncOrchestratorTests(unittest.TestCase):
    def test_normal_sync_marks_all_included_replay_relevant_analyses_synced(self) -> None:
        class _Writer:
            def __init__(self) -> None:
                self.marked_posts: list[str] = []
                self.marked_analyses: list[str] = []

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
                return [{"id": "post-1", "channel_id": "channel-1"}]

            def get_post_bundles_batch(self, posts):
                del posts
                return [
                    {
                        "post": {"id": "post-1"},
                        "comments": [],
                        "analyses": {},
                        "analysis_records": [
                            {"id": "post-analysis-1"},
                            {"id": "batch-analysis-1"},
                        ],
                    }
                ]

            def mark_posts_neo4j_synced(self, post_ids):
                self.marked_posts.extend(post_ids)
                return len(post_ids)

            def mark_analyses_synced(self, analysis_ids):
                self.marked_analyses.extend(analysis_ids)
                return len(analysis_ids)

            def reconcile_post_analysis_sync(self, limit=300):
                del limit
                return 0

        writer = _Writer()

        class _Neo4jWriter:
            def sync_post_batch(self, bundles):
                self.synced = [bundle["post"]["id"] for bundle in bundles]

        with patch.object(scrape_orchestrator, "_get_background_writer", return_value=_Neo4jWriter()), \
             patch.object(scrape_orchestrator.config, "NEO4J_SYNC_BATCH_CHUNK_SIZE", 20):
            result = scrape_orchestrator._run_ai_process_and_sync_blocking(
                writer,
                comment_limit=10,
                post_limit=10,
                sync_limit=10,
            )

        self.assertEqual(writer.marked_posts, ["post-1"])
        self.assertEqual(sorted(writer.marked_analyses), ["batch-analysis-1", "post-analysis-1"])
        self.assertEqual(result["posts_synced"], 1)


if __name__ == "__main__":
    unittest.main()
