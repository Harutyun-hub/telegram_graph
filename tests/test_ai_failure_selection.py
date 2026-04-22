from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from buffer.supabase_writer import SupabaseWriter
from ingester import neo4j_writer
from scraper import scrape_orchestrator
from utils.taxonomy import iter_non_issue_topics


class _RetryQuery:
    def __init__(self, existing_keys: list[str]) -> None:
        self._existing_keys = existing_keys
        self.updated_payload: dict | None = None
        self.filters: list[tuple[str, object]] = []

    def select(self, _columns: str):
        return self

    def update(self, payload: dict):
        self.updated_payload = dict(payload)
        return self

    def eq(self, field: str, value):
        self.filters.append((field, value))
        return self

    def in_(self, field: str, values: list[str]):
        self.filters.append((field, tuple(values)))
        return self

    def execute(self):
        if self.updated_payload is None:
            return type("Resp", (), {"data": [{"scope_key": key} for key in self._existing_keys]})()
        return type("Resp", (), {"data": [{"scope_key": key} for key in self._existing_keys]})()


class _RetryClient:
    def __init__(self, existing_keys: list[str]) -> None:
        self.query = _RetryQuery(existing_keys)

    def table(self, name: str):
        if name != "ai_processing_failures":
            raise AssertionError(f"Unexpected table: {name}")
        return self.query


class _FallbackFailureQuery:
    def __init__(self) -> None:
        self.select_calls: list[str] = []

    def select(self, columns: str):
        self.select_calls.append(columns)
        if "failure_class" in columns:
            raise Exception("column ai_processing_failures.failure_class does not exist")
        return self

    def eq(self, _field: str, _value):
        return self

    def in_(self, _field: str, _values):
        return self

    def execute(self):
        return type(
            "Resp",
            (),
            {"data": [{"scope_key": "blocked-1", "is_dead_letter": True, "next_retry_at": None}]},
        )()


class _FallbackFailureClient:
    def __init__(self) -> None:
        self.query = _FallbackFailureQuery()

    def table(self, name: str):
        if name != "ai_processing_failures":
            raise AssertionError(f"Unexpected table: {name}")
        return self.query


class _FakeTopicWriteResult:
    def __init__(self, updated: int) -> None:
        self.updated = updated

    def single(self):
        return {"updated": self.updated}


class _FakeTopicWriteTx:
    def __init__(self, updated: int) -> None:
        self.updated = updated
        self.query = ""
        self.params: dict | None = None

    def run(self, query: str, params: dict):
        self.query = query
        self.params = dict(params)
        return _FakeTopicWriteResult(self.updated)


class SelectionHelpersTests(unittest.TestCase):
    def test_filter_out_blocked_rows_skips_dead_letter_items(self) -> None:
        writer = object.__new__(SupabaseWriter)
        writer.get_blocked_scopes = lambda scope_type, keys: {"blocked-1", "blocked-2"}

        rows = [
            {"id": "blocked-1"},
            {"id": "ok-1"},
            {"id": "blocked-2"},
            {"id": "ok-2"},
        ]

        selected = writer._filter_out_blocked_rows(
            rows,
            scope_type="post",
            scope_key_builder=lambda row: row.get("id"),
            limit=2,
        )

        self.assertEqual([row["id"] for row in selected], ["ok-1", "ok-2"])

    def test_retry_processing_failures_resets_attempt_counter(self) -> None:
        writer = object.__new__(SupabaseWriter)
        writer.client = _RetryClient(["post-1", "post-2"])
        writer._failure_table_warning_emitted = False
        writer._warn_failure_table_once = lambda error: (_ for _ in ()).throw(error)

        retried = writer.retry_processing_failures(scope_type="post", scope_keys=["post-1", "post-2"])

        self.assertEqual(retried, 2)
        self.assertIsNotNone(writer.client.query.updated_payload)
        self.assertEqual(writer.client.query.updated_payload.get("attempt_count"), 0)
        self.assertEqual(writer.client.query.updated_payload.get("is_dead_letter"), False)

    def test_get_blocked_scopes_falls_back_when_new_columns_are_missing(self) -> None:
        writer = object.__new__(SupabaseWriter)
        writer.client = _FallbackFailureClient()
        writer._failure_table_warning_emitted = False
        writer._warn_failure_table_once = lambda error: (_ for _ in ()).throw(error)

        blocked = writer.get_blocked_scopes("post", ["blocked-1", "ok-1"])

        self.assertEqual(blocked, {"blocked-1"})
        self.assertEqual(len(writer.client.query.select_calls), 2)

    def test_extract_message_topic_items_maps_category_keys_for_neo4j(self) -> None:
        raw_response = {
            "message_topics": [
                {
                    "comment_id": "comment-1",
                    "topics": [
                        {
                            "name": "Housing Costs",
                            "closest_category": "Housing & Infrastructure",
                            "domain": "Society",
                            "proposed": False,
                        }
                    ],
                }
            ]
        }

        items = neo4j_writer._extract_message_topic_items(raw_response, "comment-1")

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["name"], "Housing Cost")
        self.assertEqual(items[0]["category"], "Housing & Infrastructure")
        self.assertEqual(items[0]["domain"], "Society & Daily Life")
        self.assertIsNotNone(items[0]["category"])
        self.assertIsNotNone(items[0]["domain"])

    def test_mark_non_issue_topics_proposed_uses_exact_non_issue_set(self) -> None:
        captured: dict[str, object] = {}

        def fake_execute_write(work, *, driver_key, op_name):
            tx = _FakeTopicWriteTx(updated=4)
            captured["tx"] = tx
            captured["driver_key"] = driver_key
            captured["op_name"] = op_name
            return work(tx)

        with patch.object(neo4j_writer.db, "execute_write", side_effect=fake_execute_write):
            writer = neo4j_writer.Neo4jWriter()
            updated = writer.mark_non_issue_topics_proposed()

        self.assertEqual(updated, 4)
        self.assertEqual(captured["op_name"], "writer.mark_non_issue_topics_proposed")
        tx = captured["tx"]
        self.assertIsInstance(tx, _FakeTopicWriteTx)
        self.assertIn("SET t.proposed = true", tx.query)
        self.assertEqual(set(tx.params["non_issue_topics"]), set(iter_non_issue_topics()))

    def test_non_issue_cleanup_guard_runs_once_per_worker_process(self) -> None:
        writer = Mock()
        writer.mark_non_issue_topics_proposed.return_value = 7

        with patch.object(scrape_orchestrator, "_topic_cleanup_completed", False):
            first = scrape_orchestrator._ensure_non_issue_topics_hidden(writer)
            second = scrape_orchestrator._ensure_non_issue_topics_hidden(writer)

        self.assertEqual(first, 7)
        self.assertEqual(second, 0)
        writer.mark_non_issue_topics_proposed.assert_called_once_with()

    def test_emerging_topic_candidates_no_longer_leak_from_proposed_count_only(self) -> None:
        writer = object.__new__(SupabaseWriter)
        writer.list_topic_proposals = lambda status, limit: [
            {
                "topic_name": "Weak Topic",
                "closest_category": "General",
                "domain": "General",
                "proposed_count": 3,
                "distinct_content_count": 1,
                "distinct_user_count": 1,
                "distinct_channel_count": 1,
                "visibility_state": "candidate",
                "visibility_eligible": False,
                "last_seen_at": "2026-04-22T00:00:00Z",
            }
        ]

        candidates = writer.list_emerging_topic_candidates(limit=10)

        self.assertEqual(candidates, [])


if __name__ == "__main__":
    unittest.main()
