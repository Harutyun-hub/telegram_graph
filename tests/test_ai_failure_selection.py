from __future__ import annotations

import unittest

from buffer.supabase_writer import SupabaseWriter
from ingester import neo4j_writer


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


if __name__ == "__main__":
    unittest.main()
