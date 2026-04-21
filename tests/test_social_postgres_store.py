from __future__ import annotations

import unittest

from social.postgres_store import SocialPostgresStore


class _CursorStub:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, query: str, params: tuple) -> None:
        self.executed.append((query, params))

    def fetchall(self):
        return []


class _TransactionStub:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _ConnectionStub:
    def __init__(self, cursor: _CursorStub) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def transaction(self):
        return _TransactionStub()

    def cursor(self):
        return self._cursor

    def close(self) -> None:
        return None


class _StoreUnderTest(SocialPostgresStore):
    def __init__(self, cursor: _CursorStub) -> None:
        super().__init__(database_url="postgresql://example")
        self._cursor = cursor

    @property
    def enabled(self) -> bool:  # type: ignore[override]
        return True

    def _connection(self):  # type: ignore[override]
        return _ConnectionStub(self._cursor)


class SocialPostgresStoreQueryTests(unittest.TestCase):
    def _build_store(self) -> tuple[SocialPostgresStore, _CursorStub]:
        cursor = _CursorStub()
        return _StoreUnderTest(cursor), cursor

    def test_claim_queries_avoid_outer_join_failure_filter(self) -> None:
        store, cursor = self._build_store()

        store.claim_collect_accounts(worker_id="worker-1", platforms=["facebook"], limit=10, lease_seconds=60)
        store.claim_analysis_activities(worker_id="worker-1", limit=20, lease_seconds=90, analysis_version="social-v1")
        store.claim_graph_activities(worker_id="worker-1", limit=20, lease_seconds=120, projection_version="social-graph-v1")

        self.assertEqual(len(cursor.executed), 3)
        expected_params = [
            (["facebook"], 60, 10, "worker-1"),
            ("social-v1", 90, 20, "worker-1"),
            ("social-graph-v1", 120, 20, "worker-1"),
        ]
        for index, (query, params) in enumerate(cursor.executed):
            self.assertIn("NOT EXISTS", query)
            self.assertNotIn("LEFT JOIN public.social_processing_failures", query)
            self.assertIn("FOR UPDATE SKIP LOCKED", query)
            self.assertEqual(params, expected_params[index])


if __name__ == "__main__":
    unittest.main()
