from __future__ import annotations

import unittest
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from social.postgres_store import SocialPostgresStore


class _CursorStub:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self.rows: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, query: str, params: tuple) -> None:
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.rows)


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

    def test_claim_rows_are_json_safe_for_supabase_payloads(self) -> None:
        store, cursor = self._build_store()
        cursor.rows = [
            {
                "id": UUID("3384fe5d-a381-4f5e-9cb7-b95d38dce352"),
                "entity_id": UUID("52a644ef-0352-47b5-8c40-eefd14fa64e3"),
                "updated_at": datetime(2026, 4, 24, tzinfo=timezone.utc),
                "entity": {"id": UUID("52a644ef-0352-47b5-8c40-eefd14fa64e3")},
            }
        ]

        rows = store.claim_collect_accounts(worker_id="worker-1", platforms=["facebook"], limit=10, lease_seconds=60)

        self.assertEqual(rows[0]["id"], "3384fe5d-a381-4f5e-9cb7-b95d38dce352")
        self.assertEqual(rows[0]["entity_id"], "52a644ef-0352-47b5-8c40-eefd14fa64e3")
        self.assertEqual(rows[0]["updated_at"], "2026-04-24T00:00:00+00:00")
        self.assertEqual(rows[0]["entity"]["id"], "52a644ef-0352-47b5-8c40-eefd14fa64e3")


class SocialFailureScopeMigrationTests(unittest.TestCase):
    def test_failure_scope_backfill_targets_only_ingest_default_source_kinds(self) -> None:
        migration = Path("supabase/migrations/20260424_social_failure_scope_kind_backfill.sql").read_text()

        self.assertIn("failure.stage = 'ingest'", migration)
        self.assertIn("failure.scope_key = CONCAT(failure.entity_id::text, ':', failure.platform)", migration)
        self.assertIn("CONCAT(failure.entity_id::text, ':', failure.platform, ':', account.source_kind)", migration)
        self.assertIn("WHEN 'facebook' THEN 'meta_ads'", migration)
        self.assertIn("WHEN 'instagram' THEN 'instagram_profile'", migration)
        self.assertIn("WHEN 'google' THEN 'google_domain'", migration)
        self.assertIn("WHEN 'tiktok' THEN 'tiktok_profile'", migration)
        self.assertNotIn("failure.stage = 'analysis'", migration)
        self.assertNotIn("failure.stage = 'graph'", migration)


if __name__ == "__main__":
    unittest.main()
