from __future__ import annotations

import unittest
from types import SimpleNamespace

from social.store import SocialStore


class _OverviewStore(SocialStore):
    def __init__(self) -> None:
        self.client = None

    def _count_rows(self, table: str, *, filters=None) -> int:
        normalized_filters = tuple(
            (op, column, tuple(value) if isinstance(value, list) else value)
            for op, column, value in (filters or ())
        )
        key = (table, normalized_filters)
        counts = {
            ("social_activities", ()): 42,
            ("social_activities", (("eq", "platform", "facebook"),)): 10,
            ("social_activities", (("eq", "platform", "instagram"),)): 12,
            ("social_activities", (("eq", "platform", "google"),)): 13,
            ("social_activities", (("eq", "platform", "tiktok"),)): 7,
            ("social_activities", (("eq", "analysis_status", "not_needed"),)): 0,
            ("social_activities", (("eq", "analysis_status", "pending"),)): 4,
            ("social_activities", (("eq", "analysis_status", "analyzed"),)): 36,
            ("social_activities", (("eq", "analysis_status", "failed"),)): 1,
            ("social_activities", (("eq", "analysis_status", "dead_letter"),)): 1,
            ("social_activities", (("in", "analysis_status", ("pending", "failed")),)): 5,
            ("social_activities", (("in", "graph_status", ("pending", "failed")),)): 2,
            ("social_processing_failures", (("eq", "is_dead_letter", True), ("is", "resolved_at", "null"))): 3,
        }
        return counts.get(key, 0)

    def list_entities(self) -> list[dict]:
        return [
            {
                "id": "entity-1",
                "name": "Unibank",
                "is_active": True,
                "sources": [
                    {
                        "id": "source-1",
                        "platform": "facebook",
                        "is_active": True,
                        "health_status": "healthy",
                        "last_collected_at": "2026-04-18T09:00:00+00:00",
                    }
                ],
            },
            {
                "id": "entity-2",
                "name": "TBank",
                "is_active": True,
                "sources": [
                    {
                        "id": "source-2",
                        "platform": "instagram",
                        "is_active": True,
                        "health_status": "provider_404",
                        "last_collected_at": None,
                    }
                ],
            },
        ]

    def list_failures(self, *, dead_letter_only=False, stage=None, limit=100) -> list[dict]:
        return [{"id": "failure-1", "is_dead_letter": True, "stage": "ingest"}]

    def list_recent_runs(self, limit: int = 12) -> list[dict]:
        return [{"id": "run-1", "status": "succeeded"}]

    def list_activities(self, *args, **kwargs):  # pragma: no cover - should not be called
        raise AssertionError("get_overview should use authoritative counts, not sampled list_activities")


class _SourceCaptureStore(SocialStore):
    def __init__(self) -> None:
        self.client = None
        self.captured: tuple[str, list[dict]] | None = None

    def upsert_sources(self, entity_id: str, sources: list[dict]) -> list[dict]:
        self.captured = (entity_id, list(sources))
        return sources

    def get_entity(self, entity_id: str) -> dict:
        return {"id": entity_id, "accounts": []}


class _ActivityUpsertClient:
    def __init__(self) -> None:
        self.payloads: list[dict] = []

    def table(self, _name: str):
        return self

    def upsert(self, payloads, on_conflict=None):
        del on_conflict
        self.payloads = list(payloads)
        return self

    def execute(self):
        return SimpleNamespace(data=self.payloads)


class _ActivityUpsertStore(SocialStore):
    def __init__(self) -> None:
        self.client = _ActivityUpsertClient()

    def _select_rows(self, table: str, *, columns="*", filters=None, order_by=None, desc=False, limit=None) -> list[dict]:
        del columns, order_by, desc, limit
        if table != "social_activities":
            return []
        filter_map = {(op, column): value for op, column, value in (filters or ())}
        activity_uids = filter_map.get(("in", "activity_uid"))
        if activity_uids == ["social:new-uid"]:
            return []
        if activity_uids == ["facebook:ad:legacy-123"]:
            return [
                {
                    "id": "activity-legacy",
                    "activity_uid": "facebook:ad:legacy-123",
                    "provider_key": "scrapecreators",
                    "platform": "facebook",
                    "source_key": "scrapecreators:facebook:page_id:196765077044445",
                    "provider_item_id": "ad-123",
                    "source_kind": "ad",
                }
            ]
        return []

    def _single_row(self, table: str, *, columns="*", filters=None):
        del columns
        if table != "social_activities":
            return None
        expected = {
            ("eq", "provider_key"): "scrapecreators",
            ("eq", "platform"): "facebook",
            ("eq", "source_key"): "scrapecreators:facebook:page_id:196765077044445",
            ("eq", "provider_item_id"): "ad-123",
            ("eq", "source_kind"): "ad",
        }
        received = {(op, column): value for op, column, value in (filters or ())}
        if received == expected:
            return {
                "id": "activity-legacy",
                "activity_uid": "facebook:ad:legacy-123",
                "provider_key": "scrapecreators",
                "platform": "facebook",
                "source_key": "scrapecreators:facebook:page_id:196765077044445",
                "provider_item_id": "ad-123",
                "source_kind": "ad",
                "text_content": "Old text",
                "analysis_status": "analyzed",
                "graph_status": "synced",
                "analysis_version": "social-v1",
                "graph_projection_version": "social-graph-v1",
                "first_seen_at": "2026-04-17T09:00:00+00:00",
            }
        return None


class SocialStoreTests(unittest.TestCase):
    def test_get_overview_uses_authoritative_counts(self) -> None:
        store = _OverviewStore()
        payload = store.get_overview()

        self.assertEqual(payload["activities_total"], 42)
        self.assertEqual(payload["platform_counts"]["facebook"], 10)
        self.assertEqual(payload["queue_depth"]["analysis"], 5)
        self.assertEqual(payload["dead_letter_failures"], 3)
        self.assertEqual(payload["stale_entities"][0]["entity_id"], "entity-2")

    def test_upsert_accounts_maps_legacy_contract_to_source_registry_defaults(self) -> None:
        store = _SourceCaptureStore()
        store.upsert_accounts(
            "entity-1",
            [
                {
                    "platform": "facebook",
                    "account_external_id": "196765077044445",
                    "is_active": True,
                }
            ],
        )

        self.assertIsNotNone(store.captured)
        entity_id, sources = store.captured or ("", [])
        self.assertEqual(entity_id, "entity-1")
        self.assertEqual(sources[0]["provider_key"], "scrapecreators")
        self.assertEqual(sources[0]["target_type"], "page_id")
        self.assertEqual(sources[0]["content_types"], ["ad"])

    def test_upsert_activities_reuses_legacy_uid_for_matching_identity(self) -> None:
        store = _ActivityUpsertStore()
        store.upsert_activities(
            [
                {
                    "entity_id": "entity-1",
                    "account_id": "source-1",
                    "activity_uid": "social:new-uid",
                    "provider_key": "scrapecreators",
                    "source_key": "scrapecreators:facebook:page_id:196765077044445",
                    "platform": "facebook",
                    "source_kind": "ad",
                    "provider_item_id": "ad-123",
                    "source_url": "https://facebook.com/ad/123",
                    "text_content": "Zero monthly fees.",
                    "provider_context": {"provider": "scrapecreators"},
                    "provider_payload": {"ad_id": "ad-123"},
                    "engagement_metrics": {"likes": 2},
                    "assets": [],
                    "ingest_status": "normalized",
                    "normalization_version": "social-v2",
                }
            ]
        )

        self.assertEqual(store.client.payloads[0]["activity_uid"], "facebook:ad:legacy-123")
        self.assertEqual(store.client.payloads[0]["provider_key"], "scrapecreators")
        self.assertEqual(store.client.payloads[0]["source_key"], "scrapecreators:facebook:page_id:196765077044445")


if __name__ == "__main__":
    unittest.main()
