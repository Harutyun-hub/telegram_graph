from __future__ import annotations

from datetime import date
from types import SimpleNamespace
import unittest
from unittest.mock import ANY, patch

from fastapi.testclient import TestClient

from api import server, topic_overviews
from api.dashboard_dates import build_dashboard_date_context
from api.queries import comparative


def _candidate(topic: str, *, mentions: int = 12, previous_mentions: int = 6) -> dict:
    return {
        "topic": topic,
        "category": "Government & Leadership",
        "mentions": mentions,
        "previousMentions": previous_mentions,
        "growth": 50,
        "distinctUsers": 5,
        "distinctChannels": 3,
        "evidenceCount": mentions,
        "latestAt": "2026-03-27T10:00:00Z",
        "topChannels": ["Channel A", "Channel B"],
        "sentimentPositive": 8,
        "sentimentNeutral": 14,
        "sentimentNegative": 78,
        "qualityTier": "high",
        "evidence": [
            {
                "id": f"{topic}-ev-1",
                "type": "message",
                "author": "author-1",
                "channel": "Channel A",
                "text": f"{topic} evidence one",
                "timestamp": "2026-03-27T09:00:00Z",
                "reactions": 10,
                "replies": 2,
            },
            {
                "id": f"{topic}-ev-2",
                "type": "reply",
                "author": "author-2",
                "channel": "Channel B",
                "text": f"{topic} evidence two",
                "timestamp": "2026-03-27T08:00:00Z",
                "reactions": 0,
                "replies": 0,
            },
        ],
        "questionEvidence": [
            {
                "id": f"{topic}-q-1",
                "type": "reply",
                "author": "author-3",
                "channel": "Channel A",
                "text": f"What is happening with {topic}?",
                "timestamp": "2026-03-27T07:00:00Z",
                "reactions": 0,
                "replies": 0,
            }
        ],
    }


def _ready_item(topic: str, ctx) -> dict:
    return {
        "topic": topic,
        "category": "Government & Leadership",
        "status": "ready",
        "summaryEn": f"{topic} summary",
        "summaryRu": f"{topic} сводка",
        "signalsEn": ["One", "Two", "Three"],
        "signalsRu": ["Один", "Два", "Три"],
        "generatedAt": "2026-03-27T12:00:00Z",
        "windowStart": ctx.from_date.isoformat(),
        "windowEnd": ctx.to_date.isoformat(),
        "windowDays": ctx.days,
        "evidenceIds": [f"{topic}-ev-1"],
    }


class _FakeRuntimeStore:
    def __init__(self) -> None:
        self.files: dict[str, dict] = {}
        self.counter = 0

    def save_runtime_json(self, path: str, payload: dict) -> bool:
        self.counter += 1
        self.files[path] = {
            "payload": payload,
            "updated_at": f"2026-03-27T12:00:{self.counter:02d}Z",
        }
        return True

    def get_runtime_json(self, path: str, default: dict | None = None) -> dict:
        row = self.files.get(path)
        if not row:
            return dict(default or {})
        return row["payload"]

    def list_runtime_files(self, folder: str) -> list[dict]:
        prefix = f"{folder}/"
        rows = []
        for path, row in self.files.items():
            if path.startswith(prefix):
                rows.append(
                    {
                        "name": path[len(prefix):],
                        "updated_at": row["updated_at"],
                    }
                )
        return rows

    def delete_runtime_files(self, paths: list[str]) -> int:
        for path in paths:
            self.files.pop(path, None)
        return len(paths)


class TopicOverviewTests(unittest.TestCase):
    def setUp(self) -> None:
        topic_overviews.invalidate_topic_overviews_cache()

    def test_refresh_only_generates_changed_topics(self) -> None:
        ctx = build_dashboard_date_context("2026-03-10", "2026-03-24")
        first = _candidate("Topic One")
        second = _candidate("Topic Two", mentions=15, previous_mentions=5)
        first_key = topic_overviews._topic_key(first["topic"], first["category"])
        first_fp = topic_overviews._candidate_fingerprint(first, ctx)
        existing_item = _ready_item("Topic One", ctx)
        state = {
            "schemaVersion": 1,
            "updatedAt": "2026-03-27T11:00:00Z",
            "topics": {
                first_key: {
                    "fingerprint": first_fp,
                    "status": "ready",
                    "updatedAt": "2026-03-27T11:00:00Z",
                    "item": existing_item,
                    "topic": "Topic One",
                }
            },
        }

        with patch.object(topic_overviews.comparative, "get_topic_overview_candidates", return_value=[first, second]), \
             patch.object(topic_overviews, "_acquire_refresh_lease", return_value=True), \
             patch.object(topic_overviews, "_load_state", return_value=state), \
             patch.object(topic_overviews, "_load_snapshot_payload", return_value=topic_overviews._default_snapshot_payload()), \
             patch.object(topic_overviews, "_save_state", return_value=True), \
             patch.object(topic_overviews, "_save_snapshot_payload", return_value=True), \
             patch.object(topic_overviews, "_generate_item", return_value=_ready_item("Topic Two", ctx)) as generate_mock:
            payload = topic_overviews.refresh_topic_overviews(ctx=ctx, force=False)

        self.assertEqual(generate_mock.call_count, 1)
        generated_candidate = generate_mock.call_args[0][0]
        self.assertEqual(generated_candidate["topic"], "Topic Two")
        self.assertEqual(len(payload["items"]), 2)
        self.assertEqual(payload["items"][0]["topic"], "Topic One")
        self.assertEqual(payload["items"][1]["topic"], "Topic Two")

    def test_snapshot_round_trip_uses_runtime_store(self) -> None:
        store = _FakeRuntimeStore()
        ctx = build_dashboard_date_context("2026-03-10", "2026-03-24")
        diagnostics = topic_overviews._new_refresh_diagnostics(force=True, ctx=ctx)
        item = _ready_item("Topic One", ctx)

        with patch.object(topic_overviews, "_get_runtime_store", return_value=store):
            saved = topic_overviews._save_snapshot_payload([item], ctx, metadata={"finalTopics": 1}, diagnostics=diagnostics)
            loaded = topic_overviews._load_snapshot_payload(diagnostics=diagnostics)

        self.assertTrue(saved)
        self.assertEqual(len(loaded["items"]), 1)
        self.assertEqual(loaded["items"][0]["topic"], "Topic One")
        self.assertEqual(diagnostics["snapshot"]["writeSucceeded"], True)
        self.assertEqual(diagnostics["snapshot"]["loadedItems"], 1)

    def test_empty_snapshot_is_cached_in_memory(self) -> None:
        empty_snapshot = topic_overviews._default_snapshot_payload()
        empty_snapshot["generatedAt"] = "2026-03-27T12:00:00Z"

        with patch.object(topic_overviews, "_load_snapshot_payload", return_value=empty_snapshot) as load_mock:
            first = topic_overviews.get_topic_overview("Missing Topic", "General")
            second = topic_overviews.get_topic_overview("Missing Topic", "General")

        self.assertIsNone(first)
        self.assertIsNone(second)
        self.assertEqual(load_mock.call_count, 1)

    def test_get_topic_overview_uses_ready_item_when_windows_overlap(self) -> None:
        requested_ctx = build_dashboard_date_context("2026-03-15", "2026-03-22")
        old_ctx = build_dashboard_date_context("2026-03-10", "2026-03-24")
        payload = topic_overviews._default_snapshot_payload()
        payload["items"] = [_ready_item("Topic One", old_ctx)]

        with patch.object(topic_overviews, "_load_snapshot_payload", return_value=payload):
            overview = topic_overviews.get_topic_overview(
                "Topic One",
                "Government & Leadership",
                ctx=requested_ctx,
            )

        self.assertIsNotNone(overview)
        self.assertEqual(overview["status"], "ready")

    def test_get_topic_overview_ignores_ready_item_when_windows_do_not_overlap(self) -> None:
        requested_ctx = build_dashboard_date_context("2026-04-13", "2026-04-15")
        old_ctx = build_dashboard_date_context("2026-03-10", "2026-03-24")
        payload = topic_overviews._default_snapshot_payload()
        payload["items"] = [_ready_item("Topic One", old_ctx)]

        with patch.object(topic_overviews, "_load_snapshot_payload", return_value=payload):
            overview = topic_overviews.get_topic_overview(
                "Topic One",
                "Government & Leadership",
                ctx=requested_ctx,
            )

        self.assertIsNone(overview)

class TopicQueryPathTests(unittest.TestCase):
    def test_candidate_mapping_uses_safe_string_conversion(self) -> None:
        ctx = build_dashboard_date_context("2026-03-10", "2026-03-24")
        row = {
            "name": "Topic One",
            "category": "Government & Leadership",
            "latestAt": "2026-03-24T12:00:00Z",
            "topChannels": ["Channel A", "Channel B"],
            "evidence": [
                {
                    "id": "ev-1",
                    "type": "message",
                    "author": "author-1",
                    "channel": "Channel A",
                    "text": "Evidence one",
                    "timestamp": "2026-03-24T11:00:00Z",
                    "reactions": 4,
                    "replies": 1,
                }
            ],
            "questionEvidence": [
                {
                    "id": "q-1",
                    "type": "reply",
                    "author": "author-2",
                    "channel": "Channel A",
                    "text": "What changed?",
                    "timestamp": "2026-03-24T10:00:00Z",
                    "reactions": 0,
                    "replies": 0,
                }
            ],
            "growth7dPct": 25.0,
            "sentimentPositive": 10,
            "sentimentNeutral": 20,
            "sentimentNegative": 70,
        }
        decorated = {
            "name": "Topic One",
            "sourceTopic": "Topic One",
            "category": "Government & Leadership",
            "topicGroup": "Admin",
            "mentionCount": 12,
            "currentMentions": 12,
            "prev7Mentions": 6,
            "previousMentions": 6,
            "distinctUsers": 5,
            "distinctChannels": 3,
            "evidenceCount": 12,
        }

        with patch.object(comparative, "run_query", return_value=[row]), \
             patch.object(comparative, "_decorate_topics_page_row", return_value=decorated), \
             patch.object(comparative, "_is_topics_page_row_allowed", return_value=True):
            candidates = comparative.get_topic_overview_candidates(
                ctx,
                limit=5,
                evidence_limit=3,
                question_limit=2,
            )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["topic"], "Topic One")
        self.assertEqual(candidates[0]["sourceTopic"], "Topic One")
        self.assertEqual(candidates[0]["topChannels"], ["Channel A", "Channel B"])

    def test_build_fallback_topic_overview_from_detail_payload(self) -> None:
        ctx = build_dashboard_date_context("2026-03-10", "2026-03-24")
        detail_payload = {
            "name": "Visa And Residency",
            "sourceTopic": "Visa And Residency",
            "category": "Emigration",
            "mentions": 185,
            "previousMentions": 92,
            "growth7dPct": 101,
            "distinctUsers": 48,
            "distinctChannels": 3,
            "evidenceCount": 185,
            "sentimentPositive": 8,
            "sentimentNeutral": 78,
            "sentimentNegative": 14,
            "topChannels": ["Channel A", "Channel B"],
            "latestAt": "2026-04-15T16:50:43Z",
            "evidence": [
                {"id": "ev-1", "text": "Evidence one", "channel": "Channel A", "timestamp": "2026-04-15T16:50:43Z"},
                {"id": "ev-2", "text": "Evidence two", "channel": "Channel B", "timestamp": "2026-04-15T16:40:43Z"},
            ],
            "questionEvidence": [
                {"id": "q-1", "text": "What visa should I choose?", "channel": "Channel A", "timestamp": "2026-04-15T16:30:43Z"},
            ],
        }

        overview = topic_overviews.build_fallback_topic_overview(detail_payload, ctx)

        self.assertIsNotNone(overview)
        self.assertEqual(overview["topic"], "Visa And Residency")
        self.assertEqual(overview["category"], "Emigration")
        self.assertEqual(overview["status"], "fallback")
        self.assertTrue(overview["summaryEn"])
        self.assertEqual(len(overview["signalsEn"]), 3)

    def test_refresh_uses_broader_candidate_limit_than_small_default_cap(self) -> None:
        ctx = build_dashboard_date_context("2026-03-10", "2026-03-24")
        first = _candidate("Topic One")

        with patch.object(topic_overviews.comparative, "get_topic_overview_candidates", return_value=[first]) as candidates_mock, \
             patch.object(topic_overviews, "_acquire_refresh_lease", return_value=True), \
             patch.object(topic_overviews, "_load_state", return_value={"schemaVersion": 1, "updatedAt": "", "topics": {}}), \
             patch.object(topic_overviews, "_load_snapshot_payload", return_value=topic_overviews._default_snapshot_payload()), \
             patch.object(topic_overviews, "_save_state", return_value=True), \
             patch.object(topic_overviews, "_save_snapshot_payload", return_value=True), \
             patch.object(topic_overviews, "_generate_item", return_value=_ready_item("Topic One", ctx)):
            topic_overviews.refresh_topic_overviews(ctx=ctx, force=True)

        self.assertEqual(candidates_mock.call_count, 1)
        self.assertGreaterEqual(
            candidates_mock.call_args.kwargs["limit"],
            topic_overviews._TOPIC_OVERVIEW_CANDIDATE_LIMIT_FLOOR,
        )


class TopicDetailOverviewEndpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._startup_handlers = list(server.app.router.on_startup)
        cls._shutdown_handlers = list(server.app.router.on_shutdown)
        server.app.router.on_startup = []
        server.app.router.on_shutdown = []
        cls.client = TestClient(server.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        server.app.router.on_startup = cls._startup_handlers
        server.app.router.on_shutdown = cls._shutdown_handlers

    def setUp(self) -> None:
        server._analytics_rate_limit_buckets.clear()

    def _ctx(self) -> SimpleNamespace:
        return SimpleNamespace(
            from_date=date(2026, 3, 15),
            to_date=date(2026, 3, 22),
            days=7,
            is_operational=False,
            range_label="Last 7 Days",
            cache_key="2026-03-15:2026-03-22",
        )

    def test_topic_detail_merges_materialized_overview(self) -> None:
        payload = {
            "name": "Topic One",
            "sourceTopic": "Topic One",
            "category": "Government & Leadership",
            "mentions": 12,
        }
        overview = {
            "topic": "Topic One",
            "category": "Government & Leadership",
            "status": "ready",
            "summaryEn": "Overview text",
            "summaryRu": "Текст обзора",
            "signalsEn": ["One", "Two", "Three"],
            "signalsRu": ["Один", "Два", "Три"],
            "generatedAt": "2026-03-27T12:00:00Z",
            "windowStart": "2026-03-10",
            "windowEnd": "2026-03-24",
            "windowDays": 14,
        }

        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_default_dashboard_context", return_value=self._ctx()), \
             patch.object(server, "get_topic_detail", return_value=payload), \
             patch.object(server.topic_overviews, "get_topic_overview", return_value=overview) as get_overview_mock:
            response = self.client.get("/api/topics/detail", params={"topic": "Topic One"})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("overview", body)
        self.assertEqual(body["overview"]["summaryEn"], "Overview text")
        get_overview_mock.assert_called_once_with("Topic One", "Government & Leadership", ctx=ANY)

    def test_topic_detail_handles_missing_materialized_overview(self) -> None:
        payload = {
            "name": "Topic One",
            "sourceTopic": "Topic One",
            "category": "Government & Leadership",
            "mentions": 12,
            "mentionCount": 12,
            "previousMentions": 6,
            "growth7dPct": 25,
            "distinctUsers": 5,
            "distinctChannels": 3,
            "sentimentPositive": 10,
            "sentimentNeutral": 20,
            "sentimentNegative": 70,
            "evidence": [
                {
                    "id": "ev-1",
                    "type": "message",
                    "author": "author-1",
                    "channel": "Channel A",
                    "text": "Evidence one",
                    "timestamp": "2026-03-24T11:00:00Z",
                    "reactions": 4,
                    "replies": 1,
                }
            ],
            "questionEvidence": [],
        }

        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_default_dashboard_context", return_value=self._ctx()), \
             patch.object(server, "get_topic_detail", return_value=payload), \
             patch.object(server.topic_overviews, "get_topic_overview", return_value=None) as get_overview_mock:
            response = self.client.get("/api/topics/detail", params={"topic": "Topic One"})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("overview", body)
        self.assertEqual(body["overview"]["status"], "fallback")
        self.assertEqual(body["overview"]["topic"], "Topic One")
        get_overview_mock.assert_called_once_with("Topic One", "Government & Leadership", ctx=ANY)

    def test_topic_detail_tolerates_materialized_overview_errors(self) -> None:
        payload = {
            "name": "Topic One",
            "sourceTopic": "Topic One",
            "category": "Government & Leadership",
            "mentions": 12,
            "mentionCount": 12,
            "previousMentions": 6,
            "growth7dPct": 25,
            "distinctUsers": 5,
            "distinctChannels": 3,
            "sentimentPositive": 10,
            "sentimentNeutral": 20,
            "sentimentNegative": 70,
            "evidence": [
                {
                    "id": "ev-1",
                    "type": "message",
                    "author": "author-1",
                    "channel": "Channel A",
                    "text": "Evidence one",
                    "timestamp": "2026-03-24T11:00:00Z",
                    "reactions": 4,
                    "replies": 1,
                }
            ],
            "questionEvidence": [],
        }

        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_default_dashboard_context", return_value=self._ctx()), \
             patch.object(server, "get_topic_detail", return_value=payload), \
             patch.object(server.topic_overviews, "get_topic_overview", side_effect=RuntimeError("boom")):
            response = self.client.get("/api/topics/detail", params={"topic": "Topic One"})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["overview"]["status"], "fallback")
        self.assertEqual(body["overview"]["topic"], "Topic One")

    def test_topic_detail_builds_fallback_overview_when_materialized_item_missing(self) -> None:
        payload = {
            "name": "Visa And Residency",
            "sourceTopic": "Visa And Residency",
            "category": "Emigration",
            "mentions": 185,
            "previousMentions": 92,
            "growth7dPct": 101,
            "distinctUsers": 48,
            "distinctChannels": 3,
            "evidenceCount": 185,
            "sentimentPositive": 8,
            "sentimentNeutral": 78,
            "sentimentNegative": 14,
            "topChannels": ["Channel A", "Channel B"],
            "latestAt": "2026-04-15T16:50:43Z",
            "evidence": [
                {"id": "ev-1", "text": "Evidence one", "channel": "Channel A", "timestamp": "2026-04-15T16:50:43Z"},
                {"id": "ev-2", "text": "Evidence two", "channel": "Channel B", "timestamp": "2026-04-15T16:40:43Z"},
            ],
            "questionEvidence": [
                {"id": "q-1", "text": "What visa should I choose?", "channel": "Channel A", "timestamp": "2026-04-15T16:30:43Z"},
            ],
        }

        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_default_dashboard_context", return_value=self._ctx()), \
             patch.object(server, "get_topic_detail", return_value=payload), \
             patch.object(server.topic_overviews, "get_topic_overview", return_value=None):
            response = self.client.get("/api/topics/detail", params={"topic": "Visa And Residency"})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("overview", body)
        self.assertEqual(body["overview"]["status"], "fallback")
        self.assertEqual(body["overview"]["topic"], "Visa And Residency")
        self.assertTrue(body["overview"]["summaryEn"])


if __name__ == "__main__":
    unittest.main()
