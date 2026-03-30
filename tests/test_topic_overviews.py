from __future__ import annotations

from datetime import date
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api import server, topic_overviews


def _detail_payload() -> dict:
    return {
        "name": "Armenian Government Performance",
        "sourceTopic": "Armenian Government Performance",
        "category": "Government & Leadership",
        "mentionCount": 12,
        "prev7Mentions": 6,
        "growth7dPct": 50,
        "sentimentPositive": 5,
        "sentimentNeutral": 12,
        "sentimentNegative": 83,
        "distinctUsers": 5,
        "distinctChannels": 3,
        "topChannels": ["Armenian Life", "Channel B"],
        "sampleEvidence": {"timestamp": "2026-03-29T12:00:00Z"},
        "evidence": [
            {"id": "ev-1", "text": "Residents keep returning to missed delivery promises.", "channel": "Armenian Life", "timestamp": "2026-03-29T12:00:00Z"},
            {"id": "ev-2", "text": "Complaints focus on visible delays and official explanations.", "channel": "Channel B", "timestamp": "2026-03-29T11:00:00Z"},
            {"id": "ev-3", "text": "People argue whether this is incompetence or poor communication.", "channel": "Channel C", "timestamp": "2026-03-29T10:00:00Z"},
            {"id": "ev-4", "text": "Criticism is increasingly tied to trust in the cabinet, not one decision.", "channel": "Channel C", "timestamp": "2026-03-29T09:00:00Z"},
            {"id": "ev-5", "text": "The discussion is turning into a broader accountability debate.", "channel": "Channel A", "timestamp": "2026-03-29T08:00:00Z"},
        ],
        "questionEvidence": [
            {"id": "q-1", "text": "Why are the same service failures repeating?", "channel": "Armenian Life", "timestamp": "2026-03-29T07:00:00Z"},
            {"id": "q-2", "text": "Is anyone taking responsibility for these delays?", "channel": "Channel B", "timestamp": "2026-03-29T06:00:00Z"},
            {"id": "q-3", "text": "What will actually change this month?", "channel": "Channel B", "timestamp": "2026-03-29T05:00:00Z"},
        ],
        "summaryEn": "Fallback summary",
        "summaryRu": "Резервная сводка",
        "signalsEn": ["Fallback one", "Fallback two", "Fallback three"],
        "signalsRu": ["Резерв один", "Резерв два", "Резерв три"],
    }


class TopicOverviewTests(unittest.TestCase):
    def setUp(self) -> None:
        topic_overviews.invalidate_topic_overviews_cache()

    def _ctx(self) -> SimpleNamespace:
        return SimpleNamespace(
            from_date=date(2026, 3, 16),
            to_date=date(2026, 3, 30),
            days=14,
        )

    def test_admin_defaults_include_topic_overview_prompt(self) -> None:
        defaults = topic_overviews.get_admin_prompt_defaults()
        self.assertIn("topic_overviews.synthesis_prompt", defaults)
        self.assertIn("insight-rich overview", defaults["topic_overviews.synthesis_prompt"])

    def test_get_topic_overview_generates_and_caches_ready_item(self) -> None:
        detail = _detail_payload()
        parsed = {
            "overview": {
                "summaryEn": "The discussion has shifted from isolated complaints toward a broader trust and accountability narrative.",
                "summaryRu": "Обсуждение сместилось от отдельных жалоб к более широкой теме доверия и ответственности властей.",
                "signalsEn": ["Complaints are clustering around repeated delivery failures, not a one-off incident.", "People are linking service frustration to confidence in government competence.", "Open questions show the audience wants ownership and visible corrective action."],
                "signalsRu": ["Жалобы группируются вокруг повторяющихся сбоев, а не одного эпизода.", "Недовольство сервисом всё чаще связывают с общей оценкой компетентности власти.", "Открытые вопросы показывают запрос на ответственность и понятные исправления."],
            }
        }

        with patch.object(topic_overviews, "_load_persisted_item", return_value=None), \
             patch.object(topic_overviews, "_save_persisted_item"), \
             patch.object(topic_overviews, "_chat_json", return_value=parsed) as chat_mock, \
             patch.object(topic_overviews, "submit_background", side_effect=lambda fn: fn()):
            first = topic_overviews.get_topic_overview(detail["name"], detail["category"], detail_payload=detail, ctx=self._ctx())
            second = topic_overviews.get_topic_overview(detail["name"], detail["category"], detail_payload=detail, ctx=self._ctx())

        self.assertEqual(first["status"], "fallback")
        self.assertEqual(first["summaryEn"], "Fallback summary")
        self.assertEqual(second["summaryEn"], parsed["overview"]["summaryEn"])
        self.assertEqual(chat_mock.call_count, 1)

    def test_get_topic_overview_schedules_single_background_generation(self) -> None:
        detail = _detail_payload()
        scheduled: list[object] = []

        with patch.object(topic_overviews, "_load_persisted_item", return_value=None), \
             patch.object(topic_overviews, "_save_persisted_item"), \
             patch.object(topic_overviews, "submit_background", side_effect=lambda fn: scheduled.append(fn)):
            first = topic_overviews.get_topic_overview(detail["name"], detail["category"], detail_payload=detail, ctx=self._ctx())
            second = topic_overviews.get_topic_overview(detail["name"], detail["category"], detail_payload=detail, ctx=self._ctx())

        self.assertEqual(first["status"], "fallback")
        self.assertEqual(second["status"], "fallback")
        self.assertEqual(len(scheduled), 1)


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

    def test_topic_detail_prefers_topic_overview_module(self) -> None:
        payload = _detail_payload()
        overview = {
            "topic": payload["name"],
            "category": payload["category"],
            "status": "ready",
            "summaryEn": "Overview text",
            "summaryRu": "Текст обзора",
            "signalsEn": ["One", "Two", "Three"],
            "signalsRu": ["Один", "Два", "Три"],
            "generatedAt": "2026-03-30T09:00:00Z",
            "windowStart": "2026-03-16",
            "windowEnd": "2026-03-30",
            "windowDays": 14,
            "evidenceIds": ["ev-1"],
        }
        ctx = SimpleNamespace(
            from_date=date(2026, 3, 16),
            to_date=date(2026, 3, 30),
            days=14,
            is_operational=False,
            range_label="Last 14 Days",
            cache_key="2026-03-16:2026-03-30",
        )

        with patch.object(server.config, "ANALYTICS_API_REQUIRE_AUTH", False), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(server, "_default_dashboard_context", return_value=ctx), \
             patch.object(server, "get_topic_detail", return_value=payload), \
             patch.object(server.topic_overviews, "get_topic_overview", return_value=overview):
            response = self.client.get("/api/topics/detail", params={"topic": payload["name"], "category": payload["category"]})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["overview"]["summaryEn"], "Overview text")


if __name__ == "__main__":
    unittest.main()
