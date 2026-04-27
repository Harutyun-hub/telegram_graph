from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("APP_ROLE", "web")
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "service-role")
os.environ.setdefault("NEO4J_URI", "bolt://localhost:7687")
os.environ.setdefault("NEO4J_PASSWORD", "password")
os.environ.setdefault("OPENAI_API_KEY", "test-openai-key")

from api import agent_analysis, server


class AgentDeepAnalysisTests(unittest.TestCase):
    def test_deep_analyze_flags_concentration_and_rejected_explanations(self) -> None:
        run_query_results = [
            [{"mentionCount": 12}],
            [{"mentionCount": 4}],
            [{"label": "negative", "count": 7}, {"label": "neutral", "count": 3}],
            [{"label": "negative", "count": 2}, {"label": "neutral", "count": 6}],
            [
                {"channel": "Docs Chat", "mentions": 9},
                {"channel": "Visa Support", "mentions": 3},
            ],
            [{"totalComments": 10, "questionCount": 5, "answerProxyCount": 1, "repeatQuestionUsers": 2}],
            [
                {
                    "id": "comment-1",
                    "type": "comment",
                    "channel": "Docs Chat",
                    "text": "Why are appointments delayed again?",
                    "timestamp": "2026-04-20T10:00:00Z",
                    "reactions": 0,
                    "replies": 0,
                }
            ],
        ]

        with patch.object(
            agent_analysis.graph_dashboard,
            "search_graph",
            return_value=[{"type": "topic", "id": "topic:Visa And Residency", "name": "Visa And Residency"}],
        ), patch.object(agent_analysis, "run_query", side_effect=run_query_results):
            payload = agent_analysis.deep_analyze(
                "What is driving concern about visa appointments?",
                window="7d",
                mode="deep",
            )

        self.assertEqual(payload["surprise"]["label"], "high")
        self.assertEqual(payload["analysis_trace"]["probe_count"], 6)
        self.assertIn("counterfactual_channel_lift", payload["analysis_trace"]["recipes"])
        self.assertIn(payload["confidence"], {"medium", "high"})
        self.assertTrue(payload["considered_and_rejected"])
        self.assertEqual(payload["evidence"][0]["channel"], "Docs Chat")
        self.assertIn("concentration risk", " ".join(payload["key_findings"]).lower())

    def test_deep_analyze_returns_low_confidence_when_unresolved(self) -> None:
        with patch.object(agent_analysis.graph_dashboard, "search_graph", return_value=[]), \
             patch.object(agent_analysis, "_top_surprising_topic", return_value={}):
            payload = agent_analysis.deep_analyze("What should I know?", window="7d")

        self.assertEqual(payload["confidence"], "low_confidence")
        self.assertEqual(payload["analysis_trace"]["probe_count"], 0)
        self.assertIn("did not resolve", payload["summary"])


class AgentDeepAnalysisEndpointTests(unittest.TestCase):
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

    def test_agent_deep_analysis_accepts_openclaw_token(self) -> None:
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False), \
             patch.object(
                 server.agent_analysis,
                 "deep_analyze",
                 return_value={"summary": "ok", "confidence": "medium", "telegram_text": "ok"},
             ):
            response = self.client.post(
                "/api/agent/analysis/deep",
                headers={"Authorization": "Bearer openclaw-secret"},
                json={"question": "Deep analyze residency permits", "window": "7d", "mode": "quick"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["summary"], "ok")

    def test_agent_deep_analysis_rejects_frontend_token(self) -> None:
        with patch.object(server.config, "IS_LOCKED_ENV", True), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server.config, "ANALYTICS_RATE_LIMIT_ENABLED", False):
            response = self.client.post(
                "/api/agent/analysis/deep",
                headers={"Authorization": "Bearer frontend-secret"},
                json={"question": "Deep analyze residency permits", "window": "7d", "mode": "quick"},
            )

        self.assertEqual(response.status_code, 403)


if __name__ == "__main__":
    unittest.main()
