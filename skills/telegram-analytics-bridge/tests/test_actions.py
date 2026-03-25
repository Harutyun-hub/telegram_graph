from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from actions import (
    ask_insights,
    get_active_alerts,
    get_declining_topics,
    get_problem_spikes,
    get_question_clusters,
    get_sentiment_overview,
    get_top_topics,
)
from models import (
    AskInsightsRequest,
    GetActiveAlertsRequest,
    GetDecliningTopicsRequest,
    GetProblemSpikesRequest,
    GetQuestionClustersRequest,
    GetSentimentOverviewRequest,
    GetTopTopicsRequest,
)


def load_fixture(name: str):
    path = Path(__file__).resolve().parent / "fixtures" / name
    return json.loads(path.read_text())


class FakeClient:
    def __init__(self, dashboard=None, sentiments=None, insight_cards=None):
        self._dashboard = dashboard if dashboard is not None else load_fixture("dashboard.json")
        self._sentiments = sentiments if sentiments is not None else load_fixture("sentiments.json")
        self._insight_cards = insight_cards if insight_cards is not None else load_fixture("insight_cards.json")

    def get_dashboard(self, window=None):
        return self._dashboard

    def get_sentiment_distribution(self, window):
        return self._sentiments

    def get_insight_cards(self, window):
        return self._insight_cards


class ActionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeClient()

    def test_top_topics_response_has_metadata(self) -> None:
        payload = get_top_topics(self.client, GetTopTopicsRequest(window="7d", limit=2))
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["schema_version"], "1.0")
        self.assertIn("generated_at", payload)
        self.assertEqual(len(payload["items"]), 2)

    def test_declining_topics_uses_negative_growth(self) -> None:
        payload = get_declining_topics(self.client, GetDecliningTopicsRequest(window="30d", limit=2))
        self.assertEqual(payload["items"][0]["topic"], "Rental costs")
        self.assertLess(payload["items"][0]["growth_7d_pct"], 0)

    def test_problem_spikes_prefers_problem_briefs(self) -> None:
        payload = get_problem_spikes(self.client, GetProblemSpikesRequest(window="7d"))
        self.assertEqual(payload["items"][0]["topic"], "Residency permits")
        self.assertEqual(payload["items"][0]["severity"], "high")

    def test_question_clusters_filters_by_topic(self) -> None:
        payload = get_question_clusters(
            self.client,
            GetQuestionClustersRequest(window="7d", topic="Residency"),
        )
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["topic"], "Residency permits")

    def test_sentiment_overview_combines_sources(self) -> None:
        payload = get_sentiment_overview(self.client, GetSentimentOverviewRequest(window="7d"))
        self.assertEqual(payload["source_endpoints"], ["/api/sentiment-distribution", "/api/dashboard"])
        self.assertIn("Community health score", " ".join(payload["bullets"]))

    def test_active_alerts_returns_current_alerts(self) -> None:
        payload = get_active_alerts(self.client, GetActiveAlertsRequest())
        self.assertEqual(payload["items"][0]["topic"], "Residency permits")

    def test_ask_insights_returns_supported_answer(self) -> None:
        payload = ask_insights(
            self.client,
            AskInsightsRequest(window="7d", question="What is driving concern about residency permits?"),
        )
        self.assertIn(payload["confidence"], {"medium", "high"})
        self.assertNotEqual(payload["confidence"], "low_confidence")
        self.assertIn("Residency permits", payload["summary"])

    def test_ask_insights_returns_low_confidence_when_evidence_is_insufficient(self) -> None:
        payload = ask_insights(
            self.client,
            AskInsightsRequest(window="7d", question="What is happening with food delivery discounts?"),
        )
        self.assertEqual(payload["confidence"], "low_confidence")
        self.assertIn("caveat", payload)

    def test_ask_insights_returns_low_confidence_for_conflicting_support(self) -> None:
        dashboard = load_fixture("dashboard.json")
        dashboard["data"]["questionBriefs"].append(
            {
                "id": "qc-rent",
                "topic": "Rental costs",
                "category": "Housing",
                "canonicalQuestionEn": "Why are rents jumping so fast this month?",
                "summaryEn": "This cluster reflects price shock and listing shortages.",
                "demandSignals": {
                    "messages": 17,
                    "uniqueUsers": 9,
                    "channels": 3,
                    "trend7dPct": 24
                },
                "evidence": [
                    {
                        "id": "q-9",
                        "quote": "Why did my landlord raise the rent again?",
                        "channel": "Rent Armenia",
                        "timestamp": "2026-03-18T11:00:00Z",
                        "kind": "message"
                    }
                ]
            }
        )
        payload = ask_insights(
            FakeClient(dashboard=dashboard),
            AskInsightsRequest(window="7d", question="What is the main issue right now?"),
        )
        self.assertEqual(payload["confidence"], "low_confidence")


if __name__ == "__main__":
    unittest.main()
