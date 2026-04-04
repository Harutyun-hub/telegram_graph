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
    get_freshness_status,
    get_active_alerts,
    get_declining_topics,
    get_problem_spikes,
    get_question_clusters,
    get_sentiment_overview,
    get_topic_detail,
    get_topic_evidence,
    get_top_topics,
    investigate_question,
    investigate_topic,
    search_entities,
)
from client import AnalyticsAPIError
from models import (
    AskInsightsRequest,
    GetFreshnessStatusRequest,
    GetActiveAlertsRequest,
    GetDecliningTopicsRequest,
    GetProblemSpikesRequest,
    GetQuestionClustersRequest,
    GetSentimentOverviewRequest,
    GetTopicDetailRequest,
    GetTopicEvidenceRequest,
    GetTopTopicsRequest,
    InvestigateQuestionRequest,
    InvestigateTopicRequest,
    SearchEntitiesRequest,
)


def load_fixture(name: str):
    path = Path(__file__).resolve().parent / "fixtures" / name
    return json.loads(path.read_text())


SEARCH_RESULTS = [
    {
        "type": "topic",
        "id": "topic:Residency permits",
        "name": "Residency permits",
        "text": "Topic",
    },
    {
        "type": "channel",
        "id": "channel:docs-chat",
        "name": "Docs Chat",
        "text": "@docschat",
    },
]

TOPIC_DETAIL = {
    "name": "Residency permits",
    "category": "Documents",
    "mentionCount": 42,
    "growth7dPct": 31.5,
    "userCount": 14,
    "distinctChannels": 4,
    "topChannels": ["Docs Chat", "Visa Support", "Expats Armenia"],
    "sentimentPositive": 18,
    "sentimentNeutral": 34,
    "sentimentNegative": 48,
    "sampleEvidence": {
        "id": "ev-1",
        "type": "message",
        "author": "docs_chat",
        "channel": "Docs Chat",
        "text": "Permit processing is taking much longer than expected this month.",
        "timestamp": "2026-03-18T12:00:00Z",
    },
}

TOPIC_EVIDENCE = {
    "items": [
        {
            "id": "ev-1",
            "type": "message",
            "author": "docs_chat",
            "channel": "Docs Chat",
            "text": "Permit processing is taking much longer than expected this month.",
            "timestamp": "2026-03-18T12:00:00Z",
            "reactions": 14,
            "replies": 3,
        },
        {
            "id": "ev-2",
            "type": "reply",
            "author": "anonymous",
            "channel": "Docs Chat",
            "text": "I was told to come back with different paperwork after two weeks.",
            "timestamp": "2026-03-18T14:00:00Z",
            "reactions": 0,
            "replies": 0,
        },
    ],
    "total": 2,
    "page": 0,
    "size": 5,
    "hasMore": False,
    "focusedItem": None,
}

FRESHNESS = {
    "health": {
        "status": "warning",
        "score": 71,
    },
    "backlog": {
        "unprocessed_posts": 3,
        "unprocessed_comments": 5,
        "unsynced_posts": 2,
    },
    "drift": {
        "latest_post_delta_minutes": 19,
    },
    "pipeline": {
        "scrape": {"age_minutes": 9},
        "process": {"age_minutes": 18},
        "sync": {"age_minutes": 21},
    },
}


class FakeClient:
    def __init__(
        self,
        dashboard=None,
        sentiments=None,
        insight_cards=None,
        search_results=None,
        search_results_by_query=None,
        topic_detail=None,
        topic_details_by_topic=None,
        topic_evidence=None,
        freshness=None,
    ):
        self._dashboard = dashboard if dashboard is not None else load_fixture("dashboard.json")
        self._sentiments = sentiments if sentiments is not None else load_fixture("sentiments.json")
        self._insight_cards = insight_cards if insight_cards is not None else load_fixture("insight_cards.json")
        self._search_results = search_results if search_results is not None else SEARCH_RESULTS
        self._search_results_by_query = search_results_by_query or {}
        self._topic_detail = topic_detail if topic_detail is not None else TOPIC_DETAIL
        self._topic_details_by_topic = topic_details_by_topic or {}
        self._topic_evidence = topic_evidence if topic_evidence is not None else TOPIC_EVIDENCE
        self._freshness = freshness if freshness is not None else FRESHNESS
        self.search_queries = []

    def get_dashboard(self, window=None):
        return self._dashboard

    def get_sentiment_distribution(self, window):
        return self._sentiments

    def get_insight_cards(self, window):
        return self._insight_cards

    def search_entities(self, query, limit=5):
        self.search_queries.append(query)
        if query in self._search_results_by_query:
            return self._search_results_by_query[query][:limit]
        return self._search_results[:limit]

    def get_topic_detail(self, topic, category=None, window="7d"):
        if self._topic_details_by_topic:
            if topic in self._topic_details_by_topic:
                detail = self._topic_details_by_topic[topic]
                if detail is None:
                    raise AnalyticsAPIError(
                        "Topic not found for the selected window.",
                        error_type="not_found",
                        status_code=404,
                    )
                return detail
            raise AnalyticsAPIError(
                "Topic not found for the selected window.",
                error_type="not_found",
                status_code=404,
            )
        return self._topic_detail

    def get_topic_evidence(self, topic, category=None, view="all", page=0, size=5, focus_id=None, window="7d"):
        return self._topic_evidence

    def get_freshness_status(self, force=False):
        return self._freshness


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

    def test_search_entities_returns_compact_results(self) -> None:
        payload = search_entities(self.client, SearchEntitiesRequest(query="permit delays", limit=2))
        self.assertEqual(payload["action"], "search_entities")
        self.assertEqual(payload["items"][0]["type"], "topic")
        self.assertIn("Found 2 matching entities", payload["summary"])

    def test_get_topic_detail_normalizes_detail_payload(self) -> None:
        payload = get_topic_detail(
            self.client,
            GetTopicDetailRequest(window="7d", topic="Residency permits", category="Documents"),
        )
        self.assertEqual(payload["items"][0]["topic"], "Residency permits")
        self.assertEqual(payload["items"][0]["distinct_channels"], 4)
        self.assertIn("growth", payload["summary"])

    def test_get_topic_evidence_returns_trimmed_rows(self) -> None:
        payload = get_topic_evidence(
            self.client,
            GetTopicEvidenceRequest(window="7d", topic="Residency permits", view="questions", limit=2),
        )
        self.assertEqual(payload["items"][0]["channel"], "Docs Chat")
        self.assertIn("question evidence", payload["summary"])

    def test_get_freshness_status_returns_backlog_summary(self) -> None:
        payload = get_freshness_status(self.client, GetFreshnessStatusRequest(force=False))
        self.assertEqual(payload["items"][0]["health_status"], "warning")
        self.assertIn("Data freshness is warning", payload["summary"])

    def test_investigate_topic_combines_detail_evidence_and_freshness(self) -> None:
        payload = investigate_topic(
            self.client,
            InvestigateTopicRequest(window="7d", topic="Residency permits", category="Documents"),
        )
        self.assertEqual(payload["action"], "investigate_topic")
        self.assertEqual(payload["items"][0]["topic"], "Residency permits")
        self.assertIn("/api/freshness", payload["source_endpoints"])
        self.assertIn("warning", payload.get("caveat", ""))

    def test_investigate_question_uses_ask_insights_topic_path(self) -> None:
        payload = investigate_question(
            self.client,
            InvestigateQuestionRequest(window="7d", question="What is driving concern about residency permits?"),
        )
        self.assertEqual(payload["action"], "investigate_question")
        self.assertEqual(payload["items"][0]["topic"], "Residency permits")
        self.assertIn("/api/topics/detail", payload["source_endpoints"])

    def test_investigate_question_falls_back_to_search_when_needed(self) -> None:
        payload = investigate_question(
            FakeClient(search_results=SEARCH_RESULTS, dashboard=load_fixture("dashboard.json"), insight_cards={"cards": []}),
            InvestigateQuestionRequest(window="7d", question="What is happening with permit administration?"),
        )
        self.assertEqual(payload["action"], "investigate_question")
        self.assertIn("closest evidence-backed match", payload["summary"])

    def test_investigate_question_returns_low_confidence_when_no_topic_candidate_exists(self) -> None:
        payload = investigate_question(
            FakeClient(
                search_results=[{"type": "channel", "id": "channel:docs", "name": "Docs Chat", "text": "@docs"}],
                dashboard=load_fixture("dashboard.json"),
                insight_cards={"cards": []},
            ),
            InvestigateQuestionRequest(window="7d", question="What is happening with permit administration?"),
        )
        self.assertEqual(payload["confidence"], "low_confidence")
        self.assertLessEqual(len(payload["items"]), 3)

    def test_investigate_question_recovers_from_topic_not_found_using_short_search_term(self) -> None:
        dashboard = {"data": {"questionBriefs": [], "problemBriefs": [], "urgencySignals": [], "trendingTopics": []}}
        insight_cards = {
            "cards": [
                {
                    "title": "Politics",
                    "summary": "politics concern",
                    "why_it_matters": "politics concern is affecting resident planning",
                    "evidence": [{"id": "1"}, {"id": "2"}],
                }
            ]
        }
        political_detail = {
            **TOPIC_DETAIL,
            "name": "Political protests",
            "category": "Politics",
            "topChannels": ["City Watch", "Civic Armenia"],
        }
        client = FakeClient(
            dashboard=dashboard,
            insight_cards=insight_cards,
            search_results=[],
            search_results_by_query={
                "Politics": [
                    {
                        "type": "topic",
                        "id": "topic:Political protests",
                        "name": "Political protests",
                        "text": "Topic",
                    }
                ]
            },
            topic_details_by_topic={
                "Politics": None,
                "Political protests": political_detail,
            },
        )

        payload = investigate_question(
            client,
            InvestigateQuestionRequest(window="7d", question="politics concern now"),
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["items"][0]["topic"], "Political protests")
        self.assertEqual(client.search_queries, ["Politics"])

    def test_investigate_question_returns_candidate_backed_answer_when_resolution_fails(self) -> None:
        dashboard = {"data": {"questionBriefs": [], "problemBriefs": [], "urgencySignals": [], "trendingTopics": []}}
        insight_cards = {
            "cards": [
                {
                    "title": "Politics",
                    "summary": "politics concern",
                    "why_it_matters": "politics concern is affecting resident planning",
                    "evidence": [{"id": "1"}, {"id": "2"}],
                }
            ]
        }
        client = FakeClient(
            dashboard=dashboard,
            insight_cards=insight_cards,
            search_results=[],
            search_results_by_query={"Politics": []},
            topic_details_by_topic={"Politics": None},
        )

        payload = investigate_question(
            client,
            InvestigateQuestionRequest(window="7d", question="politics concern now"),
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["confidence"], "low_confidence")
        self.assertGreaterEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["topic"], "Politics")


if __name__ == "__main__":
    unittest.main()
