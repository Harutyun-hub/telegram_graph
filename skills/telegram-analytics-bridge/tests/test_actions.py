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
    compare_channels,
    compare_topics,
    get_freshness_status,
    get_active_alerts,
    get_declining_topics,
    get_graph_snapshot,
    get_node_context,
    get_problem_spikes,
    get_question_clusters,
    get_sentiment_overview,
    get_topic_detail,
    get_topic_evidence,
    get_top_topics,
    investigate_channel,
    investigate_question,
    investigate_topic,
    search_entities,
)
from client import AnalyticsAPIError
from models import (
    AskInsightsRequest,
    CompareChannelsRequest,
    CompareTopicsRequest,
    GetFreshnessStatusRequest,
    GetActiveAlertsRequest,
    GetDecliningTopicsRequest,
    GetGraphSnapshotRequest,
    GetNodeContextRequest,
    GetProblemSpikesRequest,
    GetQuestionClustersRequest,
    GetSentimentOverviewRequest,
    GetTopicDetailRequest,
    GetTopicEvidenceRequest,
    GetTopTopicsRequest,
    InvestigateChannelRequest,
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

CHANNEL_DETAIL = {
    "username": "docs_chat",
    "title": "Docs Chat",
    "memberCount": 15400,
    "description": "Migration and permits support channel",
    "postCount": 21,
    "avgViews": 840,
    "lastPost": "2026-03-18T16:00:00Z",
    "dailyMessages": 3,
    "growth7dPct": 18.4,
    "topTopics": [
        {"name": "Residency permits", "mentions": 11, "pct": 46},
        {"name": "School admissions", "mentions": 5, "pct": 21},
    ],
    "messageTypes": [{"type": "text", "count": 18}],
    "topVoices": [{"name": "12345", "posts": 7, "helpScore": 35}],
    "recentPosts": [
        {
            "id": "post-1",
            "author": "docs_chat",
            "text": "Applicants are still reporting longer waits for permit appointments this week.",
            "timestamp": "2026-03-18T16:00:00Z",
            "reactions": 18,
            "replies": 4,
        }
    ],
    "sentimentPositive": 19,
    "sentimentNeutral": 42,
    "sentimentNegative": 39,
}

CHANNEL_POSTS = {
    "items": [
        {
            "id": "post-1",
            "author": "docs_chat",
            "text": "Applicants are still reporting longer waits for permit appointments this week.",
            "timestamp": "2026-03-18T16:00:00Z",
            "reactions": 18,
            "replies": 4,
        },
        {
            "id": "post-2",
            "author": "docs_chat",
            "text": "Several users were asked for additional paperwork after their first appointment.",
            "timestamp": "2026-03-18T12:00:00Z",
            "reactions": 9,
            "replies": 2,
        },
    ],
    "total": 2,
    "page": 0,
    "size": 3,
    "hasMore": False,
}

GRAPH_DATA = {
    "nodes": [
        {
            "id": "category:Documents",
            "name": "Documents",
            "type": "category",
            "mentionCount": 58,
            "topicCount": 3,
        },
        {
            "id": "topic:Residency permits",
            "name": "Residency permits",
            "type": "topic",
            "mentionCount": 42,
        },
        {
            "id": "topic:School admissions",
            "name": "School admissions",
            "type": "topic",
            "mentionCount": 16,
        },
        {
            "id": "channel:docs-chat",
            "name": "Docs Chat",
            "type": "channel",
            "mentionCount": 14,
        },
    ],
    "links": [],
    "meta": {
        "visibleTopicCount": 2,
        "visibleCategoryCount": 1,
        "visibleChannelCount": 1,
        "totalMentions": 58,
    },
}

GRAPH_INSIGHTS = {
    "insight": "Conversation map for Last 7 Days: 2 topics across 1 category. Top categories: Documents. Leading topics: Residency permits, School admissions.",
    "timestamp": "2026-03-18T18:00:00Z",
}

TOP_CHANNELS = [
    {"id": "channel:docs-chat", "name": "Docs Chat", "adCount": 14},
    {"id": "channel:visa-support", "name": "Visa Support", "adCount": 10},
]

TRENDING_TOPICS = [
    {"id": "topic:Residency permits", "name": "Residency permits", "adCount": 42},
    {"id": "topic:School admissions", "name": "School admissions", "adCount": 16},
]

TOPIC_NODE_DETAIL = {
    "id": "topic:Residency permits",
    "name": "Residency permits",
    "type": "topic",
    "category": "Documents",
    "mentionCount": 42,
    "evidenceCount": 12,
    "distinctChannels": 4,
    "trendPct": 31.5,
    "dominantSentiment": "Negative",
    "topChannels": [{"name": "Docs Chat", "mentions": 11}],
    "relatedTopics": [{"name": "Visa appointments", "category": "Documents", "mentions": 8}],
    "overview": {"summaryEn": "Permit delays are being driven by long waits and repeat paperwork requests."},
    "evidence": TOPIC_EVIDENCE["items"],
}

CATEGORY_NODE_DETAIL = {
    "id": "category:Documents",
    "name": "Documents",
    "type": "category",
    "topicCount": 3,
    "mentionCount": 58,
    "trendPct": 21.4,
    "dominantSentiment": "Negative",
    "topTopics": [{"name": "Residency permits", "mentions": 42}],
    "topChannels": [{"name": "Docs Chat", "mentions": 14}],
    "overview": {"summaryEn": "Documents pressure is concentrated around permits and appointments."},
    "evidence": TOPIC_EVIDENCE["items"],
}

CHANNEL_NODE_DETAIL = {
    "id": "channel:docs-chat",
    "name": "Docs Chat",
    "type": "channel",
    "username": "docs_chat",
    "postCount": 21,
    "topics": [{"name": "Residency permits", "category": "Documents"}],
    "categories": [{"name": "Documents", "topicCount": 2}],
}


class FakeClient:
    def __init__(
        self,
        dashboard=None,
        sentiments=None,
        sentiment_error=None,
        insight_cards=None,
        search_results=None,
        search_results_by_query=None,
        topic_detail=None,
        topic_details_by_topic=None,
        topic_evidence=None,
        freshness=None,
        channel_detail=None,
        channel_posts=None,
        graph_data=None,
        graph_insights=None,
        top_channels=None,
        trending_topics=None,
        node_details_by_key=None,
    ):
        self._dashboard = dashboard if dashboard is not None else load_fixture("dashboard.json")
        self._sentiments = sentiments if sentiments is not None else load_fixture("sentiments.json")
        self._sentiment_error = sentiment_error
        self._insight_cards = insight_cards if insight_cards is not None else load_fixture("insight_cards.json")
        self._search_results = search_results if search_results is not None else SEARCH_RESULTS
        self._search_results_by_query = search_results_by_query or {}
        self._topic_detail = topic_detail if topic_detail is not None else TOPIC_DETAIL
        self._topic_details_by_topic = topic_details_by_topic or {}
        self._topic_evidence = topic_evidence if topic_evidence is not None else TOPIC_EVIDENCE
        self._freshness = freshness if freshness is not None else FRESHNESS
        self._channel_detail = channel_detail if channel_detail is not None else CHANNEL_DETAIL
        self._channel_posts = channel_posts if channel_posts is not None else CHANNEL_POSTS
        self._graph_data = graph_data if graph_data is not None else GRAPH_DATA
        self._graph_insights = graph_insights if graph_insights is not None else GRAPH_INSIGHTS
        self._top_channels = top_channels if top_channels is not None else TOP_CHANNELS
        self._trending_topics = trending_topics if trending_topics is not None else TRENDING_TOPICS
        self._node_details_by_key = node_details_by_key or {
            ("topic:Residency permits", "topic"): TOPIC_NODE_DETAIL,
            ("category:Documents", "category"): CATEGORY_NODE_DETAIL,
            ("channel:docs-chat", "channel"): CHANNEL_NODE_DETAIL,
        }
        self.search_queries = []

    def get_dashboard(self, window=None):
        return self._dashboard

    def get_sentiment_distribution(self, window):
        if self._sentiment_error is not None:
            raise self._sentiment_error
        return self._sentiments

    def get_insight_cards(self, window):
        return self._insight_cards

    def search_entities(self, query, limit=5):
        self.search_queries.append(query)
        if query in self._search_results_by_query:
            return self._search_results_by_query[query][:limit]
        normalized = str(query or "").strip().lower()
        matches = [
            row for row in self._search_results
            if normalized and (
                normalized in str(row.get("name") or "").lower()
                or normalized in str(row.get("text") or "").lower()
                or normalized in str(row.get("id") or "").lower()
            )
        ]
        if matches:
            return matches[:limit]
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

    def get_channel_detail(self, channel, window="7d"):
        return self._channel_detail

    def get_channel_posts(self, channel, limit=5, page=0, window="7d"):
        return self._channel_posts

    def get_graph_data(self, window="7d", category=None, signal_focus=None, max_nodes=12):
        return self._graph_data

    def get_graph_insights(self, window="7d"):
        return self._graph_insights

    def get_top_channels(self, limit=5, window="7d"):
        return self._top_channels[:limit]

    def get_trending_topics(self, limit=5, window="7d"):
        return self._trending_topics[:limit]

    def get_node_details(self, node_id, node_type, window="7d"):
        detail = self._node_details_by_key.get((node_id, node_type))
        if detail is None:
            raise AnalyticsAPIError(
                "Node not found",
                error_type="not_found",
                status_code=404,
            )
        return detail


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

    def test_sentiment_overview_falls_back_when_sentiment_endpoint_fails(self) -> None:
        payload = get_sentiment_overview(
            FakeClient(
                sentiment_error=AnalyticsAPIError(
                    "Temporary upstream failure.",
                    error_type="upstream_error",
                    status_code=500,
                )
            ),
            GetSentimentOverviewRequest(window="7d"),
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["confidence"], "low_confidence")
        self.assertIn("/api/sentiment-distribution", payload["source_endpoints"])
        self.assertIn("temporarily unavailable", payload["summary"])

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

    def test_ask_insights_uses_alias_weighting_for_permit_question(self) -> None:
        payload = ask_insights(
            self.client,
            AskInsightsRequest(window="7d", question="Why are permits getting delayed right now?"),
        )
        self.assertNotEqual(payload["confidence"], "low_confidence")
        self.assertEqual(payload["items"][0]["topic"], "Residency permits")

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

    def test_search_entities_returns_alias_hint_when_backend_search_is_empty(self) -> None:
        payload = search_entities(
            FakeClient(search_results=[]),
            SearchEntitiesRequest(query="permits", limit=2),
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["confidence"], "low_confidence")
        self.assertEqual(payload["items"][0]["type"], "topic_hint")
        self.assertEqual(payload["items"][0]["name"], "Residency permits")
        self.assertIn("No exact backend entities matched", payload["summary"])

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

    def test_get_graph_snapshot_returns_compact_graph_summary(self) -> None:
        payload = get_graph_snapshot(
            self.client,
            GetGraphSnapshotRequest(window="7d", category=None, signal_focus="all", max_nodes=12),
        )
        self.assertEqual(payload["action"], "get_graph_snapshot")
        self.assertEqual(payload["items"][0]["topic_count"], 2)
        self.assertIn("/api/graph", payload["source_endpoints"])

    def test_get_node_context_resolves_topic_node(self) -> None:
        payload = get_node_context(
            self.client,
            GetNodeContextRequest(window="7d", entity="Residency permits", type="topic"),
        )
        self.assertEqual(payload["items"][0]["type"], "topic")
        self.assertIn("/api/node-details", payload["source_endpoints"])

    def test_get_node_context_auto_resolves_channel_via_search(self) -> None:
        payload = get_node_context(
            self.client,
            GetNodeContextRequest(window="7d", entity="Docs Chat", type="auto"),
        )
        self.assertEqual(payload["items"][0]["type"], "channel")
        self.assertIn("/api/search", payload["source_endpoints"])

    def test_investigate_topic_combines_detail_evidence_and_freshness(self) -> None:
        payload = investigate_topic(
            self.client,
            InvestigateTopicRequest(window="7d", topic="Residency permits", category="Documents"),
        )
        self.assertEqual(payload["action"], "investigate_topic")
        self.assertEqual(payload["items"][0]["topic"], "Residency permits")
        self.assertIn("/api/freshness", payload["source_endpoints"])
        self.assertIn("warning", payload.get("caveat", ""))

    def test_investigate_channel_combines_detail_posts_and_freshness(self) -> None:
        payload = investigate_channel(
            self.client,
            InvestigateChannelRequest(window="7d", channel="Docs Chat"),
        )
        self.assertEqual(payload["action"], "investigate_channel")
        self.assertEqual(payload["items"][0]["channel"], "Docs Chat")
        self.assertIn("/api/channels/posts", payload["source_endpoints"])

    def test_compare_topics_returns_comparison_item(self) -> None:
        second_topic = {
            **TOPIC_DETAIL,
            "name": "Rental costs",
            "category": "Housing",
            "mentionCount": 30,
            "growth7dPct": 45.0,
            "topChannels": ["Rent Armenia", "Expats Armenia"],
        }
        payload = compare_topics(
            FakeClient(topic_details_by_topic={"Residency permits": TOPIC_DETAIL, "Rental costs": second_topic}),
            CompareTopicsRequest(window="7d", topic_a="Residency permits", topic_b="Rental costs"),
        )
        self.assertEqual(payload["action"], "compare_topics")
        self.assertEqual(payload["items"][0]["larger_by_mentions"], "Residency permits")
        self.assertEqual(payload["items"][0]["faster_growth"], "Rental costs")

    def test_compare_channels_returns_comparison_item(self) -> None:
        second_channel = {
            **CHANNEL_DETAIL,
            "title": "Visa Support",
            "username": "visa_support",
            "postCount": 15,
            "avgViews": 910,
            "growth7dPct": 9.5,
            "topTopics": [{"name": "Visa appointments", "mentions": 9, "pct": 60}],
        }

        class CompareClient(FakeClient):
            def get_channel_detail(self, channel, window="7d"):
                if channel == "Docs Chat":
                    return CHANNEL_DETAIL
                return second_channel

        payload = compare_channels(
            CompareClient(),
            CompareChannelsRequest(window="7d", channel_a="Docs Chat", channel_b="Visa Support"),
        )
        self.assertEqual(payload["action"], "compare_channels")
        self.assertEqual(payload["items"][0]["higher_volume"], "Docs Chat")
        self.assertEqual(payload["items"][0]["higher_engagement_proxy"], "Visa Support")

    def test_investigate_question_uses_ask_insights_topic_path(self) -> None:
        payload = investigate_question(
            self.client,
            InvestigateQuestionRequest(window="7d", question="What is driving concern about residency permits?"),
        )
        self.assertEqual(payload["action"], "investigate_question")
        self.assertEqual(payload["items"][0]["topic"], "Residency permits")
        self.assertIn("/api/topics/detail", payload["source_endpoints"])

    def test_investigate_question_falls_back_to_search_when_needed(self) -> None:
        political_detail = {
            **TOPIC_DETAIL,
            "name": "Political protests",
            "category": "Politics",
            "topChannels": ["City Watch", "Civic Armenia"],
        }
        payload = investigate_question(
            FakeClient(
                dashboard={"data": {"questionBriefs": [], "problemBriefs": [], "urgencySignals": [], "trendingTopics": []}},
                insight_cards={"cards": []},
                search_results=[],
                search_results_by_query={
                    "Political protests": [
                        {
                            "type": "topic",
                            "id": "topic:Political protests",
                            "name": "Political protests",
                            "text": "Topic",
                        }
                    ],
                    "politics": [
                        {
                            "type": "topic",
                            "id": "topic:Political protests",
                            "name": "Political protests",
                            "text": "Topic",
                        }
                    ]
                },
                topic_details_by_topic={"Political protests": political_detail},
            ),
            InvestigateQuestionRequest(window="7d", question="What is happening with politics?"),
        )
        self.assertEqual(payload["action"], "investigate_question")
        self.assertIn("closest evidence-backed match", payload["summary"])

    def test_investigate_question_returns_low_confidence_when_no_topic_candidate_exists(self) -> None:
        payload = investigate_question(
            FakeClient(
                dashboard={"data": {"questionBriefs": [], "problemBriefs": [], "urgencySignals": [], "trendingTopics": []}},
                insight_cards={"cards": []},
                search_results=[],
            ),
            InvestigateQuestionRequest(window="7d", question="What is happening with food delivery discounts?"),
        )
        self.assertEqual(payload["confidence"], "low_confidence")
        self.assertLessEqual(len(payload["items"]), 3)

    def test_investigate_question_returns_alias_backed_low_confidence_when_exact_topic_is_missing(self) -> None:
        payload = investigate_question(
            FakeClient(
                dashboard={"data": {"questionBriefs": [], "problemBriefs": [], "urgencySignals": [], "trendingTopics": []}},
                insight_cards={"cards": []},
                search_results=[],
                search_results_by_query={"Residency permits": []},
                topic_details_by_topic={"Residency permits": None},
            ),
            InvestigateQuestionRequest(window="7d", question="What is driving concern about residency permits?"),
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["confidence"], "low_confidence")
        self.assertEqual(payload["items"][0]["topic"], "Residency permits")
        self.assertIn("closest local interpretation", payload["summary"])

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

    def test_investigate_question_routes_channel_question_to_channel_investigation(self) -> None:
        payload = investigate_question(
            self.client,
            InvestigateQuestionRequest(window="7d", question="What is going on in Docs Chat this week?"),
        )
        self.assertEqual(payload["action"], "investigate_question")
        self.assertEqual(payload["items"][0]["channel"], "Docs Chat")
        self.assertIn("/api/channels/detail", payload["source_endpoints"])

    def test_investigate_question_routes_graph_question_to_graph_snapshot(self) -> None:
        payload = investigate_question(
            self.client,
            InvestigateQuestionRequest(window="7d", question="Which channels are shaping the current discussion graph right now?"),
        )
        self.assertEqual(payload["action"], "investigate_question")
        self.assertEqual(payload["items"][0]["topic_count"], 2)
        self.assertIn("/api/graph", payload["source_endpoints"])

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
