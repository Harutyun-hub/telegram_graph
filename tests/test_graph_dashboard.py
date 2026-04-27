from __future__ import annotations

from datetime import date
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from api.queries import graph_dashboard


class GraphDashboardNodeDetailsTests(unittest.TestCase):
    def setUp(self) -> None:
        graph_dashboard.invalidate_graph_cache()

    def _cache_ctx(self) -> SimpleNamespace:
        return SimpleNamespace(cache_key="2026-03-13:2026-03-27")

    def _scope_ctx(self) -> SimpleNamespace:
        return SimpleNamespace(
            from_date=date(2026, 3, 13),
            to_date=date(2026, 3, 27),
        )

    def test_topic_node_details_include_overview_and_evidence(self) -> None:
        topic_rows = [
            {
                "id": "topic:Iranian-Armenian Relation",
                "name": "Iranian-Armenian Relation",
                "category": "Regional Security",
                "mentionCount": 23,
                "evidenceCount": 23,
                "distinctChannels": 6,
                "trendPct": -72.0,
                "dominantSentiment": "Negative",
                "askSignalCount": 1,
                "needSignalCount": 1,
                "fearSignalCount": 13,
                "topChannels": [
                    {"id": "channel:1", "name": "Yerevan.Today Rus", "mentions": 9},
                ],
            }
        ]
        topic_detail = {
            "name": "Iranian-Armenian Relation",
            "sourceTopic": "Iranian-Armenian Relation",
            "category": "Regional Security",
            "sampleEvidence": {
                "id": "ev-1",
                "channel": "Yerevan.Today Rus",
                "author": "author-1",
                "text": "Evidence from a source post",
                "timestamp": "2026-03-27T09:00:00Z",
                "reactions": 10,
                "replies": 2,
            },
            "sampleQuote": "Evidence from a source post",
            "dailyRows": [{"day": "2026-03-27", "count": 3}],
            "weeklyRows": [{"year": 2026, "week": 13, "count": 23}],
            "latestAt": "2026-03-27T09:00:00Z",
        }
        topic_evidence = {
            "items": [
                {
                    "id": "ev-1",
                    "channel": "Yerevan.Today Rus",
                    "author": "author-1",
                    "text": "Evidence from a source post",
                    "timestamp": "2026-03-27T09:00:00Z",
                    "reactions": 10,
                    "replies": 2,
                }
            ]
        }
        topic_questions = {
            "items": [
                {
                    "id": "q-1",
                    "channel": "Yerevan.Today Rus",
                    "author": "author-2",
                    "text": "What does this mean for Armenia?",
                    "timestamp": "2026-03-27T08:00:00Z",
                    "reactions": 0,
                    "replies": 0,
                }
            ]
        }
        overview = {
            "topic": "Iranian-Armenian Relation",
            "category": "Regional Security",
            "status": "ready",
            "summaryEn": "Grounded AI summary",
            "summaryRu": "AI сводка",
        }

        with patch.object(graph_dashboard, "_resolve_context", return_value=self._cache_ctx()), \
             patch.object(graph_dashboard, "_graph_scope_topic_rows", return_value=(topic_rows, self._scope_ctx())), \
             patch.object(graph_dashboard.comparative, "get_topic_detail", return_value=topic_detail) as detail_mock, \
             patch.object(graph_dashboard.comparative, "get_topic_evidence_page", side_effect=[topic_evidence, topic_questions]) as evidence_mock, \
             patch.object(graph_dashboard.topic_overviews, "get_topic_overview", return_value=overview), \
             patch.object(graph_dashboard, "run_query", return_value=[{"name": "Regional Stability", "category": "Regional Security", "mentions": 8}]):
            details = graph_dashboard.get_node_details(
                "topic:Iranian-Armenian Relation",
                "topic",
                from_date="2026-03-13",
                to_date="2026-03-27",
            )

        self.assertIsNotNone(details)
        assert details is not None
        self.assertEqual(details["name"], "Iranian-Armenian Relation")
        self.assertEqual(details["category"], "Regional Security")
        self.assertEqual(details["overview"]["summaryEn"], "Grounded AI summary")
        self.assertEqual(len(details["evidence"]), 1)
        self.assertEqual(details["evidence"][0]["id"], "ev-1")
        self.assertEqual(len(details["questionEvidence"]), 1)
        self.assertEqual(details["questionEvidence"][0]["id"], "q-1")
        self.assertEqual(details["sampleQuote"], "Evidence from a source post")
        self.assertEqual(details["relatedTopics"][0]["name"], "Regional Stability")
        detail_mock.assert_called_once()
        self.assertEqual(evidence_mock.call_count, 2)


class GraphDashboardDataTests(unittest.TestCase):
    def setUp(self) -> None:
        graph_dashboard.invalidate_graph_cache()

    def _ctx(self) -> SimpleNamespace:
        return SimpleNamespace(
            cache_key="2026-03-13:2026-03-27",
            from_date=date(2026, 3, 13),
            to_date=date(2026, 3, 27),
            days=15,
        )

    def _topic_row(
        self,
        name: str,
        mentions: int,
        *,
        category: str = "Security",
        trend: float = 10.0,
        dominant: str = "Negative",
        fear_signals: int = 8,
        need_signals: int = 4,
        ask_signals: int = 3,
        distinct_channels: int = 2,
    ) -> dict:
        return {
            "id": f"topic:{name}",
            "name": name,
            "type": "topic",
            "category": category,
            "mentionCount": mentions,
            "postCount": mentions // 2,
            "commentCount": mentions - (mentions // 2),
            "evidenceCount": mentions,
            "distinctUsers": max(1, mentions // 3),
            "distinctChannels": distinct_channels,
            "trendPct": trend,
            "sentimentPositive": 80 if dominant == "Positive" else 10,
            "sentimentNeutral": 80 if dominant == "Neutral" else 20,
            "sentimentNegative": 70 if dominant == "Negative" else 10,
            "dominantSentiment": dominant,
            "askSignalCount": ask_signals,
            "needSignalCount": need_signals,
            "fearSignalCount": fear_signals,
            "topChannels": [{"id": "channel:one", "name": "One", "mentions": max(1, mentions // 2)}],
            "val": mentions,
        }

    def test_graph_data_is_curated_and_preserves_topic_metrics(self) -> None:
        topic_rows = [
            self._topic_row("Alpha", 30, trend=42.5),
            self._topic_row("Beta", 29, category="Services"),
            *[self._topic_row(f"Topic {idx}", 28 - idx) for idx in range(11)],
        ]

        with patch.object(graph_dashboard, "_resolve_context", return_value=self._ctx()), \
             patch.object(graph_dashboard, "_load_topic_rows", return_value=topic_rows):
            graph = graph_dashboard.get_graph_data({"max_nodes": 12})

        topics = [node for node in graph["nodes"] if node["type"] == "topic"]
        self.assertEqual(len(topics), 12)
        self.assertEqual([node["name"] for node in topics[:2]], ["Alpha", "Beta"])
        self.assertEqual(topics[0]["mentionCount"], 30)
        self.assertEqual(topics[0]["evidenceCount"], 30)
        self.assertEqual(topics[0]["trendPct"], 42.5)
        self.assertEqual(topics[0]["sentimentNegative"], 70)
        self.assertEqual(topics[0]["topChannels"][0]["name"], "One")
        self.assertTrue(any(link["type"] == "category-topic" for link in graph["links"]))
        self.assertTrue(any(
            link["type"] == "channel-topic"
            and link["source"] == "channel:one"
            and link["target"] == "topic:Alpha"
            and link["value"] == 15
            for link in graph["links"]
        ))
        self.assertEqual(graph["meta"]["visibleTopicCount"], 12)
        self.assertEqual(graph["meta"]["totalEligibleTopicCount"], 13)
        self.assertEqual(graph["meta"]["topicLimit"], 12)
        self.assertEqual(graph["meta"]["isCurated"], True)

    def test_topic_filters_are_scoped_and_ranked_deterministically(self) -> None:
        topic_rows = [
            self._topic_row("Momentum", 12, trend=90.0, fear_signals=2),
            self._topic_row("Volume", 30, trend=10.0, fear_signals=3),
            self._topic_row("Positive", 40, trend=95.0, dominant="Positive", fear_signals=5),
            self._topic_row("Other category", 50, category="Services", trend=100.0, fear_signals=5),
            self._topic_row("No fear", 60, trend=110.0, fear_signals=0),
        ]
        filters = graph_dashboard._resolve_filters({
            "category": "Security",
            "sentiments": ["Negative"],
            "signalFocus": "fear",
            "rankingMode": "momentum",
            "max_nodes": 12,
        })

        visible_topics, available_categories, total_eligible = graph_dashboard._filter_topic_rows(topic_rows, filters)

        self.assertEqual(available_categories, ["Security", "Services"])
        self.assertEqual(total_eligible, 2)
        self.assertEqual([row["name"] for row in visible_topics], ["Momentum", "Volume"])

    def test_load_topic_rows_passes_channel_and_min_mentions_filters_to_neo4j(self) -> None:
        filters = graph_dashboard._resolve_filters({
            "channels": ["@ChannelOne"],
            "minMentions": 5,
        })
        ctx = SimpleNamespace(
            start_at=date(2026, 3, 13),
            end_at=date(2026, 3, 27),
            previous_start_at=date(2026, 2, 27),
            previous_end_at=date(2026, 3, 13),
        )

        with patch.object(graph_dashboard, "run_query", return_value=[]) as run_query_mock:
            graph_dashboard._load_topic_rows(ctx, filters)

        params = run_query_mock.call_args.args[1]
        self.assertEqual(params["channels"], ["channelone"])
        self.assertEqual(params["channel_count"], 1)
        self.assertEqual(params["min_mentions"], 5)


class GraphDashboardSearchTests(unittest.TestCase):
    def setUp(self) -> None:
        graph_dashboard.invalidate_graph_cache()

    def test_search_graph_uses_unscoped_subquery(self) -> None:
        with patch.object(graph_dashboard, "run_query", return_value=[]) as run_query_mock:
            graph_dashboard.search_graph("permits", 5)

        query = run_query_mock.call_args.args[0]
        self.assertIn("CALL {", query)
        self.assertNotIn("CALL (t) {", query)


if __name__ == "__main__":
    unittest.main()
