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
