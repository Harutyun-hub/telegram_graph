from __future__ import annotations

import unittest
from unittest.mock import patch

from api.dashboard_dates import build_dashboard_date_context
from api.queries import strategic


class StrategicQueryContractTests(unittest.TestCase):
    def test_topic_bubbles_uses_scoped_subqueries_and_preserves_shape(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-14")
        captured: dict[str, object] = {}
        rows = [
            {
                "name": "Housing",
                "category": "Daily Life",
                "postMentions": 11,
                "commentMentions": 7,
                "mentionCount": 18,
                "mentions7d": 9,
                "mentionsPrev7d": 4,
                "growthSupport": 13,
                "growth7dPct": 55.6,
            }
        ]

        def fake_run_query(query: str, params=None):
            captured["query"] = query
            captured["params"] = params
            return rows

        with patch.object(strategic, "run_query", side_effect=fake_run_query):
            result = strategic.get_topic_bubbles(ctx)

        self.assertEqual(result, rows)
        query = str(captured.get("query") or "")
        self.assertIn("CALL {", query)
        self.assertIn("WITH t", query)
        self.assertNotIn("CALL (t) {", query)
        self.assertIn("postMentionsRecent AS postMentions", query)
        self.assertIn("commentMentionsRecent AS commentMentions", query)
        self.assertIn("mentionCountRecent AS mentionCount", query)
        self.assertIn("growth7dPct", query)
        params = captured.get("params")
        self.assertIsInstance(params, dict)
        assert isinstance(params, dict)
        for key in ("start", "end", "current_start", "previous_start", "previous_end", "compare_days", "baseline_days", "total_days"):
            self.assertIn(key, params)

    def test_trend_lines_uses_scoped_subqueries_and_preserves_shape(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-14")
        captured: dict[str, object] = {}
        rows = [{"topic": "Housing", "bucket": "2026-04-10", "posts": 6}]

        def fake_run_query(query: str, params=None):
            captured["query"] = query
            captured["params"] = params
            return rows

        with patch.object(strategic, "run_query", side_effect=fake_run_query):
            result = strategic.get_trend_lines(ctx)

        self.assertEqual(result, rows)
        query = str(captured.get("query") or "")
        self.assertIn("CALL {", query)
        self.assertIn("WITH t", query)
        self.assertNotIn("CALL (t) {", query)
        self.assertIn("RETURN topic, bucket, mentions AS posts", query)
        params = captured.get("params")
        self.assertIsInstance(params, dict)
        assert isinstance(params, dict)
        self.assertIn("start", params)
        self.assertIn("end", params)

    def test_question_brief_candidates_uses_scoped_subqueries_and_preserves_shape(self) -> None:
        captured: dict[str, object] = {}
        rows = [
            {
                "topic": "Visa Support",
                "category": "Legal",
                "signalCount": 12,
                "uniqueUsers": 4,
                "channelCount": 3,
                "signals7d": 8,
                "signalsPrev7d": 3,
                "latestAt": "2026-04-14T09:00:00Z",
                "evidence": [{"id": "ev-1", "text": "How can I renew a visa?"}],
            }
        ]

        def fake_run_query(query: str, params=None):
            captured["query"] = query
            captured["params"] = params
            return rows

        with patch.object(strategic, "run_query", side_effect=fake_run_query):
            result = strategic.get_question_brief_candidates(days=21, limit_topics=10, evidence_per_topic=9)

        self.assertEqual(result, rows)
        query = str(captured.get("query") or "")
        self.assertIn("CALL {", query)
        self.assertIn("WITH t", query)
        self.assertNotIn("CALL (t) {", query)
        self.assertIn("t.name AS topic", query)
        self.assertIn("cat.name AS category", query)
        self.assertIn("size(uniqueChannels) AS channelCount", query)
        self.assertIn("evidence", query)
        params = captured.get("params")
        self.assertEqual(
            params,
            {
                "days": 21,
                "limit_topics": 10,
                "evidence_per_topic": 9,
            },
        )


if __name__ == "__main__":
    unittest.main()
