from __future__ import annotations

import unittest
from unittest.mock import patch

from api.dashboard_dates import build_dashboard_date_context
from api.queries import comparative


class TopicsPageSummaryTests(unittest.TestCase):
    def test_get_all_topics_accepts_minimal_summary_rows(self) -> None:
        ctx = build_dashboard_date_context("2026-04-01", "2026-04-15")
        minimal_row = {
            "name": "Community Solidarity",
            "category": "Community Life",
            "postCount": 3,
            "commentCount": 2,
            "mentionCount": 5,
            "userCount": 2,
            "last7Mentions": 5,
            "prev7Mentions": 4,
            "evidenceCount": 5,
            "distinctUsers": 2,
            "distinctChannels": 1,
            "sentimentPositive": 20,
            "sentimentNeutral": 40,
            "sentimentNegative": 40,
            "growth7dPct": 25.0,
            "sampleEvidenceId": "",
            "sampleQuote": "",
            "topChannels": [],
        }

        with patch.object(comparative, "run_query", return_value=[minimal_row]), patch.object(
            comparative, "_is_topics_page_topic_allowed", return_value=True
        ):
            rows = comparative.get_all_topics(page=0, size=50, ctx=ctx)

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["sourceTopic"], "Community Solidarity")
        self.assertEqual(row["sampleEvidenceId"], "")
        self.assertEqual(row["sampleQuote"], "")
        self.assertEqual(row["topChannels"], [])
        self.assertEqual(row["mentionCount"], 5)
        self.assertEqual(row["evidenceCount"], 5)
        self.assertEqual(row["distinctUsers"], 2)
        self.assertEqual(row["deltaMentions"], 1)

    def test_get_all_topics_query_avoids_evidence_row_collection(self) -> None:
        ctx = build_dashboard_date_context("2026-04-01", "2026-04-15")

        with patch.object(comparative, "run_query", return_value=[] ) as run_query:
            comparative.get_all_topics(page=0, size=50, ctx=ctx)

        query = run_query.call_args.args[0]
        self.assertNotIn("head(evidenceRows)", query)
        self.assertNotIn("collect({", query)
        self.assertNotIn("collect(channel)[..3] AS topChannels", query)
        self.assertIn("count(*) AS evidenceCount", query)


if __name__ == "__main__":
    unittest.main()
