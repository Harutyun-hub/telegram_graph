from __future__ import annotations

import unittest

from social.analysis import SocialActivityAnalyzer


class SocialAnalysisTests(unittest.TestCase):
    def test_parse_batch_response_accepts_wrapped_items_object(self) -> None:
        raw = '{"items":[{"batch_index":0,"activity_uid":"facebook:ad:1"},{"batch_index":1,"activity_uid":"facebook:ad:2"}]}'
        parsed = SocialActivityAnalyzer._parse_batch_response(raw)
        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[1]["activity_uid"], "facebook:ad:2")

    def test_normalize_result_clamps_sentiment_score(self) -> None:
        normalized = SocialActivityAnalyzer._normalize_result(
            {
                "batch_index": 0,
                "activity_uid": "facebook:ad:1",
                "summary": "Offer",
                "sentiment": "Positive",
                "sentiment_score": 9,
                "topics": ["Credit Cards"],
            }
        )
        self.assertEqual(normalized["summary"], "Offer")
        self.assertEqual(normalized["sentiment"], "Positive")
        self.assertEqual(normalized["sentiment_score"], 1.0)
        self.assertEqual(normalized["topics"], ["Credit Card"])

    def test_normalize_result_drops_structural_signal_and_rejected_topics(self) -> None:
        raw = {
            "batch_index": 0,
            "activity_uid": "facebook:post:1",
            "topics": [
                "Media And News",
                "Community Solidarity",
                "unknown",
                "Tax Policy",
            ],
        }

        normalized = SocialActivityAnalyzer._normalize_result(raw)

        self.assertEqual(normalized["topics"], ["Tax Policy"])
        self.assertEqual(raw["topics"], ["Media And News", "Community Solidarity", "unknown", "Tax Policy"])

    def test_normalize_result_preserves_issue_topics_as_canonical_strings(self) -> None:
        normalized = SocialActivityAnalyzer._normalize_result(
            {
                "batch_index": 0,
                "activity_uid": "facebook:comment:1",
                "topics": ["credit cards", "Tax Policy", "credit cards"],
            }
        )

        self.assertEqual(normalized["topics"], ["Credit Card", "Tax Policy"])


if __name__ == "__main__":
    unittest.main()
