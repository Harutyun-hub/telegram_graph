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
                "lens_relevance": "high",
                "matched_lenses": ["finance_markets"],
                "lens_signals": ["product positioning"],
                "topics": [{"name": "Credit Cards", "evidence": "Offer", "confidence": 0.82}],
            }
        )
        self.assertEqual(normalized["summary"], "Offer")
        self.assertEqual(normalized["sentiment"], "Positive")
        self.assertEqual(normalized["sentiment_score"], 1.0)
        self.assertEqual(normalized["topics"][0]["name"], "Credit Cards")
        self.assertEqual(normalized["topics"][0]["confidence"], 0.82)
        self.assertEqual(normalized["lens_quality"], "accepted")
        self.assertEqual(normalized["matched_lenses"], ["finance_markets"])


if __name__ == "__main__":
    unittest.main()
