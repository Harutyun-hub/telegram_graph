from __future__ import annotations

import unittest
from unittest.mock import patch

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
                "activity_uid": "facebook:post:1",
                "summary": "Offer",
                "sentiment": "Positive",
                "sentiment_score": 9,
                "primary_intent": "Praise",
                "topics": ["Tax Policy"],
            }
        )
        self.assertEqual(normalized["summary"], "Offer")
        self.assertEqual(normalized["marketing_intent"], "Praise")
        self.assertEqual(normalized["sentiment"], "Positive")
        self.assertEqual(normalized["sentiment_score"], 1.0)
        self.assertEqual(normalized["topics"], ["Tax Policy"])

    def test_analyze_batch_accepts_thread_response_without_batch_index(self) -> None:
        analyzer = object.__new__(SocialActivityAnalyzer)
        item = {
            "id": "activity-1",
            "entity_id": "entity-1",
            "platform": "facebook",
            "activity_uid": "facebook:post:1",
            "entity": {"id": "entity-1", "name": "Nikol Pashinyan"},
        }

        with patch.object(
            analyzer,
            "_request",
            return_value='{"items":[{"activity_uid":"facebook:post:1","summary":"Thread summary","sentiment":"Mixed","topics":["Tax Policy"]}]}',
        ):
            rows = analyzer.analyze_batch([item])

        self.assertEqual(rows[0]["activity_id"], "activity-1")
        self.assertEqual(rows[0]["analysis_payload"]["summary"], "Thread summary")
        self.assertEqual(rows[0]["analysis_payload"]["topics"], ["Tax Policy"])

    def test_build_batch_payload_includes_bounded_thread_comments(self) -> None:
        analyzer = object.__new__(SocialActivityAnalyzer)
        payload = analyzer._build_batch_payload(
            [
                {
                    "activity_uid": "facebook:post:1",
                    "source_kind": "post",
                    "platform": "facebook",
                    "text_content": "Parent post",
                    "entity": {"id": "entity-1", "name": "Nikol Pashinyan"},
                    "thread_comments": [
                        {"activity_uid": "facebook:comment:1", "provider_item_id": "comment-1", "text_content": "First"},
                        {"activity_uid": "facebook:comment:2", "provider_item_id": "comment-2", "text_content": "Second"},
                    ],
                }
            ]
        )

        item = payload["items"][0]
        self.assertEqual(item["activity_uid"], "facebook:post:1")
        self.assertEqual(len(item["comments"]), 2)
        self.assertEqual(item["comments"][0]["comment_id"], "facebook:comment:1")

    def test_normalize_result_drops_structural_signal_and_rejected_topics(self) -> None:
        raw = {
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


if __name__ == "__main__":
    unittest.main()
