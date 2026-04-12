from __future__ import annotations

import unittest

from ingester.neo4j_writer import _collect_post_topic_items, _extract_message_topic_items
from processor.intent_extractor import _normalize_payload


class TopicMessageTaggingTests(unittest.TestCase):
    def test_post_topics_only_use_direct_post_analysis(self) -> None:
        post_analysis = {
            "topics": ["placeholder"],
            "raw_llm_response": {
                "topics": [
                    {
                        "name": "Cryptocurrency",
                        "closest_category": "Technology",
                        "domain": "Technology",
                    }
                ],
                "message_topics": [
                    {
                        "comment_id": "comment-1",
                        "topics": [
                            {
                                "name": "Customer Support",
                                "closest_category": "Social Services",
                                "domain": "Society",
                            }
                        ],
                    }
                ],
            },
        }

        items = _collect_post_topic_items(post_analysis)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["name"], "Cryptocurrency")

    def test_message_topics_are_scoped_to_comment_id(self) -> None:
        raw_response = {
            "message_topics": [
                {
                    "comment_id": "comment-1",
                    "topics": [
                        {
                            "name": "TestTopic",
                            "closest_category": "General",
                            "domain": "General",
                        }
                    ],
                },
                {
                    "comment_id": "comment-2",
                    "topics": [
                        {
                            "name": "OtherTopic",
                            "closest_category": "General",
                            "domain": "General",
                        }
                    ],
                },
            ]
        }

        first_items = _extract_message_topic_items(raw_response, "comment-1")
        second_items = _extract_message_topic_items(raw_response, "comment-2")
        missing_items = _extract_message_topic_items(raw_response, "comment-3")

        self.assertEqual([item["name"].lower() for item in first_items], ["testtopic"])
        self.assertEqual([item["name"].lower() for item in second_items], ["othertopic"])
        self.assertEqual(missing_items, [])

    def test_normalize_payload_keeps_per_comment_topics_and_aggregates_names(self) -> None:
        payload = _normalize_payload(
            {
                "topics": [],
                "message_topics": [
                    {
                        "message_ref": "MSG 1",
                        "comment_id": "comment-1",
                        "topics": [
                            {
                                "name": "TestTopic",
                                "closest_category": "General",
                                "domain": "General",
                            }
                        ],
                    }
                ],
            }
        )

        self.assertEqual(payload["message_topics"][0]["comment_id"], "comment-1")
        self.assertEqual([item["name"].lower() for item in payload["topics"]], ["testtopic"])

    def test_normalize_payload_normalizes_message_sentiment_enum_values(self) -> None:
        payload = _normalize_payload(
            {
                "topics": [],
                "sentiment_score": 0.25,
                "message_sentiments": [
                    {
                        "message_ref": "MSG 1",
                        "comment_id": "comment-1",
                        "sentiment": "very urgent",
                    },
                    {
                        "message_ref": "MSG 2",
                        "comment_id": "comment-2",
                        "sentiment": "unknown",
                    },
                ],
            }
        )

        self.assertEqual(payload["message_sentiments"][0]["sentiment"], "Very_Urgent")
        self.assertIsNone(payload["message_sentiments"][1]["sentiment"])


if __name__ == "__main__":
    unittest.main()
