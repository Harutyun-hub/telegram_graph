from __future__ import annotations

import unittest
from unittest.mock import patch

from api import behavioral_briefs


class ServiceGapDetectionTests(unittest.TestCase):
    def test_hidden_service_request_in_non_service_topic_is_aligned(self) -> None:
        self.assertTrue(
            behavioral_briefs._is_service_evidence_aligned(
                topic="Political Protest",
                category="Opposition & Protest",
                message="Can anyone recommend a lawyer to help with detention paperwork after the rally?",
                context="Several people were discussing arrests after the protest.",
                ask_like=1,
                support_intent=1,
            )
        )

    def test_abstract_political_complaint_is_not_service_aligned(self) -> None:
        self.assertFalse(
            behavioral_briefs._is_service_evidence_aligned(
                topic="Political Protest",
                category="Opposition & Protest",
                message="The government is failing everyone and this situation is unacceptable.",
                context="People are angry after the march.",
                ask_like=1,
                support_intent=0,
            )
        )

    def test_normalize_candidates_keeps_only_concrete_service_requests(self) -> None:
        rows = [
            {
                "topic": "Political Protest",
                "category": "Opposition & Protest",
                "latestAt": "2026-03-19T12:00:00Z",
                "evidence": [
                    {
                        "id": "ev-1",
                        "kind": "comment",
                        "text": "Can anyone recommend a lawyer for detention paperwork?",
                        "parentText": "People are sharing what happened after the rally.",
                        "channel": "chan-a",
                        "userId": "u1",
                        "timestamp": "2026-03-19T11:00:00Z",
                        "label": "Negative",
                        "distressHit": 1,
                        "askLike": 1,
                        "supportIntent": 1,
                    },
                    {
                        "id": "ev-2",
                        "kind": "comment",
                        "text": "Need legal help and a translator for the police documents.",
                        "parentText": "Follow-up requests from the same discussion.",
                        "channel": "chan-b",
                        "userId": "u2",
                        "timestamp": "2026-03-18T10:00:00Z",
                        "label": "Urgent",
                        "distressHit": 1,
                        "askLike": 1,
                        "supportIntent": 1,
                    },
                    {
                        "id": "ev-3",
                        "kind": "post",
                        "text": "Where can I get legal consultation for protest-related court papers?",
                        "parentText": "",
                        "channel": "chan-b",
                        "userId": "",
                        "timestamp": "2026-03-11T10:00:00Z",
                        "label": "Negative",
                        "distressHit": 0,
                        "askLike": 1,
                        "supportIntent": 0,
                    },
                    {
                        "id": "ev-4",
                        "kind": "comment",
                        "text": "The authorities are corrupt and nobody listens.",
                        "parentText": "General outrage in the thread.",
                        "channel": "chan-c",
                        "userId": "u3",
                        "timestamp": "2026-03-17T10:00:00Z",
                        "label": "Negative",
                        "distressHit": 1,
                        "askLike": 1,
                        "supportIntent": 0,
                    },
                ],
            }
        ]

        clusters = behavioral_briefs._normalize_candidates(rows, "service")

        self.assertEqual(len(clusters), 1)
        self.assertEqual(clusters[0]["messages"], 3)
        self.assertEqual(clusters[0]["uniqueUsers"], 3)
        self.assertEqual(clusters[0]["channels"], 2)
        self.assertEqual(clusters[0]["signals7d"], 2)
        self.assertEqual(clusters[0]["signalsPrev7d"], 1)

    def test_refresh_kind_for_services_has_no_deterministic_fallback(self) -> None:
        cluster = {
            "clusterId": "sg-political-protest",
            "topic": "Political Protest",
            "category": "Opposition & Protest",
            "messages": 3,
            "uniqueUsers": 2,
            "channels": 1,
            "signals7d": 2,
            "signalsPrev7d": 1,
            "trend7dPct": 20,
            "latestAt": "2026-03-19T12:00:00Z",
            "unmetPct": 88,
            "signals": [
                {
                    "id": "ev-1",
                    "kind": "comment",
                    "channel": "chan-a",
                    "userId": "u1",
                    "timestamp": "2026-03-19T11:00:00Z",
                    "message": "Need a lawyer for detention documents.",
                    "context": "People are seeking help after the rally.",
                    "label": "Urgent",
                    "distressHit": 1,
                    "askLike": 1,
                    "supportIntent": 1,
                },
                {
                    "id": "ev-2",
                    "kind": "comment",
                    "channel": "chan-a",
                    "userId": "u2",
                    "timestamp": "2026-03-18T11:00:00Z",
                    "message": "Where can I find legal aid for court paperwork?",
                    "context": "The thread is full of follow-up requests.",
                    "label": "Negative",
                    "distressHit": 0,
                    "askLike": 1,
                    "supportIntent": 1,
                },
            ],
        }

        with patch("api.behavioral_briefs._synthesize_service_cards", return_value=[]):
            cards, state, changed = behavioral_briefs._refresh_kind(
                kind="service",
                clusters=[cluster],
                state_clusters={},
                force=True,
            )

        self.assertEqual(changed, 1)
        self.assertEqual(cards, [])
        self.assertEqual(state["sg-political-protest"]["status"], "rejected")
        self.assertEqual(state["sg-political-protest"]["rejectionReason"], "insufficient_grounding")


if __name__ == "__main__":
    unittest.main()
