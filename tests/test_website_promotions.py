from __future__ import annotations

import asyncio
import unittest

from social.website_promotions import WebsitePromotionResearcher


class _Reply:
    def __init__(self, text: str) -> None:
        self.text = text


class _Provider:
    def __init__(self, replies: list[str]) -> None:
        self.replies = list(replies)
        self.calls: list[str] = []

    async def chat(self, message: str, *, session_id: str | None = None, request_id: str | None = None):
        self.calls.append(message)
        return _Reply(self.replies.pop(0))


class WebsitePromotionResearcherTests(unittest.TestCase):
    def test_research_repairs_invalid_json_once(self) -> None:
        provider = _Provider(
            [
                "I found one offer: cashback.",
                """
                {
                  "company": "Example Bank",
                  "website": "https://example.am",
                  "checked_at": "2026-05-05T10:00:00+00:00",
                  "promotions": [
                    {
                      "title": "5% cashback",
                      "source_url": "https://example.am/promotions/cards",
                      "evidence_text": "5% cashback for new card customers",
                      "valid_from": null,
                      "valid_until": null,
                      "conditions": "New card customers",
                      "detected_offer_type": "cashback",
                      "confidence": 0.8
                    }
                  ]
                }
                """,
            ]
        )
        researcher = WebsitePromotionResearcher(provider=provider)

        result = asyncio.run(
            researcher.research(
                {
                    "id": "entity-1",
                    "name": "Example Bank",
                    "website": "https://example.am",
                }
            )
        )

        self.assertEqual(len(provider.calls), 2)
        self.assertEqual(len(result.promotions), 1)
        self.assertEqual(result.promotions[0]["title"], "5% cashback")

    def test_research_drops_promotions_without_same_domain_evidence(self) -> None:
        provider = _Provider(
            [
                """
                {
                  "company": "Example Bank",
                  "website": "https://example.am",
                  "promotions": [
                    {
                      "title": "External rumor",
                      "source_url": "https://other.example/promo",
                      "evidence_text": "Some outside claim",
                      "confidence": 0.9
                    },
                    {
                      "title": "Valid fee discount",
                      "source_url": "https://example.am/offers",
                      "evidence_text": "No transfer fee through May",
                      "confidence": 0.7
                    }
                  ]
                }
                """,
            ]
        )
        researcher = WebsitePromotionResearcher(provider=provider)

        result = asyncio.run(
            researcher.research(
                {
                    "id": "entity-1",
                    "name": "Example Bank",
                    "website": "https://example.am",
                }
            )
        )

        self.assertEqual(len(result.promotions), 1)
        self.assertEqual(result.promotions[0]["title"], "Valid fee discount")


if __name__ == "__main__":
    unittest.main()
