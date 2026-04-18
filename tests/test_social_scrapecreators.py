from __future__ import annotations

import unittest
from urllib.error import HTTPError
from unittest.mock import patch

from social.scrapecreators import ScrapeCreatorsClient, SocialCollectionError


class SocialScrapeCreatorsTests(unittest.TestCase):
    def test_collect_account_requires_platform_identifier(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")

        with self.assertRaises(SocialCollectionError) as ctx:
            client.collect_account(
                {"id": "acc-1", "platform": "instagram", "account_handle": None},
                max_pages=1,
                page_size=10,
                include_tiktok=False,
            )

        self.assertEqual(ctx.exception.health_status, "invalid_identifier")

    def test_http_404_maps_to_provider_health(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")

        with patch(
            "social.providers.scrapecreators.urlopen",
            side_effect=HTTPError(
                url="https://api.scrapecreators.com/v2/instagram/user/posts",
                code=404,
                msg="not found",
                hdrs=None,
                fp=None,
            ),
        ):
            with self.assertRaises(SocialCollectionError) as ctx:
                client.fetch_instagram_posts(handle="missing-handle")

        self.assertEqual(ctx.exception.health_status, "provider_404")
        self.assertEqual(ctx.exception.status_code, 404)

    def test_normalize_payloads_emits_canonical_activity_shape(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        activities = client.normalize_payloads(
            {
                "id": "source-1",
                "entity_id": "entity-1",
                "provider_key": "scrapecreators",
                "platform": "facebook",
                "target_type": "page_id",
                "account_external_id": "196765077044445",
                "source_key": "scrapecreators:facebook:page_id:196765077044445",
                "content_types": ["ad"],
            },
            [
                {
                    "results": [
                        {
                            "ad_id": "ad-123",
                            "url": "https://facebook.com/ad/123",
                            "ad_text": "Zero monthly fees.",
                            "start_date": "2026-04-18T09:00:00+00:00",
                            "page_name": "unibank",
                            "ad_cta_type": "Learn More",
                            "display_format": "image",
                            "region_name": "Armenia",
                            "like_count": 5,
                            "comment_count": 2,
                            "share_count": 1,
                            "impression_count": 20,
                        }
                    ]
                }
            ],
        )

        self.assertEqual(len(activities), 1)
        activity = activities[0]
        self.assertTrue(activity["activity_uid"].startswith("social:"))
        self.assertEqual(activity["provider_key"], "scrapecreators")
        self.assertEqual(activity["source_key"], "scrapecreators:facebook:page_id:196765077044445")
        self.assertEqual(activity["source_kind"], "ad")
        self.assertEqual(
            activity["engagement_metrics"],
            {
                "likes": 5,
                "comments": 2,
                "shares": 1,
                "views": 0,
                "plays": 0,
                "impressions": 20,
                "reactions": 0,
            },
        )
        self.assertEqual(activity["provider_context"]["provider"], "scrapecreators")
        self.assertEqual(activity["provider_context"]["target_type"], "page_id")
        self.assertEqual(activity["provider_payload"]["ad_id"], "ad-123")


if __name__ == "__main__":
    unittest.main()
