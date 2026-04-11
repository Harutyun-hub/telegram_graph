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
            "social.scrapecreators.urlopen",
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


if __name__ == "__main__":
    unittest.main()
