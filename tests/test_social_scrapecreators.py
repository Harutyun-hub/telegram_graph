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

    def test_collect_facebook_page_uses_posts_and_comments(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        account = {
            "id": "acc-facebook-page",
            "platform": "facebook",
            "source_kind": "facebook_page",
            "metadata": {"page_url": "https://www.facebook.com/nikol.pashinyan"},
        }

        with patch.object(
            client,
            "fetch_facebook_profile_posts",
            return_value={
                "posts": [
                    {
                        "id": "post-1",
                        "text": "Main post",
                        "url": "https://www.facebook.com/post-1",
                        "permalink": "https://www.facebook.com/post-1",
                        "author": {"name": "Nikol Pashinyan", "id": "1378368079150250"},
                        "reactionCount": 5,
                        "commentCount": 2,
                        "publishTime": 1734553170,
                    }
                ]
            },
        ) as posts_mock, patch.object(
            client,
            "fetch_facebook_post_comments",
            return_value={
                "comments": [
                    {
                        "id": "comment-1",
                        "text": "Comment text",
                        "created_at": "2025-09-01T01:10:40.000Z",
                        "author": {"name": "Commenter", "id": "user-1"},
                        "reaction_count": 3,
                        "reply_count": 1,
                    }
                ]
            },
        ) as comments_mock:
            payloads = client.collect_account(account, max_pages=1, page_size=10, include_tiktok=False)
            normalized = client.normalize_payloads(
                {**account, "entity": {"id": "entity-1"}},
                payloads,
            )

        posts_mock.assert_called_once()
        comments_mock.assert_called_once()
        self.assertEqual(posts_mock.call_args.kwargs["page_size"], 3)
        self.assertEqual(comments_mock.call_args.kwargs["page_size"], 10)
        self.assertEqual(len(normalized), 2)
        self.assertEqual(normalized[0]["source_kind"], "post")
        self.assertIsNotNone(normalized[0]["published_at"])
        self.assertEqual(normalized[1]["source_kind"], "comment")
        self.assertEqual(normalized[1]["parent_activity_uid"], normalized[0]["activity_uid"])
        self.assertEqual(normalized[1]["source_url"], "https://www.facebook.com/post-1")

    def test_collect_facebook_page_supports_separate_post_and_comment_limits(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        account = {
            "id": "acc-facebook-page",
            "platform": "facebook",
            "source_kind": "facebook_page",
            "metadata": {"page_url": "https://www.facebook.com/nikol.pashinyan"},
        }

        with patch.object(
            client,
            "fetch_facebook_profile_posts",
            return_value={
                "posts": [
                    {
                        "id": "post-1",
                        "text": "Main post",
                        "url": "https://www.facebook.com/post-1",
                        "permalink": "https://www.facebook.com/post-1",
                        "publishTime": 1734553170,
                    },
                    {
                        "id": "post-2",
                        "text": "Second post",
                        "url": "https://www.facebook.com/post-2",
                        "permalink": "https://www.facebook.com/post-2",
                        "publishTime": 1734553180,
                    },
                ]
            },
        ) as posts_mock, patch.object(
            client,
            "fetch_facebook_post_comments",
            return_value={"comments": [{"id": str(index), "text": f"Comment {index}"} for index in range(25)]},
        ) as comments_mock:
            payloads = client.collect_account(
                account,
                max_pages=1,
                page_size=50,
                include_tiktok=False,
                facebook_page_post_limit=1,
                facebook_page_comment_limit=20,
            )
            normalized = client.normalize_payloads(
                {**account, "entity": {"id": "entity-1"}},
                payloads,
            )

        posts_mock.assert_called_once()
        comments_mock.assert_called_once()
        self.assertEqual(posts_mock.call_args.kwargs["page_size"], 1)
        self.assertEqual(comments_mock.call_args.kwargs["page_size"], 20)
        self.assertEqual(len([row for row in normalized if row["source_kind"] == "post"]), 1)
        self.assertEqual(len([row for row in normalized if row["source_kind"] == "comment"]), 20)

    def test_collect_meta_ads_still_requires_id(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")

        with self.assertRaises(SocialCollectionError) as ctx:
            client.collect_account(
                {"id": "acc-meta-ads", "platform": "facebook", "source_kind": "meta_ads", "account_external_id": None},
                max_pages=1,
                page_size=10,
                include_tiktok=False,
            )

        self.assertEqual(ctx.exception.health_status, "invalid_identifier")


if __name__ == "__main__":
    unittest.main()
