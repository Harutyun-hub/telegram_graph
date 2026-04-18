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

    def test_normalize_payloads_emits_canonical_facebook_post_shape(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        activities = client.normalize_payloads(
            {
                "id": "source-1",
                "entity_id": "entity-1",
                "provider_key": "scrapecreators",
                "platform": "facebook",
                "target_type": "page_id",
                "account_external_id": "1378368079150250",
                "source_key": "scrapecreators:facebook:page_id:1378368079150250",
                "content_types": ["post"],
            },
            [
                {
                    "posts": [
                        {
                            "id": "1204545088344463",
                            "text": "I've had such a blast doing the challenge this year!",
                            "url": "https://www.facebook.com/reel/486651220706068/",
                            "permalink": "https://www.facebook.com/reel/486651220706068/",
                            "author": {"name": "Pace Morby", "id": "100063669491743"},
                            "reactionCount": 133,
                            "commentCount": 12,
                            "publishTime": "2025-09-01T00:38:58.000Z",
                        }
                    ]
                }
            ],
        )

        self.assertEqual(len(activities), 1)
        activity = activities[0]
        self.assertTrue(activity["activity_uid"].startswith("social:"))
        self.assertEqual(activity["provider_key"], "scrapecreators")
        self.assertEqual(activity["source_key"], "scrapecreators:facebook:page_id:1378368079150250")
        self.assertEqual(activity["source_kind"], "post")
        self.assertEqual(activity["provider_item_id"], "1204545088344463")
        self.assertEqual(activity["author_handle"], "Pace Morby")
        self.assertEqual(
            activity["engagement_metrics"],
            {
                "likes": 0,
                "comments": 12,
                "shares": 0,
                "views": 0,
                "plays": 0,
                "impressions": 0,
                "reactions": 133,
            },
        )
        self.assertEqual(activity["provider_context"]["provider"], "scrapecreators")
        self.assertEqual(activity["provider_context"]["target_type"], "page_id")
        self.assertEqual(activity["provider_payload"]["id"], "1204545088344463")

    def test_collect_source_expands_bounded_facebook_comments(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        source = {
            "id": "source-1",
            "entity_id": "entity-1",
            "provider_key": "scrapecreators",
            "platform": "facebook",
            "target_type": "page_id",
            "account_external_id": "1378368079150250",
            "source_key": "scrapecreators:facebook:page_id:1378368079150250",
            "content_types": ["post"],
            "provider_metadata": {
                "include_comments": True,
                "max_post_pages": 1,
                "max_posts": 1,
                "max_comment_pages_per_post": 1,
                "max_comments_per_post": 10,
                "use_feedback_id_for_comments": True,
            },
        }
        comments = [
            {
                "id": f"comment-{index}",
                "text": f"Comment {index}",
                "created_at": "2025-09-01T00:38:58.000Z",
                "reply_count": index,
                "reaction_count": index + 1,
                "author": {"name": f"User {index}"},
            }
            for index in range(12)
        ]

        with patch.object(
            client,
            "fetch_facebook_profile_posts",
            return_value={
                "success": True,
                "posts": [
                    {
                        "id": "1204545088344463",
                        "text": "Primary post",
                        "url": "https://www.facebook.com/reel/486651220706068/",
                        "author": {"name": "Page Name", "id": "1378368079150250"},
                        "reactionCount": 133,
                        "commentCount": 12,
                        "publishTime": "2025-09-01T00:38:58.000Z",
                    }
                ],
                "has_next_page": True,
                "cursor": "post-cursor-2",
            },
        ), patch.object(
            client,
            "fetch_facebook_post",
            return_value={
                "success": True,
                "post_id": "1204545088344463",
                "feedback_id": "ZmVlZGJhY2s6MTIwNDU0NTA4ODM0NDQ2Mw==",
                "comment_count": 12,
                "reaction_count": 133,
                "creation_time": "2025-09-01T00:38:58.000Z",
                "url": "https://www.facebook.com/reel/486651220706068/",
                "description": "Primary post",
            },
        ) as post_detail_mock, patch.object(
            client,
            "fetch_facebook_post_comments",
            return_value={
                "success": True,
                "comments": comments,
                "has_next_page": True,
                "cursor": "comment-cursor-2",
            },
        ) as comments_mock:
            activities = client.collect_source(source, max_pages=3, page_size=50)

        self.assertEqual(len(activities), 11)
        self.assertEqual(activities[0]["source_kind"], "post")
        self.assertEqual(activities[1]["source_kind"], "comment")
        self.assertEqual(activities[1]["parent_provider_item_id"], activities[0]["provider_item_id"])
        self.assertEqual(activities[1]["parent_activity_uid"], activities[0]["activity_uid"])
        self.assertEqual(activities[1]["provider_context"]["feedback_id"], "ZmVlZGJhY2s6MTIwNDU0NTA4ODM0NDQ2Mw==")
        self.assertEqual(activities[1]["engagement_metrics"]["comments"], 0)
        self.assertEqual(activities[1]["engagement_metrics"]["reactions"], 1)
        self.assertEqual(activities[-1]["provider_item_id"], "comment-9")
        post_detail_mock.assert_called_once()
        comments_mock.assert_called_once()

    def test_collect_source_falls_back_to_page_url_when_page_id_returns_empty_posts(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        source = {
            "id": "source-1",
            "entity_id": "entity-1",
            "provider_key": "scrapecreators",
            "platform": "facebook",
            "target_type": "page_id",
            "account_external_id": "1378368079150250",
            "source_key": "scrapecreators:facebook:page_id:1378368079150250",
            "content_types": ["post"],
            "metadata": {
                "page_url": "https://www.facebook.com/nikol.pashinyan",
            },
        }

        with patch.object(
            client,
            "fetch_facebook_profile_posts",
            side_effect=[
                {"success": True, "posts": [], "cursor": None},
                {
                    "success": True,
                    "posts": [
                        {
                            "id": "1498926111588685",
                            "text": "Primary post",
                            "url": "https://www.facebook.com/reel/1583932262675815/",
                            "author": {"name": "Nikol Pashinyan / Նիկոլ Փաշինյան", "id": "100044139324388"},
                            "reactionCount": 568,
                            "commentCount": 53,
                            "publishTime": "2026-04-17T17:30:14.000Z",
                        }
                    ],
                    "cursor": None,
                },
            ],
        ) as posts_mock:
            activities = client.collect_source(source, max_pages=1, page_size=50)

        self.assertEqual(len(activities), 1)
        self.assertEqual(activities[0]["source_kind"], "post")
        self.assertEqual(activities[0]["provider_item_id"], "1498926111588685")
        self.assertEqual(posts_mock.call_count, 2)
        first_call = posts_mock.call_args_list[0]
        second_call = posts_mock.call_args_list[1]
        self.assertEqual(first_call.kwargs["page_id"], "1378368079150250")
        self.assertIsNone(first_call.kwargs.get("url"))
        self.assertEqual(second_call.kwargs["url"], "https://www.facebook.com/nikol.pashinyan")
        self.assertIsNone(second_call.kwargs.get("page_id"))


if __name__ == "__main__":
    unittest.main()
