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
        self.assertEqual(len(normalized), 2)
        self.assertEqual(normalized[0]["source_kind"], "post")
        self.assertIsNotNone(normalized[0]["published_at"])
        self.assertEqual(normalized[1]["source_kind"], "comment")
        self.assertEqual(normalized[1]["source_url"], "https://www.facebook.com/post-1")

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

    def test_fetch_facebook_ads_uses_documented_ad_library_path(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")

        with patch.object(client, "_get", return_value={"results": []}) as get_mock:
            payload = client.fetch_facebook_ads(page_id="138239466852", page_size=25)

        self.assertEqual(payload, {"results": []})
        get_mock.assert_called_once()
        path, params = get_mock.call_args.args
        self.assertEqual(path, "/v1/facebook/adLibrary/company/ads")
        self.assertEqual(params["pageId"], "138239466852")
        self.assertEqual(params["count"], 25)
        self.assertEqual(params["status"], "ACTIVE")

    def test_collect_instagram_limits_to_latest_five_posts(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        account = {
            "id": "acc-instagram",
            "platform": "instagram",
            "source_kind": "instagram_profile",
            "account_handle": "xtb_de",
            "entity": {"id": "entity-1"},
        }

        with patch.object(
            client,
            "fetch_instagram_posts",
            return_value={
                "items": [
                    {
                        "id": f"post-{index}",
                        "text": f"Instagram post {index}",
                        "url": f"https://www.instagram.com/p/post-{index}/",
                    }
                    for index in range(1, 9)
                ]
            },
        ) as posts_mock:
            payloads = client.collect_account(account, max_pages=10, page_size=50, include_tiktok=False)
            normalized = client.normalize_payloads(account, payloads)

        posts_mock.assert_called_once()
        self.assertEqual(posts_mock.call_args.kwargs["page_size"], 5)
        self.assertEqual(len(normalized), 5)
        self.assertEqual([item["provider_item_id"] for item in normalized], [f"post-{index}" for index in range(1, 6)])

    def test_collect_facebook_page_limits_to_five_posts_and_comments(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        account = {
            "id": "acc-facebook-page",
            "platform": "facebook",
            "source_kind": "facebook_page",
            "metadata": {"page_url": "https://www.facebook.com/xtb"},
            "entity": {"id": "entity-1"},
        }

        def _post(index: int) -> dict:
            return {
                "id": f"post-{index}",
                "text": f"Facebook post {index}",
                "url": f"https://www.facebook.com/post-{index}",
                "permalink": f"https://www.facebook.com/post-{index}",
                "publishTime": 1734553170 + index,
            }

        def _comments(*, post_url: str, **_: object) -> dict:
            post_id = post_url.rsplit("-", 1)[-1]
            return {
                "comments": [
                    {
                        "id": f"comment-{post_id}",
                        "text": f"Comment text {post_id}",
                        "created_at": "2025-09-01T01:10:40.000Z",
                    }
                ]
            }

        with patch.object(
            client,
            "fetch_facebook_profile_posts",
            side_effect=[
                {"posts": [_post(1), _post(2), _post(3)], "cursor": "next-page"},
                {"posts": [_post(4), _post(5), _post(6)]},
            ],
        ) as posts_mock, patch.object(
            client,
            "fetch_facebook_post_comments",
            side_effect=_comments,
        ) as comments_mock:
            payloads = client.collect_account(account, max_pages=10, page_size=10, include_tiktok=False)
            normalized = client.normalize_payloads(account, payloads)

        self.assertEqual(posts_mock.call_count, 2)
        self.assertEqual(comments_mock.call_count, 5)
        posts = [item for item in normalized if item["source_kind"] == "post"]
        comments = [item for item in normalized if item["source_kind"] == "comment"]
        self.assertEqual(len(posts), 5)
        self.assertEqual(len(comments), 5)
        self.assertNotIn("post-6", {item["provider_item_id"] for item in posts})

    def test_collect_google_ads_uses_yesterday_window(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        account = {
            "id": "acc-google",
            "platform": "google",
            "source_kind": "google_domain",
            "domain": "xtb.com",
            "entity": {"id": "entity-1"},
        }

        with patch("social.scrapecreators._utc_yesterday_date", return_value="2026-05-03"), patch.object(
            client,
            "fetch_google_ads",
            return_value={"ads": []},
        ) as ads_mock:
            payloads = client.collect_account(account, max_pages=10, page_size=50, include_tiktok=False)

        self.assertEqual(payloads, [{"ads": []}])
        ads_mock.assert_called_once()
        self.assertEqual(ads_mock.call_args.kwargs["start_date"], "2026-05-03")
        self.assertEqual(ads_mock.call_args.kwargs["end_date"], "2026-05-03")

    def test_normalize_meta_ads_extracts_snapshot_body_text(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        account = {
            "id": "acc-meta-ads",
            "platform": "facebook",
            "source_kind": "meta_ads",
            "account_external_id": "138239466852",
            "entity": {"id": "entity-1"},
        }

        normalized = client.normalize_payloads(
            account,
            [
                {
                    "results": [
                        {
                            "adArchiveId": "ad-1",
                            "url": "https://www.facebook.com/ads/library/?id=ad-1",
                            "snapshot": {
                                "body": {"text": "Trade market volatility with XTB."},
                                "title": "Fallback title",
                            },
                        }
                    ]
                }
            ],
        )

        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0]["source_kind"], "ad")
        self.assertEqual(normalized[0]["text_content"], "Trade market volatility with XTB.")

    def test_normalize_google_ads_extracts_ad_text(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        account = {
            "id": "acc-google",
            "platform": "google",
            "source_kind": "google_domain",
            "domain": "xtb.com",
            "entity": {"id": "entity-1"},
        }

        normalized = client.normalize_payloads(
            account,
            [
                {
                    "results": [
                        {
                            "creativeId": "creative-1",
                            "url": "https://adstransparency.google.com/advertiser/creative-1",
                            "ad_text": "Explore trading education and market analysis.",
                        }
                    ]
                }
            ],
        )

        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0]["source_kind"], "ad")
        self.assertEqual(normalized[0]["text_content"], "Explore trading education and market analysis.")

    def test_normalize_without_readable_text_does_not_store_raw_payload(self) -> None:
        client = ScrapeCreatorsClient(api_key="test-key")
        account = {
            "id": "acc-instagram",
            "platform": "instagram",
            "source_kind": "instagram_profile",
            "account_handle": "xtb_de",
            "entity": {"id": "entity-1"},
        }

        normalized = client.normalize_payloads(
            account,
            [
                {
                    "items": [
                        {
                            "id": "post-1",
                            "url": "https://www.instagram.com/p/post-1/",
                            "pk": "raw-only",
                            "bit_flags": 0,
                        }
                    ]
                }
            ],
        )

        self.assertEqual(len(normalized), 1)
        self.assertIsNone(normalized[0]["text_content"])


if __name__ == "__main__":
    unittest.main()
