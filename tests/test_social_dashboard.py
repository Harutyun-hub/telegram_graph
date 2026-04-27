from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from api import social_dashboard
from api import server
from api.social_dashboard import build_social_dashboard_snapshot


class _FakeSocialDashboardStore:
    def __init__(self, *, empty: bool = False) -> None:
        self.empty = empty
        self.activities = [] if empty else [
            {
                "id": "activity-post-1",
                "entity_id": "entity-1",
                "account_id": "account-fb-page",
                "activity_uid": "facebook:post:post-1",
                "platform": "facebook",
                "source_kind": "post",
                "provider_item_id": "post-1",
                "source_url": "https://facebook.com/example/posts/1",
                "text_content": "Why are card fees so high? The app support is slow.",
                "published_at": "2026-04-10T10:00:00+00:00",
                "author_handle": "customer-1",
                "cta_type": None,
                "content_format": "Text",
                "region_name": "Armenia",
                "engagement_metrics": {"like_count": 4, "comment_count": 2, "share_count": 1},
                "assets": [{"kind": "image", "url": "https://cdn.example/post.jpg"}],
                "provider_payload": {},
                "ingest_status": "normalized",
                "analysis_status": "analyzed",
                "graph_status": "synced",
                "last_seen_at": "2026-04-10T10:03:00+00:00",
                "created_at": "2026-04-10T10:03:00+00:00",
            },
            {
                "id": "activity-comment-1",
                "entity_id": "entity-1",
                "account_id": "account-fb-page",
                "activity_uid": "facebook:comment:comment-1",
                "platform": "facebook",
                "source_kind": "comment",
                "provider_item_id": "comment-1",
                "source_url": "https://facebook.com/example/posts/1",
                "text_content": "Customer service does not answer quickly.",
                "published_at": "2026-04-10T10:05:00+00:00",
                "author_handle": "customer-2",
                "cta_type": None,
                "content_format": "Comment",
                "region_name": "Armenia",
                "engagement_metrics": {"reactionCount": 3},
                "assets": [],
                "provider_payload": {},
                "ingest_status": "normalized",
                "analysis_status": "analyzed",
                "graph_status": "synced",
                "last_seen_at": "2026-04-10T10:06:00+00:00",
                "created_at": "2026-04-10T10:06:00+00:00",
            },
            {
                "id": "activity-ad-1",
                "entity_id": "entity-1",
                "account_id": "account-meta",
                "activity_uid": "facebook:ad:ad-1",
                "platform": "facebook",
                "source_kind": "meta_ads",
                "provider_item_id": "ad-1",
                "source_url": "https://facebook.com/ads/ad-1",
                "text_content": "Apply for a credit card with zero monthly fee.",
                "published_at": "2026-04-11T11:00:00+00:00",
                "author_handle": "example-bank",
                "cta_type": "Apply Now",
                "content_format": "Image",
                "region_name": "Armenia",
                "engagement_metrics": {"impression_count": 1000, "click_count": 55},
                "assets": [],
                "provider_payload": {},
                "ingest_status": "normalized",
                "analysis_status": "analyzed",
                "graph_status": "synced",
                "last_seen_at": "2026-04-11T11:02:00+00:00",
                "created_at": "2026-04-11T11:02:00+00:00",
            },
        ]
        self.analyses = [] if empty else [
            {
                "activity_id": "activity-post-1",
                "summary": "Customers ask about card fees and support speed.",
                "marketing_intent": None,
                "sentiment": "negative",
                "sentiment_score": -0.6,
                "analysis_payload": {
                    "summary": "Customers ask about card fees and support speed.",
                    "topics": ["Card Fees"],
                    "pain_points": ["High fees", "Slow support"],
                    "customer_intent": "Questions",
                    "sentiment": "negative",
                    "sentiment_score": -0.6,
                },
                "raw_model_output": {},
                "analyzed_at": "2026-04-10T10:07:00+00:00",
            },
            {
                "activity_id": "activity-comment-1",
                "summary": "Customer service speed complaint.",
                "marketing_intent": None,
                "sentiment": "negative",
                "sentiment_score": -0.5,
                "analysis_payload": {
                    "summary": "Customer service speed complaint.",
                    "topics": ["Customer Service"],
                    "pain_points": ["Slow support"],
                    "customer_intent": "Complaints",
                    "sentiment": "negative",
                    "sentiment_score": -0.5,
                },
                "raw_model_output": {},
                "analyzed_at": "2026-04-10T10:08:00+00:00",
            },
            {
                "activity_id": "activity-ad-1",
                "summary": "Credit card acquisition ad.",
                "marketing_intent": "Acquisition",
                "sentiment": "positive",
                "sentiment_score": 0.4,
                "analysis_payload": {
                    "summary": "Credit card acquisition ad.",
                    "marketing_intent": "Acquisition",
                    "topics": ["Credit Cards"],
                    "products": ["Credit Card"],
                    "value_propositions": ["Zero monthly fee"],
                    "urgency_indicators": [],
                    "sentiment": "positive",
                    "sentiment_score": 0.4,
                },
                "raw_model_output": {},
                "analyzed_at": "2026-04-11T11:03:00+00:00",
            },
        ]
        self.entities = [] if empty else [
            {
                "id": "entity-1",
                "name": "Example Bank",
                "industry": "Finance",
                "website": "https://example.am",
                "logo_url": None,
                "is_active": True,
            }
        ]
        self.accounts = [] if empty else [
            {"id": "account-fb-page", "entity_id": "entity-1", "platform": "facebook", "source_kind": "facebook_page"},
            {"id": "account-meta", "entity_id": "entity-1", "platform": "facebook", "source_kind": "meta_ads"},
        ]

    def _select_rows(self, table: str, *, filters=(), limit=None, **_kwargs):
        rows = {
            "social_activities": self.activities,
            "social_activity_analysis": self.analyses,
            "social_entities": self.entities,
            "social_entity_accounts": self.accounts,
        }[table]
        filtered = list(rows)
        for op, column, value in filters or ():
            if op == "eq":
                filtered = [row for row in filtered if row.get(column) == value]
            elif op == "in":
                filtered = [row for row in filtered if row.get(column) in value]
        return filtered[:limit] if limit is not None else filtered

    def get_topic_metric_enrichment(self, topic_names, **_kwargs) -> dict:
        return {
            topic: {
                "engagementTotal": 7,
                "likes": 4,
                "comments": 2,
                "shares": 1,
                "views": 0,
                "reactions": 4,
                "evidenceCount": 1,
                "sampleSummary": "Customers ask about card fees and support speed.",
                "evidence": [{"activity_uid": "facebook:post:post-1"}],
            }
            for topic in topic_names
        }

    def get_graph_sync_coverage(self, **_kwargs) -> dict:
        return {
            "totalParentActivities": 1,
            "analyzedParentActivities": 1,
            "graphSyncedParentActivities": 1,
            "graphPendingParentActivities": 0,
            "failedParentActivities": 0,
            "semanticCoveragePct": 100.0,
            "rowCap": 10000,
            "rowCapReached": False,
        }


class SocialDashboardSnapshotTests(unittest.TestCase):
    def test_empty_store_returns_valid_snapshot_with_diagnostics(self) -> None:
        payload = build_social_dashboard_snapshot(_FakeSocialDashboardStore(empty=True), use_cache=False)

        self.assertEqual(payload["meta"]["usedActivities"], 0)
        self.assertEqual(payload["deepAnalysis"]["topicBubbles"], [])
        self.assertIn("snapshot", payload["meta"]["emptyReasons"])
        self.assertIn("total", payload["meta"]["timingsMs"])

    def test_organic_discussion_maps_to_topics_sentiment_and_evidence(self) -> None:
        graph_payload = {
            "items": [
                {
                    "topic": "Card Fees",
                    "count": 1,
                    "previousCount": 0,
                    "deltaCount": 1,
                    "growthPct": 100.0,
                    "growthReliable": False,
                    "avgSentimentScore": -0.6,
                    "dominantSentiment": "negative",
                    "sentimentCounts": {"positive": 0, "neutral": 0, "negative": 1},
                    "topEntities": ["Example Bank"],
                    "topPlatforms": ["facebook"],
                    "activityUids": ["facebook:post:post-1"],
                }
            ],
            "meta": {"source": "neo4j"},
        }
        trend_payload = {"items": [{"bucket": "2026-04-06", "total": 1, "positive": 0, "neutral": 0, "negative": 1}]}
        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", return_value=graph_payload), \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", return_value=trend_payload):
            payload = build_social_dashboard_snapshot(
                _FakeSocialDashboardStore(),
                from_date="2026-04-01",
                to_date="2026-04-15",
                use_cache=False,
            )

        topic_names = {item["topic"] for item in payload["deepAnalysis"]["topicBubbles"]}
        self.assertIn("Card Fees", topic_names)
        self.assertEqual(payload["deepAnalysis"]["sentimentTrend"][0]["negative"], 1)
        self.assertEqual(payload["adIntelligence"]["items"][0]["source_kind"], "meta_ads")
        self.assertTrue(all(item["source_kind"] != "meta_ads" for item in payload["deepAnalysis"]["evidence"]))
        self.assertEqual(payload["meta"]["missingAnalysis"], 0)

    def test_ads_are_separate_from_organic_widgets(self) -> None:
        graph_payload = {"items": [{"topic": "Card Fees", "count": 1, "dominantSentiment": "negative"}]}
        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", return_value=graph_payload), \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", return_value={"items": []}):
            payload = build_social_dashboard_snapshot(_FakeSocialDashboardStore(), use_cache=False)

        self.assertEqual(len(payload["adIntelligence"]["items"]), 1)
        self.assertEqual(payload["adIntelligence"]["summary"]["topMarketingIntent"], "Acquisition")
        organic_topics = {item["topic"] for item in payload["deepAnalysis"]["topicBubbles"]}
        self.assertNotIn("Credit Cards", organic_topics)

    def test_social_dashboard_endpoint_uses_new_snapshot_path(self) -> None:
        startup_handlers = list(server.app.router.on_startup)
        shutdown_handlers = list(server.app.router.on_shutdown)
        server.app.router.on_startup = []
        server.app.router.on_shutdown = []
        try:
            client = TestClient(server.app)
            with patch.object(server.config, "IS_LOCKED_ENV", True), \
                 patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
                 patch.object(server, "get_social_store", return_value=_FakeSocialDashboardStore()), \
                 patch.object(social_dashboard.social_semantic, "get_topic_aggregates", return_value={"items": []}), \
                 patch.object(social_dashboard.social_semantic, "get_sentiment_trend", return_value={"items": []}):
                response = client.get(
                    "/api/social/dashboard?from=2026-04-01&to=2026-04-15",
                    headers={"Authorization": "Bearer admin-secret"},
                )
            client.close()
        finally:
            server.app.router.on_startup = startup_handlers
            server.app.router.on_shutdown = shutdown_handlers

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["meta"]["usedActivities"], 3)
        self.assertIn("deepAnalysis", payload)


if __name__ == "__main__":
    unittest.main()
