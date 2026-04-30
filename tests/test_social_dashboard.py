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
            {"id": "account-fb-page", "entity_id": "entity-1", "platform": "facebook", "source_kind": "facebook_page", "is_active": True},
            {"id": "account-meta", "entity_id": "entity-1", "platform": "facebook", "source_kind": "meta_ads", "is_active": True},
        ]
        self.runtime_settings = {
            "ai_brief_snapshot": {
                "status": "ready",
                "generatedAt": "2026-04-12T00:00:00+00:00",
                "intentCards": [
                    {
                        "family": "Questions",
                        "intent": "Questions",
                        "title_en": "Customers ask about card fees",
                        "title_ru": "Клиенты спрашивают о комиссиях по картам",
                        "summary_en": "People are asking why card fees remain high.",
                        "summary_ru": "Люди спрашивают, почему комиссии по картам остаются высокими.",
                        "main_topic": "Card Fees",
                        "sentiment": "negative",
                        "signal_count": 1,
                        "count": 1,
                        "trend_pct": 0,
                        "delta": 0,
                        "confidence": 0.82,
                        "evidence_ids": ["facebook:post:post-1"],
                        "evidence_quotes": ["Why are card fees so high?"],
                        "examples": ["Why are card fees so high?"],
                    }
                ],
                "topSignals": [
                    {
                        "family": "Questions",
                        "title_en": "Card fee questions",
                        "title_ru": "Вопросы о комиссиях по картам",
                        "summary_en": "People ask why card fees remain high.",
                        "summary_ru": "Люди спрашивают, почему комиссии по картам остаются высокими.",
                        "main_topic": "Card Fees",
                        "sentiment": "negative",
                        "signal_count": 1,
                        "count": 1,
                        "confidence": 0.82,
                        "evidence_ids": ["facebook:post:post-1"],
                        "evidence_quotes": ["Why are card fees so high?"],
                        "examples": ["Why are card fees so high?"],
                    }
                ],
                "topQuestions": [],
                "topProblems": [
                    {
                        "problem": "Customers complain about slow support",
                        "problem_en": "Customers complain about slow support",
                        "problem_ru": "Клиенты жалуются на медленную поддержку",
                        "summary_en": "Evidence-backed complaints focus on slow response times.",
                        "summary_ru": "Жалобы с доказательствами связаны с медленными ответами.",
                        "topic": "Customer Service",
                        "sentiment": "negative",
                        "count": 1,
                        "confidence": 0.86,
                        "sources": ["Example Bank"],
                        "evidence_quotes": ["Support is too slow on weekends."],
                        "evidence_count": 1,
                        "evidence_ids": ["facebook:post:post-1"],
                    }
                ],
                "metadata": {
                    "window": {"from": "2026-04-01T00:00:00+00:00", "to": "2026-04-15T00:00:00+00:00"},
                    "promptVersion": "social-ai-briefs-v1",
                },
            },
            "ai_brief_signal_history": [
                {"bucket": "2026-04-12", "questions": 1, "total": 1}
            ],
        }

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
            elif op == "gte":
                filtered = [row for row in filtered if row.get(column) and row.get(column) >= value]
            elif op == "lte":
                filtered = [row for row in filtered if row.get(column) and row.get(column) <= value]
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

    def get_runtime_setting(self, key: str, default: dict) -> dict:
        return self.runtime_settings.get(key, default)


class SocialDashboardSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        social_dashboard._CACHE.clear()
        social_dashboard._REFRESHING.clear()
        social_dashboard.social_semantic.invalidate_social_semantic_cache()

    def test_empty_store_returns_valid_snapshot_with_diagnostics(self) -> None:
        payload = build_social_dashboard_snapshot(_FakeSocialDashboardStore(empty=True), use_cache=False)

        self.assertEqual(payload["meta"]["usedActivities"], 0)
        self.assertEqual(payload["deepAnalysis"]["topicBubbles"], [])
        self.assertEqual(payload["deepAnalysis"]["communityInterests"], [])
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
        self.assertEqual(payload["deepAnalysis"]["communityInterests"][0]["interest"], "Economy & Jobs")
        self.assertEqual(payload["deepAnalysis"]["communityInterests"][0]["score"], 100.0)
        self.assertEqual(payload["deepAnalysis"]["sentimentTrend"][0]["negative"], 1)
        self.assertEqual(payload["deepAnalysis"]["topSignals"][0]["family"], "Questions")
        self.assertEqual(payload["deepAnalysis"]["signalTrend"][0]["questions"], 1)
        self.assertEqual(payload["deepAnalysis"]["painPoints"][0]["problem_en"], "Customers complain about slow support")
        self.assertEqual(payload["meta"]["dataSources"]["deepAnalysis.communityInterests"], "neo4j_topic_aggregates")
        self.assertEqual(payload["meta"]["dataSources"]["deepAnalysis.painPoints"], "social_ai_brief_snapshot")
        self.assertEqual(payload["strictMetrics"]["sentimentByEntity"][0]["entity_id"], "entity-1")
        self.assertEqual(payload["strictMetrics"]["summary"]["trackedSources"], 2)
        self.assertEqual(payload["strictMetrics"]["summary"]["posts"], 1)
        self.assertEqual(payload["strictMetrics"]["summary"]["comments"], 1)
        self.assertEqual(payload["strictMetrics"]["summary"]["ads"], 1)
        self.assertEqual(payload["strictMetrics"]["visibilityData"][0]["reach"], 0)
        self.assertEqual(payload["strictMetrics"]["visibilityData"][0]["interactions"], 10)
        self.assertIsNone(payload["strictMetrics"]["visibilityData"][0]["engagementRate"])
        self.assertEqual(payload["adIntelligence"]["items"][0]["source_kind"], "meta_ads")
        self.assertTrue(all(item["source_kind"] != "meta_ads" for item in payload["deepAnalysis"]["evidence"]))
        self.assertEqual(payload["meta"]["missingAnalysis"], 0)

    def test_community_interests_use_specific_discussion_focus_areas(self) -> None:
        interests = social_dashboard._community_interests([
            {"topic": "Political Support", "count": 4},
            {"topic": "Church Evidence Debate", "count": 2},
            {"topic": "Community Education Event", "count": 2},
            {"topic": "Concert Programming", "count": 1},
            {"topic": "Charitable Foundation", "count": 1},
        ])

        by_interest = {item["interest"]: item for item in interests}
        self.assertNotIn("Community Life", by_interest)
        self.assertEqual(by_interest["Government & Leadership"]["score"], 40.0)
        self.assertEqual(by_interest["Church & Identity Debate"]["mentions"], 2)
        self.assertEqual(by_interest["Education & Community Programs"]["topTopics"], ["Community Education Event"])
        self.assertEqual(by_interest["Culture & Public Events"]["topTopics"], ["Concert Programming"])
        self.assertEqual(by_interest["Charity & Social Support"]["topTopics"], ["Charitable Foundation"])
        self.assertAlmostEqual(sum(item["score"] for item in interests), 100.0)

    def test_dashboard_filters_incomplete_ai_problem_cards(self) -> None:
        store = _FakeSocialDashboardStore()
        store.runtime_settings["ai_brief_snapshot"]["topProblems"] = [
            {
                "problem": "Legacy generated problem",
                "topic": "Audience Issue",
                "count": 1,
            },
            {
                "problem": "Evidence-backed issue",
                "problem_en": "Evidence-backed issue",
                "confidence": 0.75,
                "evidence_count": 1,
                "evidence_ids": ["facebook:post:post-1"],
                "evidence_quotes": ["This is a real issue."],
            },
        ]

        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", return_value={"items": []}), \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", return_value={"items": []}):
            payload = build_social_dashboard_snapshot(
                store,
                from_date="2026-04-01",
                to_date="2026-04-15",
                use_cache=False,
            )

        self.assertEqual(len(payload["deepAnalysis"]["painPoints"]), 1)
        self.assertEqual(payload["deepAnalysis"]["painPoints"][0]["problem"], "Evidence-backed issue")
        self.assertEqual(payload["meta"]["socialAiBriefs"]["topProblems"], 1)

    def test_ads_are_separate_from_organic_widgets(self) -> None:
        graph_payload = {"items": [{"topic": "Card Fees", "count": 1, "dominantSentiment": "negative"}]}
        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", return_value=graph_payload), \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", return_value={"items": []}):
            payload = build_social_dashboard_snapshot(_FakeSocialDashboardStore(), use_cache=False)

        self.assertEqual(len(payload["adIntelligence"]["items"]), 1)
        self.assertEqual(payload["adIntelligence"]["summary"]["topMarketingIntent"], "Acquisition")
        organic_topics = {item["topic"] for item in payload["deepAnalysis"]["topicBubbles"]}
        self.assertNotIn("Credit Cards", organic_topics)

    def test_strict_metrics_use_supabase_facts_without_mixing_comments_as_posts(self) -> None:
        rows = [
            {
                "id": "post-a",
                "entity_id": "entity-a",
                "account_id": "account-a",
                "source_kind": "post",
                "platform": "facebook",
                "published_at": "2026-04-10T10:00:00+00:00",
                "text_content": "A public post about jobs.",
                "source_url": "https://www.facebook.com/example/posts/1",
                "assets": [{"type": "image", "url": "https://cdn.example/post.jpg"}],
                "engagement_metrics": {"like_count": 10, "comment_count": 3, "share_count": 2, "view_count": 100},
                "entity": {"id": "entity-a", "name": "Source A"},
                "account": {"id": "account-a"},
                "analysis": {"sentiment": "positive", "sentiment_score": 0.7, "analysis_payload": {"topics": ["Jobs"]}},
            },
            {
                "id": "comment-a",
                "entity_id": "entity-a",
                "account_id": "account-a",
                "source_kind": "comment",
                "platform": "facebook",
                "published_at": "2026-04-10T10:05:00+00:00",
                "engagement_metrics": {"reactionCount": 5},
                "entity": {"id": "entity-a", "name": "Source A"},
                "account": {"id": "account-a"},
                "analysis": None,
            },
            {
                "id": "video-a",
                "entity_id": "entity-a",
                "account_id": "account-a",
                "source_kind": "video",
                "platform": "facebook",
                "published_at": "2026-04-10T11:00:00+00:00",
                "text_content": "A short organic video update.",
                "engagement_metrics": {"like_count": 1},
                "entity": {"id": "entity-a", "name": "Source A"},
                "account": {"id": "account-a"},
                "analysis": {"sentiment": "neutral", "sentiment_score": 0, "analysis_payload": {"topics": ["Updates"]}},
            },
            {
                "id": "ad-a",
                "entity_id": "entity-a",
                "account_id": "account-ad",
                "source_kind": "ad",
                "platform": "facebook",
                "published_at": "2026-04-11T10:00:00+00:00",
                "engagement_metrics": {"impression_count": 500, "click_count": 20},
                "entity": {"id": "entity-a", "name": "Source A"},
                "account": {"id": "account-ad"},
                "analysis": None,
            },
        ]

        metrics = social_dashboard._strict_metrics(rows, [], source_rows=[{"id": "account-a"}, {"id": "account-ad"}])
        visibility = metrics["visibilityData"][0]

        self.assertEqual(metrics["summary"]["trackedSources"], 2)
        self.assertEqual(metrics["summary"]["posts"], 2)
        self.assertEqual(metrics["summary"]["comments"], 1)
        self.assertEqual(metrics["summary"]["ads"], 1)
        self.assertEqual(visibility["reach"], 100)
        self.assertEqual(visibility["interactions"], 21)
        self.assertEqual(visibility["engagementRate"], 21.0)
        self.assertEqual(metrics["scorecard"][0]["posts"], 2)
        self.assertEqual(metrics["scorecard"][0]["comments"], 1)
        self.assertEqual(metrics["visibilityTrend"][0]["source_a"], 100.0)
        organic_posts = metrics["organicPosts"]["items"]
        self.assertEqual([item["source_kind"] for item in organic_posts], ["video", "post"])
        post_item = next(item for item in organic_posts if item["source_kind"] == "post")
        self.assertEqual(post_item["text"], "A public post about jobs.")
        self.assertEqual(post_item["source_url"], "https://www.facebook.com/example/posts/1")
        self.assertEqual(post_item["reach"], 100)
        self.assertEqual(post_item["likes"], 10)
        self.assertEqual(post_item["comments"], 3)
        self.assertEqual(post_item["shares"], 2)
        self.assertEqual(post_item["media"]["url"], "https://cdn.example/post.jpg")
        video_item = next(item for item in organic_posts if item["source_kind"] == "video")
        self.assertIsNone(video_item["reach"])
        self.assertEqual(metrics["organicPosts"]["summary"]["total"], 2)

    def test_social_dashboard_cache_miss_returns_warming(self) -> None:
        with patch.object(social_dashboard, "_schedule_refresh", return_value=True) as schedule:
            with self.assertRaises(social_dashboard.SocialDashboardWarmingError):
                build_social_dashboard_snapshot(
                    _FakeSocialDashboardStore(),
                    from_date="2026-04-01",
                    to_date="2026-04-15",
                )
        schedule.assert_called_once()

    def test_compare_entity_changes_cache_key(self) -> None:
        base = {
            "from": "2026-04-01",
            "to": "2026-04-15",
            "entity_id": "entity-1",
            "compare_entity_id": None,
            "platform": None,
            "source_kind": None,
        }
        compared = {**base, "compare_entity_id": "entity-2"}

        self.assertNotEqual(social_dashboard._cache_key(base), social_dashboard._cache_key(compared))

    def test_compare_entity_fetches_both_entities_and_passes_semantic_filter(self) -> None:
        store = _FakeSocialDashboardStore()
        store.activities.append(
            {
                "id": "activity-post-2",
                "entity_id": "entity-2",
                "account_id": "account-fb-page-2",
                "activity_uid": "facebook:post:post-2",
                "platform": "facebook",
                "source_kind": "post",
                "provider_item_id": "post-2",
                "source_url": "https://facebook.com/other/posts/2",
                "text_content": "Supporters praise the campaign message.",
                "published_at": "2026-04-10T12:00:00+00:00",
                "author_handle": "customer-3",
                "cta_type": None,
                "content_format": "Text",
                "region_name": "Armenia",
                "engagement_metrics": {"like_count": 2},
                "assets": [],
                "provider_payload": {},
                "ingest_status": "normalized",
                "analysis_status": "analyzed",
                "graph_status": "synced",
                "last_seen_at": "2026-04-10T12:03:00+00:00",
                "created_at": "2026-04-10T12:03:00+00:00",
            }
        )
        store.analyses.append(
            {
                "activity_id": "activity-post-2",
                "summary": "Supporters praise campaign message.",
                "marketing_intent": None,
                "sentiment": "positive",
                "sentiment_score": 0.7,
                "analysis_payload": {
                    "summary": "Supporters praise campaign message.",
                    "topics": ["Campaign Messaging"],
                    "sentiment": "positive",
                    "sentiment_score": 0.7,
                },
                "raw_model_output": {},
                "analyzed_at": "2026-04-10T12:07:00+00:00",
            }
        )
        store.entities.append(
            {
                "id": "entity-2",
                "name": "Other Entity",
                "industry": "Public",
                "website": "https://other.example",
                "logo_url": None,
                "is_active": True,
            }
        )
        store.accounts.append(
            {"id": "account-fb-page-2", "entity_id": "entity-2", "platform": "facebook", "source_kind": "facebook_page"}
        )

        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", return_value={"items": []}) as topics, \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", return_value={"items": []}) as trend:
            payload = build_social_dashboard_snapshot(
                store,
                from_date="2026-04-01",
                to_date="2026-04-15",
                entity_id="entity-1",
                compare_entity_id="entity-2",
                use_cache=False,
            )

        self.assertEqual(payload["meta"]["usedActivities"], 4)
        self.assertEqual(payload["meta"]["graphSyncCoverage"]["totalParentActivities"], 2)
        topics.assert_called_once()
        trend.assert_called_once()
        self.assertEqual(topics.call_args.kwargs["entity_ids"], ["entity-1", "entity-2"])
        self.assertEqual(trend.call_args.kwargs["entity_ids"], ["entity-1", "entity-2"])

    def test_selected_entity_semantic_queries_start_from_tracked_entity(self) -> None:
        with patch.object(social_dashboard.social_semantic, "_query_rows", return_value=[]) as query_rows:
            social_dashboard.social_semantic.get_topic_aggregates(
                from_date="2026-04-01",
                to_date="2026-04-15",
                entity_ids=["entity-1", "entity-2"],
            )

        topic_cypher = query_rows.call_args.args[0]
        topic_params = query_rows.call_args.args[1]
        self.assertIn("MATCH (entity:TrackedEntity)-[:HAS_ACTIVITY]->(a:SocialActivity)-[:COVERS]->(t:Topic)", topic_cypher)
        self.assertNotIn("EXISTS { MATCH (matchedEntity:TrackedEntity)-[:HAS_ACTIVITY]->(a)", topic_cypher)
        self.assertEqual(topic_params["entity_ids"], ["entity-1", "entity-2"])

        social_dashboard.social_semantic.invalidate_social_semantic_cache()
        with patch.object(social_dashboard.social_semantic, "_query_rows", return_value=[]) as query_rows:
            social_dashboard.social_semantic.get_sentiment_trend(
                from_date="2026-04-01",
                to_date="2026-04-15",
                entity_ids=["entity-1", "entity-2"],
            )

        sentiment_cypher = query_rows.call_args.args[0]
        sentiment_params = query_rows.call_args.args[1]
        self.assertIn("MATCH (entity:TrackedEntity)-[:HAS_ACTIVITY]->(a:SocialActivity)-[:HAS_SENTIMENT]->(s:Sentiment)", sentiment_cypher)
        self.assertNotIn("EXISTS { MATCH (matchedEntity:TrackedEntity)-[:HAS_ACTIVITY]->(a)", sentiment_cypher)
        self.assertEqual(sentiment_params["entity_ids"], ["entity-1", "entity-2"])

    def test_social_dashboard_stale_cache_returns_without_rebuild(self) -> None:
        filters = {
            "from": "2026-04-01",
            "to": "2026-04-15",
            "entity_id": None,
            "compare_entity_id": None,
            "platform": None,
            "source_kind": None,
        }
        key = social_dashboard._cache_key(filters)
        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", return_value={"items": []}), \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", return_value={"items": []}):
            snapshot = build_social_dashboard_snapshot(
                _FakeSocialDashboardStore(),
                from_date="2026-04-01",
                to_date="2026-04-15",
                use_cache=False,
            )
        social_dashboard._CACHE[key] = (0.0, snapshot)

        with patch.object(social_dashboard, "_schedule_refresh", return_value=True) as schedule:
            payload = build_social_dashboard_snapshot(
                _FakeSocialDashboardStore(empty=True),
                from_date="2026-04-01",
                to_date="2026-04-15",
            )

        schedule.assert_called_once()
        self.assertEqual(payload["meta"]["cache"]["status"], "stale")
        self.assertGreater(payload["meta"]["usedActivities"], 0)

    def test_social_dashboard_expired_stale_cache_is_marked(self) -> None:
        filters = {
            "from": "2026-04-01",
            "to": "2026-04-15",
            "entity_id": None,
            "compare_entity_id": None,
            "platform": None,
            "source_kind": None,
        }
        key = social_dashboard._cache_key(filters)
        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", return_value={"items": []}), \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", return_value={"items": []}):
            snapshot = build_social_dashboard_snapshot(
                _FakeSocialDashboardStore(),
                from_date="2026-04-01",
                to_date="2026-04-15",
                use_cache=False,
            )
        social_dashboard._CACHE[key] = (
            social_dashboard.time.time() - social_dashboard.SNAPSHOT_STALE_SECONDS - 10,
            snapshot,
        )

        with patch.object(social_dashboard, "_schedule_refresh", return_value=True):
            payload = build_social_dashboard_snapshot(
                _FakeSocialDashboardStore(empty=True),
                from_date="2026-04-01",
                to_date="2026-04-15",
            )

        self.assertTrue(payload["meta"]["cache"]["expired"])
        self.assertIn("expiredStaleCache", payload["meta"]["degradedSections"])

    def test_semantic_failure_preserves_previous_good_topic_and_sentiment_widgets(self) -> None:
        good_topics = {
            "items": [
                {
                    "topic": "Card Fees",
                    "count": 3,
                    "previousCount": 1,
                    "deltaCount": 2,
                    "growthPct": 200.0,
                    "growthReliable": True,
                    "avgSentimentScore": -0.5,
                    "dominantSentiment": "negative",
                    "sentimentCounts": {"positive": 0, "neutral": 1, "negative": 2},
                    "topEntities": ["Example Bank"],
                    "topPlatforms": ["facebook"],
                    "activityUids": ["facebook:post:post-1"],
                }
            ]
        }
        good_trend = {"items": [{"bucket": "2026-04-10", "total": 3, "positive": 0, "neutral": 1, "negative": 2}]}
        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", return_value=good_topics), \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", return_value=good_trend):
            previous = build_social_dashboard_snapshot(
                _FakeSocialDashboardStore(),
                from_date="2026-04-01",
                to_date="2026-04-15",
                use_cache=False,
            )

        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", side_effect=RuntimeError("neo4j timeout")), \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", side_effect=RuntimeError("neo4j timeout")):
            broken = build_social_dashboard_snapshot(
                _FakeSocialDashboardStore(),
                from_date="2026-04-01",
                to_date="2026-04-15",
                use_cache=False,
            )

        self.assertEqual(broken["deepAnalysis"]["topicBubbles"], [])
        self.assertEqual(broken["deepAnalysis"]["sentimentTrend"], [])

        preserved = social_dashboard._preserve_semantic_sections_from_cache(broken, previous)

        self.assertEqual(preserved["deepAnalysis"]["topicBubbles"], previous["deepAnalysis"]["topicBubbles"])
        self.assertEqual(preserved["deepAnalysis"]["topicRanking"], previous["deepAnalysis"]["topicRanking"])
        self.assertEqual(preserved["deepAnalysis"]["sentimentTrend"], previous["deepAnalysis"]["sentimentTrend"])
        self.assertTrue(preserved["meta"]["semanticStale"])
        self.assertTrue(preserved["meta"]["semanticPreservedFromCache"])
        self.assertIn("semanticRefreshFailed", preserved["meta"]["degradedSections"])
        self.assertNotIn("semanticTopics", preserved["meta"]["degradedSections"])
        self.assertNotIn("semanticSentimentTrend", preserved["meta"]["degradedSections"])

    def test_semantic_failure_without_previous_snapshot_stays_empty_and_degraded(self) -> None:
        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", side_effect=RuntimeError("neo4j timeout")), \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", side_effect=RuntimeError("neo4j timeout")):
            broken = build_social_dashboard_snapshot(
                _FakeSocialDashboardStore(),
                from_date="2026-04-01",
                to_date="2026-04-15",
                use_cache=False,
            )

        preserved = social_dashboard._preserve_semantic_sections_from_cache(broken, None)

        self.assertEqual(preserved["deepAnalysis"]["topicBubbles"], [])
        self.assertEqual(preserved["deepAnalysis"]["sentimentTrend"], [])
        self.assertIn("semanticTopics", preserved["meta"]["degradedSections"])
        self.assertIn("semanticSentimentTrend", preserved["meta"]["degradedSections"])
        self.assertNotIn("semanticRefreshFailed", preserved["meta"]["degradedSections"])

    def test_graph_coverage_failure_degrades_only_coverage_metadata(self) -> None:
        store = _FakeSocialDashboardStore()

        def _fail_coverage(**_kwargs):
            raise RuntimeError("coverage unavailable")

        store.get_graph_sync_coverage = _fail_coverage
        with patch.object(social_dashboard.social_semantic, "get_topic_aggregates", return_value={"items": []}), \
             patch.object(social_dashboard.social_semantic, "get_sentiment_trend", return_value={"items": []}):
            payload = build_social_dashboard_snapshot(
                store,
                from_date="2026-04-01",
                to_date="2026-04-15",
                use_cache=False,
            )

        self.assertIn("graphSyncCoverage", payload["meta"]["degradedSections"])
        self.assertTrue(payload["meta"]["graphSyncCoverage"]["unavailable"])
        self.assertIn("topicBubbles", payload["deepAnalysis"])

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
                filters = {
                    "from": "2026-04-01",
                    "to": "2026-04-15",
                    "entity_id": None,
                    "compare_entity_id": None,
                    "platform": None,
                    "source_kind": None,
                }
                key = social_dashboard._cache_key(filters)
                social_dashboard._CACHE[key] = (
                    social_dashboard.time.time(),
                    build_social_dashboard_snapshot(
                        _FakeSocialDashboardStore(),
                        from_date="2026-04-01",
                        to_date="2026-04-15",
                        use_cache=False,
                    ),
                )
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

    def test_social_dashboard_endpoint_returns_warming_on_cache_miss(self) -> None:
        startup_handlers = list(server.app.router.on_startup)
        shutdown_handlers = list(server.app.router.on_shutdown)
        server.app.router.on_startup = []
        server.app.router.on_shutdown = []
        try:
            client = TestClient(server.app)
            with patch.object(server.config, "IS_LOCKED_ENV", True), \
                 patch.object(server.config, "ADMIN_API_KEY", "admin-secret"), \
                 patch.object(server, "get_social_store", return_value=_FakeSocialDashboardStore()), \
                 patch.object(social_dashboard, "_schedule_refresh", return_value=True):
                response = client.get(
                    "/api/social/dashboard?from=2026-04-01&to=2026-04-15",
                    headers={"Authorization": "Bearer admin-secret"},
                )
            client.close()
        finally:
            server.app.router.on_startup = startup_handlers
            server.app.router.on_shutdown = shutdown_handlers

        self.assertEqual(response.status_code, 503)
        self.assertIn("warming", response.text.lower())


if __name__ == "__main__":
    unittest.main()
