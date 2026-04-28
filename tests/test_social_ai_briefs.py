from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from api import social_ai_briefs


class _FakeBriefStore:
    def __init__(self, *, activity_count: int = 60, snapshot: dict | None = None) -> None:
        self.saved: dict | None = None
        self.settings = {}
        if snapshot is not None:
            self.settings[social_ai_briefs.SNAPSHOT_SETTING_KEY] = snapshot
        self.activities = [
            {
                "id": f"activity-{index}",
                "entity_id": "entity-1",
                "account_id": "account-1",
                "activity_uid": f"facebook:post:{index}",
                "platform": "facebook",
                "source_kind": "post",
                "source_url": f"https://facebook.com/example/posts/{index}",
                "text_content": f"Question about public service quality number {index}?",
                "published_at": "2026-04-20T10:00:00+00:00",
                "author_handle": f"user-{index}",
                "engagement_metrics": {"likes": 1, "comments": 2},
                "assets": [],
                "analysis_status": "analyzed",
                "graph_status": "synced",
            }
            for index in range(activity_count)
        ]
        self.analyses = [
            {
                "activity_id": f"activity-{index}",
                "summary": f"People discuss service quality {index}.",
                "sentiment": "negative",
                "sentiment_score": -0.4,
                "analysis_payload": {
                    "summary": f"People discuss service quality {index}.",
                    "topics": ["Service Quality"],
                    "sentiment": "negative",
                    "sentiment_score": -0.4,
                    "customer_intent": "Questions",
                    "pain_points": ["Slow service"],
                },
                "raw_model_output": {},
                "analyzed_at": "2026-04-20T10:01:00+00:00",
            }
            for index in range(activity_count)
        ]
        self.entities = [{"id": "entity-1", "name": "Example Source", "industry": "Public"}]

    def get_runtime_setting(self, key: str, default: dict) -> dict:
        return self.settings.get(key, default)

    def save_runtime_setting(self, key: str, value: dict) -> dict:
        self.settings[key] = value
        self.saved = value
        return value

    def _select_rows(self, table: str, *, filters=(), limit=None, **_kwargs):
        rows = {
            "social_activities": self.activities,
            "social_activity_analysis": self.analyses,
            "social_entities": self.entities,
        }[table]
        filtered = list(rows)
        for op, column, value in filters or ():
            if op == "eq":
                filtered = [row for row in filtered if row.get(column) == value]
            elif op == "in":
                filtered = [row for row in filtered if row.get(column) in value]
            elif op == "gte":
                filtered = [row for row in filtered if row.get(column) >= value]
            elif op == "lte":
                filtered = [row for row in filtered if row.get(column) <= value]
        return filtered[:limit] if limit is not None else filtered


class SocialAiBriefTests(unittest.TestCase):
    def test_refresh_does_not_run_below_new_thread_threshold(self) -> None:
        store = _FakeBriefStore(activity_count=49)
        result = social_ai_briefs.should_refresh_social_ai_briefs(
            store,
            now=datetime(2026, 4, 25, tzinfo=timezone.utc),
        )

        self.assertFalse(result["eligible"])
        self.assertEqual(result["newProcessedParentThreads"], 49)

    def test_refresh_does_not_run_before_24_hours(self) -> None:
        snapshot = {
            "metadata": {
                "generatedAt": (datetime(2026, 4, 25, tzinfo=timezone.utc) - timedelta(hours=2)).isoformat(),
                "includedActivityUids": [],
            }
        }
        store = _FakeBriefStore(activity_count=60, snapshot=snapshot)
        result = social_ai_briefs.should_refresh_social_ai_briefs(
            store,
            now=datetime(2026, 4, 25, tzinfo=timezone.utc),
        )

        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "too_recent")

    def test_manual_refresh_bypasses_gate_and_publishes_valid_cards(self) -> None:
        store = _FakeBriefStore(activity_count=1)
        graph_payload = {
            "items": [
                {
                    "topic": "Service Quality",
                    "count": 1,
                    "dominantSentiment": "negative",
                    "sentimentCounts": {"negative": 1},
                    "activityUids": ["facebook:post:0"],
                    "growthPct": 0,
                }
            ]
        }
        ai_payload = {
            "intentCards": [
                {
                    "family": "Questions",
                    "title_en": "People ask about service quality",
                    "title_ru": "Люди спрашивают о качестве услуг",
                    "summary_en": "The audience is asking why service quality is slow.",
                    "summary_ru": "Аудитория спрашивает, почему качество услуг остается низким.",
                    "main_topic": "Service Quality",
                    "sentiment": "negative",
                    "signal_count": 1,
                    "trend_pct": 0,
                    "confidence": 0.8,
                    "evidence_ids": ["facebook:post:0"],
                    "evidence_quotes": ["Question about public service quality number 0?"],
                },
                {
                    "family": "Concern",
                    "title_en": "Unsupported weak card",
                    "title_ru": "Слабая карточка",
                    "summary_en": "Weak.",
                    "summary_ru": "Слабая.",
                    "confidence": 0.2,
                    "evidence_ids": ["facebook:post:0"],
                },
            ],
            "topSignals": [
                {
                    "family": "Questions",
                    "title_en": "Service quality questions",
                    "title_ru": "Вопросы о качестве услуг",
                    "summary_en": "People ask why public service quality remains slow.",
                    "summary_ru": "Люди спрашивают, почему качество услуг остается низким.",
                    "main_topic": "Service Quality",
                    "sentiment": "negative",
                    "signal_count": 1,
                    "trend_pct": 0,
                    "confidence": 0.82,
                    "evidence_ids": ["facebook:post:0"],
                    "evidence_quotes": ["Question about public service quality number 0?"],
                }
            ],
            "topQuestions": [],
        }

        with patch.object(social_ai_briefs.social_semantic, "get_topic_aggregates", return_value=graph_payload), \
             patch.object(social_ai_briefs, "_request_ai_synthesis", return_value=ai_payload):
            result = social_ai_briefs.refresh_social_ai_briefs(
                store,
                force=True,
                now=datetime(2026, 4, 25, tzinfo=timezone.utc),
            )

        self.assertEqual(result["status"], "refreshed")
        self.assertIsNotNone(store.saved)
        self.assertEqual(len(store.saved["intentCards"]), 1)
        self.assertEqual(store.saved["intentCards"][0]["family"], "Questions")
        self.assertEqual(len(store.saved["topSignals"]), 1)
        self.assertEqual(store.saved["topSignals"][0]["examples"], ["Question about public service quality number 0?"])
        self.assertEqual(store.settings[social_ai_briefs.SIGNAL_HISTORY_SETTING_KEY][0]["questions"], 1)
        self.assertEqual(store.saved["metadata"]["diagnostics"]["rejected"]["low_confidence"], 1)

    def test_validator_rejects_cards_without_real_evidence(self) -> None:
        result = social_ai_briefs.validate_social_ai_brief_output(
            {
                "intentCards": [
                    {
                        "family": "Support",
                        "title_en": "Support",
                        "title_ru": "Поддержка",
                        "summary_en": "Supported.",
                        "summary_ru": "Поддержано.",
                        "confidence": 0.9,
                        "evidence_ids": ["missing"],
                    }
                ]
            },
            evidence_by_uid={"facebook:post:1": {"quote": "real"}},
        )

        self.assertEqual(result["intentCards"], [])
        self.assertEqual(result["diagnostics"]["rejected"]["missing_evidence"], 1)

    def test_validator_rejects_top_signals_without_real_evidence(self) -> None:
        result = social_ai_briefs.validate_social_ai_brief_output(
            {
                "topSignals": [
                    {
                        "family": "Support",
                        "title_en": "Support signal",
                        "title_ru": "Сигнал поддержки",
                        "summary_en": "Supported.",
                        "summary_ru": "Поддержано.",
                        "confidence": 0.9,
                        "evidence_ids": ["missing"],
                    }
                ]
            },
            evidence_by_uid={"facebook:post:1": {"quote": "real quote"}},
        )

        self.assertEqual(result["topSignals"], [])

    def test_signal_history_is_bounded(self) -> None:
        store = _FakeBriefStore(activity_count=1)
        store.settings[social_ai_briefs.SIGNAL_HISTORY_SETTING_KEY] = [
            {"bucket": f"2026-03-{day:02d}", "support": day, "total": day}
            for day in range(1, 35)
        ]

        history = social_ai_briefs._append_signal_history(
            store,
            {
                "generatedAt": "2026-04-25T00:00:00+00:00",
                "topSignals": [{"family": "Support", "signal_count": 2}],
            },
        )

        self.assertEqual(len(history), social_ai_briefs.SIGNAL_HISTORY_LIMIT)
        self.assertEqual(history[-1]["bucket"], "2026-04-25")
        self.assertEqual(history[-1]["support"], 2)


if __name__ == "__main__":
    unittest.main()
