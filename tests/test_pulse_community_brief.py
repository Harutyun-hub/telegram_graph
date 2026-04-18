from __future__ import annotations

from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from api.dashboard_dates import build_dashboard_date_context
from api.queries import pulse
from buffer.supabase_writer import SupabaseWriter


class _FakeCommunityBriefWriter:
    def __init__(self) -> None:
        self.post_ids_by_window: dict[tuple[str, str], list[str]] = {}
        self.comment_scopes_by_window: dict[tuple[str, str], list[dict]] = {}
        self.post_analyses: dict[str, dict] = {}
        self.scope_analyses: dict[tuple[str, str, str], dict] = {}

    def get_post_ids_in_exact_window(self, start_iso: str, end_iso: str) -> list[str]:
        return list(self.post_ids_by_window.get((start_iso, end_iso), []))

    def get_comment_scopes_in_exact_window(self, start_iso: str, end_iso: str) -> list[dict]:
        return [dict(item) for item in self.comment_scopes_by_window.get((start_iso, end_iso), [])]

    def get_latest_post_analyses_for_post_ids(self, post_ids: list[str]) -> dict[str, dict]:
        return {
            post_id: dict(self.post_analyses[post_id])
            for post_id in post_ids
            if post_id in self.post_analyses
        }

    def get_latest_batch_analyses_for_scopes(self, scopes: list[dict]) -> dict[tuple[str, str, str], dict]:
        resolved: dict[tuple[str, str, str], dict] = {}
        for scope in scopes:
            key = (
                str(scope.get("postId") or ""),
                str(scope.get("channelId") or ""),
                str(scope.get("telegramUserId") or ""),
            )
            analysis = self.scope_analyses.get(key)
            if analysis is not None:
                resolved[key] = dict(analysis)
        return resolved


class _FakeSupabaseTable:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self._filters: list[tuple[str, str, object]] = []

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, key: str, value):
        self._filters.append(("eq", key, value))
        return self

    def in_(self, key: str, values):
        self._filters.append(("in", key, list(values)))
        return self

    def is_(self, key: str, value):
        self._filters.append(("is", key, value))
        return self

    def execute(self):
        rows = list(self._rows)
        for op, key, value in self._filters:
            if op == "eq":
                rows = [row for row in rows if row.get(key) == value]
            elif op == "in":
                allowed = set(value)
                rows = [row for row in rows if row.get(key) in allowed]
            elif op == "is":
                rows = [row for row in rows if (row.get(key) is None) == (str(value).lower() == "null")]
        return type("Response", (), {"data": rows})()


class _FakeSupabaseClient:
    def __init__(self, datasets: dict[str, list[dict]]) -> None:
        self._datasets = datasets

    def table(self, name: str):
        return _FakeSupabaseTable(list(self._datasets.get(name, [])))


class CommunityBriefWindowTests(unittest.TestCase):
    def test_exact_window_counts_are_monotonic_for_nested_ranges(self) -> None:
        writer = _FakeCommunityBriefWriter()
        one_day = build_dashboard_date_context("2026-04-15", "2026-04-15")
        three_day = build_dashboard_date_context("2026-04-13", "2026-04-15")

        scope_one = {"postId": "post-1", "channelId": "channel-1", "telegramUserId": "101"}
        scope_two = {"postId": "post-2", "channelId": "channel-1", "telegramUserId": "202"}

        writer.post_ids_by_window[(one_day.start_at.isoformat(), one_day.end_at.isoformat())] = ["post-1"]
        writer.post_ids_by_window[(three_day.start_at.isoformat(), three_day.end_at.isoformat())] = ["post-1", "post-2"]
        writer.comment_scopes_by_window[(one_day.start_at.isoformat(), one_day.end_at.isoformat())] = [scope_one]
        writer.comment_scopes_by_window[(three_day.start_at.isoformat(), three_day.end_at.isoformat())] = [scope_one, scope_two]
        writer.post_analyses = {
            "post-1": {"content_id": "post-1", "primary_intent": "support", "sentiment_score": 0.8, "created_at": "2026-04-16T00:05:00+00:00"},
            "post-2": {"content_id": "post-2", "primary_intent": "question", "sentiment_score": 0.1, "created_at": "2026-04-16T00:06:00+00:00"},
        }
        writer.scope_analyses = {
            ("post-1", "channel-1", "101"): {"content_id": "post-1", "channel_id": "channel-1", "telegram_user_id": 101, "primary_intent": "critique", "sentiment_score": -0.6, "created_at": "2026-04-16T00:07:00+00:00"},
            ("post-2", "channel-1", "202"): {"content_id": "post-2", "channel_id": "channel-1", "telegram_user_id": 202, "primary_intent": "support", "sentiment_score": 0.4, "created_at": "2026-04-16T00:08:00+00:00"},
        }

        with patch.object(pulse, "_supabase", return_value=writer):
            brief_one = pulse._community_brief_from_source_scope(one_day, top_topic_rows=[])
            brief_three = pulse._community_brief_from_source_scope(three_day, top_topic_rows=[])

        self.assertEqual(brief_one["postsAnalyzedInWindow"], 1)
        self.assertEqual(brief_one["commentScopesAnalyzedInWindow"], 1)
        self.assertEqual(brief_three["postsAnalyzedInWindow"], 2)
        self.assertEqual(brief_three["commentScopesAnalyzedInWindow"], 2)
        self.assertGreaterEqual(brief_three["postsAnalyzedInWindow"], brief_one["postsAnalyzedInWindow"])
        self.assertGreaterEqual(
            brief_three["commentScopesAnalyzedInWindow"],
            brief_one["commentScopesAnalyzedInWindow"],
        )
        self.assertEqual(brief_three["totalAnalysesInWindow"], 4)
        self.assertEqual(brief_three["totalAnalyses24h"], 4)
        self.assertEqual(brief_three["postsAnalyzed24h"], brief_three["postsAnalyzedInWindow"])
        self.assertEqual(
            brief_three["commentScopesAnalyzed24h"],
            brief_three["commentScopesAnalyzedInWindow"],
        )

    def test_source_window_scope_counts_even_if_analysis_created_at_is_outside_range(self) -> None:
        writer = _FakeCommunityBriefWriter()
        ctx = build_dashboard_date_context("2026-04-15", "2026-04-15")
        writer.post_ids_by_window[(ctx.start_at.isoformat(), ctx.end_at.isoformat())] = ["post-1"]
        writer.comment_scopes_by_window[(ctx.start_at.isoformat(), ctx.end_at.isoformat())] = [
            {"postId": "post-1", "channelId": "channel-1", "telegramUserId": "101"}
        ]
        writer.post_analyses = {
            "post-1": {
                "content_id": "post-1",
                "primary_intent": "support",
                "sentiment_score": 0.9,
                "created_at": "2026-04-01T00:00:00+00:00",
            }
        }
        writer.scope_analyses = {
            ("post-1", "channel-1", "101"): {
                "content_id": "post-1",
                "channel_id": "channel-1",
                "telegram_user_id": 101,
                "primary_intent": "support",
                "sentiment_score": 0.6,
                "created_at": "2026-04-02T00:00:00+00:00",
            }
        }

        with patch.object(pulse, "_supabase", return_value=writer):
            brief = pulse._community_brief_from_source_scope(ctx, top_topic_rows=[])

        self.assertEqual(brief["postsAnalyzedInWindow"], 1)
        self.assertEqual(brief["commentScopesAnalyzedInWindow"], 1)
        self.assertEqual(brief["totalAnalysesInWindow"], 2)
        self.assertEqual(brief["uniqueUsersInWindow"], 1)


class BatchScopeResolutionTests(unittest.TestCase):
    def _writer(self, ai_analysis_rows: list[dict]) -> SupabaseWriter:
        writer = SupabaseWriter.__new__(SupabaseWriter)
        writer.client = _FakeSupabaseClient({"ai_analysis": ai_analysis_rows})
        return writer

    def test_scoped_batch_analysis_is_preferred_over_channel_level_fallback(self) -> None:
        writer = self._writer(
            [
                {
                    "id": "fallback",
                    "content_type": "batch",
                    "content_id": None,
                    "channel_id": "channel-1",
                    "telegram_user_id": 101,
                    "primary_intent": "critique",
                    "sentiment_score": -0.8,
                    "created_at": "2026-04-15T12:00:00+00:00",
                },
                {
                    "id": "scoped",
                    "content_type": "batch",
                    "content_id": "post-1",
                    "channel_id": "channel-1",
                    "telegram_user_id": 101,
                    "primary_intent": "support",
                    "sentiment_score": 0.8,
                    "created_at": "2026-04-15T12:05:00+00:00",
                },
            ]
        )

        resolved = writer.get_latest_batch_analyses_for_scopes(
            [{"postId": "post-1", "channelId": "channel-1", "telegramUserId": "101"}]
        )

        selected = resolved[("post-1", "channel-1", "101")]
        self.assertEqual(selected["id"], "scoped")


if __name__ == "__main__":
    unittest.main()
