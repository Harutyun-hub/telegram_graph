from __future__ import annotations

import unittest
from unittest.mock import patch

from api import aggregator
from api import behavioral_briefs
from api import question_briefs
from api.ai_widget_storage import dedupe_cards, load_nearest_shorter_range_cards, select_portfolio_cards
from api.dashboard_dates import build_dashboard_date_context
from api import opportunity_briefs


class _FakeRuntimeStore:
    def __init__(self) -> None:
        self.files: dict[str, dict] = {}
        self.counter = 0

    def save_runtime_json(self, path: str, payload: dict) -> bool:
        self.counter += 1
        self.files[path] = {
            "payload": payload,
            "updated_at": f"2026-04-22T10:00:{self.counter:02d}Z",
        }
        return True

    def get_runtime_json(self, path: str, default: dict | None = None) -> dict:
        row = self.files.get(path)
        if not row:
            return dict(default or {})
        return dict(row["payload"])

    def read_runtime_json(
        self,
        path: str,
        *,
        prefer_signed_read: bool = False,
        timeout_seconds: float = 1.5,
    ) -> dict:
        row = self.files.get(path)
        if not row:
            return {"status": "missing"}
        return {"status": "ok", "payload": dict(row["payload"])}

    def list_runtime_files(self, folder: str) -> list[dict]:
        prefix = f"{folder}/"
        rows = []
        for path, row in self.files.items():
            if path.startswith(prefix):
                rows.append(
                    {
                        "name": path[len(prefix):],
                        "updated_at": row["updated_at"],
                    }
                )
        return rows

    def delete_runtime_files(self, paths: list[str]) -> int:
        for path in paths:
            self.files.pop(path, None)
        return len(paths)


class AiWidgetRangeSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        question_briefs.invalidate_question_briefs_cache()
        behavioral_briefs.invalidate_behavioral_briefs_cache()
        opportunity_briefs.invalidate_opportunity_briefs_cache()

    def test_question_briefs_use_exact_range_snapshot_before_global_fallback(self) -> None:
        store = _FakeRuntimeStore()
        ctx = build_dashboard_date_context("2026-04-01", "2026-04-15")
        range_key = f"{ctx.from_date.isoformat()}__{ctx.to_date.isoformat()}"

        store.save_runtime_json("question_cards/latest.json", {"cards": [{"id": "global-q"}]})

        with patch.object(question_briefs, "_get_runtime_store", return_value=store), \
             patch.object(question_briefs, "_ensure_range_refresh") as ensure_mock:
            self.assertEqual(question_briefs.get_question_briefs(ctx=ctx), [{"id": "global-q"}])
            ensure_mock.assert_called_once_with(ctx)

        store.save_runtime_json(
            f"question_cards/ranges/{range_key}/latest.json",
            {"cards": [{"id": "range-q"}]},
        )

        with patch.object(question_briefs, "_get_runtime_store", return_value=store), \
             patch.object(question_briefs, "_ensure_range_refresh") as ensure_mock:
            self.assertEqual(question_briefs.get_question_briefs(ctx=ctx), [{"id": "range-q"}])
            ensure_mock.assert_not_called()

    def test_question_briefs_use_exact_range_empty_snapshot_without_global_fallback(self) -> None:
        store = _FakeRuntimeStore()
        ctx = build_dashboard_date_context("2026-04-01", "2026-04-15")
        range_key = f"{ctx.from_date.isoformat()}__{ctx.to_date.isoformat()}"

        store.save_runtime_json("question_cards/latest.json", {"cards": [{"id": "global-q"}]})
        store.save_runtime_json(f"question_cards/ranges/{range_key}/latest.json", {"cards": []})

        with patch.object(question_briefs, "_get_runtime_store", return_value=store), \
             patch.object(question_briefs, "_ensure_range_refresh") as ensure_mock:
            self.assertEqual(question_briefs.get_question_briefs(ctx=ctx), [])
            ensure_mock.assert_not_called()

    def test_question_refresh_persists_empty_exact_range_snapshot(self) -> None:
        store = _FakeRuntimeStore()
        ctx = build_dashboard_date_context("2026-04-01", "2026-04-15")
        range_root = f"question_cards/ranges/{ctx.from_date.isoformat()}__{ctx.to_date.isoformat()}"

        with patch.object(question_briefs, "_get_runtime_store", return_value=store), \
             patch.object(question_briefs.strategic, "get_question_brief_candidates", return_value=[]):
            cards = question_briefs.refresh_question_briefs(ctx=ctx)

        self.assertEqual(cards, [])
        self.assertIn(f"{range_root}/latest.json", store.files)
        self.assertIn(f"{range_root}/state.json", store.files)
        self.assertTrue(any(path.startswith(f"{range_root}/snapshots/") for path in store.files))

    def test_question_support_gate_keeps_small_but_real_family(self) -> None:
        cluster = {"messages": 4, "uniqueUsers": 3, "channels": 1, "trend7dPct": 15}
        self.assertTrue(question_briefs._support_gate(cluster))

    def test_load_nearest_shorter_range_cards_prefers_largest_nested_window(self) -> None:
        store = _FakeRuntimeStore()
        long_ctx = build_dashboard_date_context("2026-01-16", "2026-04-15")
        ctx_15 = build_dashboard_date_context("2026-04-01", "2026-04-15")
        ctx_30 = build_dashboard_date_context("2026-03-17", "2026-04-15")

        store.save_runtime_json(
            f"question_cards/ranges/{ctx_15.from_date.isoformat()}__{ctx_15.to_date.isoformat()}/latest.json",
            {"cards": [{"id": "q-15", "topic": "Visa", "canonicalQuestionEn": "15d card?", "evidence": [{"id": "ev-15"}]}]},
        )
        store.save_runtime_json(
            f"question_cards/ranges/{ctx_30.from_date.isoformat()}__{ctx_30.to_date.isoformat()}/latest.json",
            {"cards": [{"id": "q-30", "topic": "Visa", "canonicalQuestionEn": "30d card?", "evidence": [{"id": "ev-30"}]}]},
        )

        cards = load_nearest_shorter_range_cards(
            store,
            family="question_cards",
            ctx=long_ctx,
            title_fields=["canonicalQuestionEn"],
            max_cards=8,
            topic_field="topic",
        )

        self.assertEqual([card["id"] for card in cards], ["q-30"])

    def test_behavioral_problem_refresh_kind_no_longer_emits_generic_fallback(self) -> None:
        cluster = {
            "clusterId": "pb-1",
            "topic": "Road And Transit",
            "category": "Housing & Infrastructure",
            "messages": 6,
            "uniqueUsers": 3,
            "channels": 2,
            "signals7d": 4,
            "signalsPrev7d": 2,
            "trend7dPct": 50,
            "latestAt": "2026-04-15T10:00:00Z",
            "severity": "high",
            "signals": [
                {
                    "id": "ev-1",
                    "kind": "comment",
                    "channel": "chan-a",
                    "userId": "u1",
                    "timestamp": "2026-04-14T10:00:00Z",
                    "message": "Road repairs are delayed again.",
                    "context": "People are angry in the thread.",
                    "label": "Negative",
                    "distressHit": 1,
                },
                {
                    "id": "ev-2",
                    "kind": "comment",
                    "channel": "chan-b",
                    "userId": "u2",
                    "timestamp": "2026-04-15T10:00:00Z",
                    "message": "Transport disruptions are getting worse.",
                    "context": "More complaints are arriving.",
                    "label": "Urgent",
                    "distressHit": 1,
                },
            ],
        }

        with patch("api.behavioral_briefs._synthesize_problem_cards", return_value=[]):
            cards, state, changed = behavioral_briefs._refresh_kind(
                kind="problem",
                clusters=[cluster],
                state_clusters={},
                force=True,
            )

        self.assertEqual(changed, 1)
        self.assertEqual(cards, [])
        self.assertEqual(state["pb-1"]["status"], "rejected")
        self.assertNotIn("card", state["pb-1"])

    def test_dedupe_cards_keeps_unique_ai_cards(self) -> None:
        cards = [
            {
                "id": "q1",
                "canonicalQuestionEn": "Is there a practical difference between a national visa and a Schengen visa when the only planned destination is Spain?",
                "evidence": [{"id": "ev-1"}, {"id": "ev-2"}],
                "confidenceScore": 0.9,
            },
            {
                "id": "q2",
                "canonicalQuestionEn": "Is there a practical difference between a national visa and a Schengen visa when the only planned destination is Spain?",
                "evidence": [{"id": "ev-2"}, {"id": "ev-3"}],
                "confidenceScore": 0.7,
            },
            {
                "id": "q3",
                "canonicalQuestionEn": "Why are political discussions becoming more hostile in Armenian government threads?",
                "evidence": [{"id": "ev-4"}, {"id": "ev-5"}],
                "confidenceScore": 0.8,
            },
        ]

        deduped = dedupe_cards(cards, title_fields=["canonicalQuestionEn"], max_cards=8)

        self.assertEqual([card["id"] for card in deduped], ["q1", "q3"])

    def test_select_portfolio_cards_prefers_topic_diversity_before_fill(self) -> None:
        cards = [
            {
                "id": "q1",
                "topic": "Visa And Residency",
                "canonicalQuestionEn": "How do I renew my visa in Armenia?",
                "evidence": [{"id": "ev-1"}, {"id": "ev-2"}],
            },
            {
                "id": "q2",
                "topic": "Visa And Residency",
                "canonicalQuestionEn": "Why are residency applications delayed so long this spring?",
                "evidence": [{"id": "ev-3"}, {"id": "ev-4"}],
            },
            {
                "id": "q3",
                "topic": "Government & Leadership",
                "canonicalQuestionEn": "Why are political discussions becoming more hostile in government threads?",
                "evidence": [{"id": "ev-5"}, {"id": "ev-6"}],
            },
        ]

        selected = select_portfolio_cards(cards, title_fields=["canonicalQuestionEn"], max_cards=2, topic_field="topic")

        self.assertEqual([card["id"] for card in selected], ["q1", "q3"])

    def test_opportunity_materialize_normalizes_delivery_fields(self) -> None:
        cluster = {
            "clusterId": "op-community-solidarity",
            "topic": "Community Solidarity",
            "category": "Community Life",
            "messages": 4,
            "uniqueUsers": 3,
            "channels": 2,
            "trend7dPct": 35,
            "latestAt": "2026-04-15T10:00:00Z",
            "signals": [
                {
                    "id": "ev-1",
                    "message": "We need a better way to coordinate urgent help requests across chats.",
                    "context": "People keep asking who can help and where to post.",
                    "channel": "chan-a",
                    "timestamp": "2026-04-15T09:00:00Z",
                    "kind": "message",
                },
                {
                    "id": "ev-2",
                    "message": "A volunteer coordination group or bot would save time for everyone.",
                    "context": "The same need came up in another channel.",
                    "channel": "chan-b",
                    "timestamp": "2026-04-14T09:00:00Z",
                    "kind": "message",
                },
            ],
        }
        ai_rows = [
            {
                "clusterId": "op-community-solidarity",
                "opportunityEn": "A community coordination service for urgent help requests",
                "opportunityRu": "Сервис координации срочных запросов о помощи",
                "summaryEn": "Repeated volunteer coordination asks suggest a real need.",
                "summaryRu": "Повторяющиеся запросы на координацию помощи указывают на реальную потребность.",
                "deliveryModel": "community program",
                "readiness": "validate now",
                "confidence": "medium",
                "confidenceScore": 0.62,
                "evidenceIds": ["ev-1", "ev-2"],
            }
        ]

        cards = opportunity_briefs._materialize_cards([cluster], ai_rows, diagnostics=opportunity_briefs._new_refresh_diagnostics(force=True))

        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["deliveryModel"], "community_program")
        self.assertEqual(cards[0]["readiness"], "validate_now")

    def test_opportunity_briefs_use_exact_range_empty_snapshot_without_global_fallback(self) -> None:
        store = _FakeRuntimeStore()
        ctx = build_dashboard_date_context("2026-04-01", "2026-04-15")
        range_key = f"{ctx.from_date.isoformat()}__{ctx.to_date.isoformat()}"

        store.save_runtime_json("opportunity_cards/latest.json", {"cards": [{"id": "global-op"}]})
        store.save_runtime_json(f"opportunity_cards/ranges/{range_key}/latest.json", {"cards": []})

        with patch.object(opportunity_briefs, "_get_runtime_store", return_value=store), \
             patch.object(opportunity_briefs, "_ensure_range_refresh") as ensure_mock:
            self.assertEqual(opportunity_briefs.get_business_opportunity_briefs(ctx=ctx), [])
            ensure_mock.assert_not_called()

    def test_opportunity_refresh_persists_empty_exact_range_snapshot(self) -> None:
        store = _FakeRuntimeStore()
        ctx = build_dashboard_date_context("2026-04-01", "2026-04-15")
        range_root = f"opportunity_cards/ranges/{ctx.from_date.isoformat()}__{ctx.to_date.isoformat()}"

        with patch.object(opportunity_briefs, "_get_runtime_store", return_value=store), \
             patch.object(opportunity_briefs, "_client", object()), \
             patch.object(opportunity_briefs.actionable, "get_business_opportunity_brief_candidates", return_value=[]):
            cards = opportunity_briefs.refresh_opportunity_briefs(ctx=ctx)

        self.assertEqual(cards, [])
        self.assertIn(f"{range_root}/latest.json", store.files)
        self.assertIn(f"{range_root}/state.json", store.files)
        self.assertTrue(any(path.startswith(f"{range_root}/snapshots/") for path in store.files))

    def test_opportunity_support_gate_accepts_small_multi_channel_family(self) -> None:
        self.assertTrue(opportunity_briefs._opportunity_support_gate(2, 2, 2, 0))

    def test_aggregator_passes_dashboard_range_into_ai_widget_loaders(self) -> None:
        ctx = build_dashboard_date_context("2026-04-01", "2026-04-15")

        with patch.object(aggregator.question_briefs, "get_question_briefs", return_value=[] ) as question_mock, \
             patch.object(aggregator.behavioral_briefs, "get_behavioral_briefs", return_value={"problemBriefs": [], "serviceGapBriefs": [], "urgencyBriefs": []}) as behavioral_mock, \
             patch.object(aggregator.opportunity_briefs, "get_business_opportunity_briefs", return_value=[]) as opportunity_mock, \
             patch.object(aggregator.strategic, "get_topic_bubbles", return_value=[]), \
             patch.object(aggregator.strategic, "get_trend_lines", return_value=[]), \
             patch.object(aggregator.strategic, "get_heatmap", return_value=[]), \
             patch.object(aggregator.strategic, "get_question_categories", return_value=[]), \
             patch.object(aggregator.strategic, "get_lifecycle_stages", return_value=[]), \
             patch.object(aggregator.behavioral, "get_problems", return_value=[]), \
             patch.object(aggregator.behavioral, "get_service_gaps", return_value=[]), \
             patch.object(aggregator.behavioral, "get_satisfaction_areas", return_value=[]), \
             patch.object(aggregator.behavioral, "get_mood_data", return_value=[]), \
             patch.object(aggregator.actionable, "get_business_opportunities", return_value=[]), \
             patch.object(aggregator.actionable, "get_job_seeking", return_value=[]), \
             patch.object(aggregator.actionable, "get_job_trends", return_value=[]), \
             patch.object(aggregator.actionable, "get_housing_data", return_value=[]):
            aggregator._tier_strategic(ctx)
            aggregator._tier_behavioral(ctx)
            aggregator._tier_actionable(ctx)

        question_mock.assert_called_once_with(ctx=ctx)
        behavioral_mock.assert_called_once_with(ctx=ctx)
        opportunity_mock.assert_called_once_with(ctx=ctx)


if __name__ == "__main__":
    unittest.main()
