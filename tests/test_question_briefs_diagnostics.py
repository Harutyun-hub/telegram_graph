from __future__ import annotations

import unittest
from unittest.mock import patch

from api import question_briefs


def _candidate_row() -> dict:
    return {
        "topic": "Visa Support",
        "category": "Legal",
        "evidence": [
            {
                "id": "ev-1",
                "kind": "comment",
                "text": "How do I renew my visa in Armenia?",
                "parentText": "People are comparing migration experiences.",
                "channel": "chan-a",
                "userId": "u1",
                "timestamp": "2026-03-18T10:00:00Z",
            },
            {
                "id": "ev-2",
                "kind": "comment",
                "text": "Where can I extend my visa in Yerevan?",
                "parentText": "The thread is asking about immigration offices.",
                "channel": "chan-b",
                "userId": "u2",
                "timestamp": "2026-03-19T10:00:00Z",
            },
        ],
    }


class _FakeRuntimeStore:
    def __init__(self) -> None:
        self.files: dict[str, dict] = {}
        self.counter = 0

    def save_runtime_json(self, path: str, payload: dict) -> bool:
        self.counter += 1
        self.files[path] = {
            "payload": payload,
            "updated_at": f"2026-03-19T12:00:{self.counter:02d}Z",
        }
        return True

    def get_runtime_json(self, path: str, default: dict | None = None) -> dict:
        row = self.files.get(path)
        if not row:
            return dict(default or {})
        return row["payload"]

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
        return {"status": "ok", "payload": row["payload"]}

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


class QuestionBriefDiagnosticsTests(unittest.TestCase):
    def setUp(self) -> None:
        question_briefs.invalidate_question_briefs_cache()

    def test_refresh_diagnostics_happy_path(self) -> None:
        synth_rows = [
            {
                "clusterId": "qc-visa-support",
                "canonicalQuestionEn": "How can people renew a visa in Armenia without conflicting instructions?",
                "canonicalQuestionRu": "Как людям продлить визу в Армении без противоречивых инструкций?",
                "summaryEn": "People repeatedly ask for a clear visa-renewal process.",
                "summaryRu": "Люди постоянно просят понятный процесс продления визы.",
                "confidence": "high",
                "confidenceScore": 0.86,
                "status": "needs_guide",
                "resolvedPct": 22,
                "evidenceIds": ["ev-1", "ev-2"],
            }
        ]

        def _fake_save_snapshot(
            cards: list[dict],
            metadata: dict | None = None,
            diagnostics: dict | None = None,
            ctx=None,
        ) -> bool:
            if isinstance(diagnostics, dict):
                diagnostics["snapshot"]["writeAttempted"] = True
                diagnostics["snapshot"]["writeSucceeded"] = True
                diagnostics["snapshot"]["readbackCards"] = len(cards)
            return True

        with patch.object(question_briefs.strategic, "get_question_brief_candidates", return_value=[_candidate_row()]), \
             patch.object(question_briefs, "_acquire_refresh_lease", return_value=True), \
             patch.object(question_briefs, "_load_state", return_value={"schemaVersion": 1, "clusters": {}}), \
             patch.object(question_briefs, "_save_state"), \
             patch.object(question_briefs, "_save_snapshot_cards", side_effect=_fake_save_snapshot), \
             patch.object(question_briefs, "_load_snapshot_cards", return_value=[{"id": "qc-qc-visa-support"}]), \
             patch.object(question_briefs, "_synthesize_cards", return_value=synth_rows), \
             patch.object(question_briefs.config, "QUESTION_BRIEFS_MIN_CLUSTER_MESSAGES", 2), \
             patch.object(question_briefs.config, "QUESTION_BRIEFS_MIN_CLUSTER_USERS", 2), \
             patch.object(question_briefs.config, "QUESTION_BRIEFS_MIN_CLUSTER_CHANNELS", 2):
            diagnostics = question_briefs.refresh_question_briefs_with_diagnostics(force=True)

        self.assertEqual(diagnostics["cardsProduced"], 1)
        self.assertEqual(diagnostics["stages"]["candidateRows"], 1)
        self.assertEqual(diagnostics["stages"]["signalCount"], 2)
        self.assertEqual(diagnostics["stages"]["clustersBeforeGate"], 1)
        self.assertEqual(diagnostics["stages"]["clustersAfterGate"], 1)
        self.assertEqual(diagnostics["stages"]["acceptedClusters"], 1)
        self.assertEqual(diagnostics["stages"]["finalCards"], 1)
        self.assertEqual(diagnostics["snapshot"]["writeSucceeded"], True)
        self.assertEqual(diagnostics["snapshot"]["readbackCards"], 1)
        self.assertEqual(diagnostics["exitReason"], "ok")

    def test_refresh_diagnostics_reports_support_gate_drop(self) -> None:
        weak = _candidate_row()
        weak["evidence"] = weak["evidence"][:1]

        with patch.object(question_briefs.strategic, "get_question_brief_candidates", return_value=[weak]), \
             patch.object(question_briefs, "_acquire_refresh_lease", return_value=True):
            diagnostics = question_briefs.refresh_question_briefs_with_diagnostics(force=True)

        self.assertEqual(diagnostics["cardsProduced"], 0)
        self.assertEqual(diagnostics["stages"]["candidateRows"], 1)
        self.assertEqual(diagnostics["stages"]["clustersBeforeGate"], 1)
        self.assertEqual(diagnostics["stages"]["clustersAfterGate"], 0)
        self.assertEqual(diagnostics["exitReason"], "no_clusters_after_support_gate")

    def test_materialization_diagnostics_reports_validation_drop(self) -> None:
        cluster = {
            "clusterId": "qc-visa-support",
            "topic": "Visa Support",
            "category": "Legal",
            "messages": 2,
            "uniqueUsers": 2,
            "channels": 2,
            "signals7d": 2,
            "signalsPrev7d": 0,
            "trend7dPct": 100,
            "latestAt": "2026-03-19T10:00:00Z",
            "signals": [
                {
                    "id": "ev-1",
                    "kind": "comment",
                    "channel": "chan-a",
                    "timestamp": "2026-03-18T10:00:00Z",
                    "message": "How do I renew my visa in Armenia?",
                },
                {
                    "id": "ev-2",
                    "kind": "comment",
                    "channel": "chan-b",
                    "timestamp": "2026-03-19T10:00:00Z",
                    "message": "Where can I extend my visa in Yerevan?",
                },
            ],
        }
        diagnostics = question_briefs._new_refresh_diagnostics(force=True)
        ai_rows = [
            {
                "clusterId": "qc-visa-support",
                "canonicalQuestionEn": "Visa renewal instructions",
                "canonicalQuestionRu": "Инструкции по продлению визы",
                "summaryEn": "Not a valid question card.",
                "summaryRu": "Это невалидная карточка вопроса.",
                "confidence": "high",
                "confidenceScore": 0.95,
                "evidenceIds": ["ev-1", "ev-2"],
            }
        ]

        cards = question_briefs._materialize_cards([cluster], ai_rows, diagnostics=diagnostics)

        self.assertEqual(cards, [])
        self.assertEqual(diagnostics["stages"]["cardsBeforeFilter"], 0)
        self.assertEqual(diagnostics["rejections"]["materialization"]["invalid_question_form"], 1)

    def test_snapshot_round_trip_uses_runtime_store(self) -> None:
        store = _FakeRuntimeStore()
        diagnostics = question_briefs._new_refresh_diagnostics(force=True)

        with patch.object(question_briefs, "_get_runtime_store", return_value=store):
            saved = question_briefs._save_snapshot_cards(
                [{"id": "qc-1", "topic": "Visa Support"}],
                metadata={"cards": 1},
                diagnostics=diagnostics,
            )
            loaded = question_briefs._load_snapshot_cards(diagnostics=diagnostics)

        self.assertTrue(saved)
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["id"], "qc-1")
        self.assertEqual(diagnostics["snapshot"]["writeSucceeded"], True)
        self.assertEqual(diagnostics["snapshot"]["loadedCards"], 1)

if __name__ == "__main__":
    unittest.main()
