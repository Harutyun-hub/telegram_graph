from __future__ import annotations

import unittest
from unittest.mock import patch

from api import behavioral_briefs, opportunity_briefs, question_briefs


class _FakeRuntimeStore:
    def __init__(self) -> None:
        self.files: dict[str, dict] = {}
        self.counter = 0

    def save_runtime_json(self, path: str, payload: dict) -> bool:
        self.counter += 1
        self.files[path] = {
            "payload": payload,
            "updated_at": f"2026-04-22T01:10:{self.counter:02d}Z",
        }
        return True

    def get_runtime_json(self, path: str, default: dict | None = None) -> dict:
        row = self.files.get(path)
        if not row:
            return dict(default or {})
        return row["payload"]

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


class AIWidgetSnapshotGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        question_briefs.invalidate_question_briefs_cache()
        behavioral_briefs.invalidate_behavioral_briefs_cache()
        opportunity_briefs.invalidate_opportunity_briefs_cache()

    def test_question_snapshot_guard_rejects_weaker_same_clusters(self) -> None:
        store = _FakeRuntimeStore()
        with patch.object(question_briefs, "_get_runtime_store", return_value=store):
            self.assertTrue(
                question_briefs._save_snapshot_cards(
                    [{"id": "q1"}, {"id": "q2"}, {"id": "q3"}, {"id": "q4"}],
                    metadata={"activeClusters": 14, "cards": 4},
                )
            )
            diagnostics = question_briefs._new_refresh_diagnostics(force=True)
            self.assertTrue(
                question_briefs._save_snapshot_cards(
                    [{"id": "q1"}, {"id": "q2"}],
                    metadata={"activeClusters": 14, "cards": 2},
                    diagnostics=diagnostics,
                )
            )
            loaded = question_briefs._load_snapshot_cards()

        self.assertEqual(len(loaded), 4)
        self.assertEqual(diagnostics["snapshot"]["publishSkipped"], True)
        self.assertEqual(diagnostics["snapshot"]["publishSkipReason"], "fewer_cards_same_active_clusters")

    def test_question_snapshot_guard_allows_richer_same_clusters(self) -> None:
        store = _FakeRuntimeStore()
        with patch.object(question_briefs, "_get_runtime_store", return_value=store):
            self.assertTrue(
                question_briefs._save_snapshot_cards(
                    [{"id": "q1"}, {"id": "q2"}],
                    metadata={"activeClusters": 14, "cards": 2},
                )
            )
            self.assertTrue(
                question_briefs._save_snapshot_cards(
                    [{"id": "q1"}, {"id": "q2"}, {"id": "q3"}, {"id": "q4"}],
                    metadata={"activeClusters": 14, "cards": 4},
                )
            )
            loaded = question_briefs._load_snapshot_cards()

        self.assertEqual(len(loaded), 4)

    def test_behavioral_snapshot_guard_rejects_weaker_problem_cards(self) -> None:
        store = _FakeRuntimeStore()
        with patch.object(behavioral_briefs, "_get_runtime_store", return_value=store):
            self.assertTrue(
                behavioral_briefs._save_snapshot_payload(
                    {
                        "problemBriefs": [{"id": f"p{i}"} for i in range(8)],
                        "serviceGapBriefs": [],
                        "urgencyBriefs": [{"id": "u1"}],
                    },
                    metadata={"activeProblemClusters": 16, "problemCards": 8},
                )
            )
            diagnostics = {"snapshot": {}}
            self.assertTrue(
                behavioral_briefs._save_snapshot_payload(
                    {
                        "problemBriefs": [{"id": f"p{i}"} for i in range(6)],
                        "serviceGapBriefs": [],
                        "urgencyBriefs": [{"id": "u1"}],
                    },
                    metadata={"activeProblemClusters": 16, "problemCards": 6},
                    diagnostics=diagnostics,
                )
            )
            loaded = behavioral_briefs._load_snapshot_payload()

        self.assertEqual(len(loaded["problemBriefs"]), 8)
        self.assertEqual(diagnostics["snapshot"]["publishSkipped"], True)
        self.assertEqual(diagnostics["snapshot"]["publishSkipReason"], "fewer_problem_cards_same_active_clusters")

    def test_opportunity_snapshot_guard_rejects_zero_over_non_empty(self) -> None:
        store = _FakeRuntimeStore()
        with patch.object(opportunity_briefs, "_get_runtime_store", return_value=store):
            self.assertTrue(
                opportunity_briefs._save_snapshot_cards(
                    [{"id": "o1"}],
                    metadata={"activeClusters": 16, "cards": 1},
                )
            )
            diagnostics = opportunity_briefs._new_refresh_diagnostics(force=True)
            self.assertTrue(
                opportunity_briefs._save_snapshot_cards(
                    [],
                    metadata={"activeClusters": 16, "cards": 0},
                    diagnostics=diagnostics,
                )
            )
            loaded = opportunity_briefs._load_snapshot_cards()

        self.assertEqual(len(loaded), 1)
        self.assertEqual(diagnostics["snapshot"]["publishSkipped"], True)
        self.assertEqual(diagnostics["snapshot"]["publishSkipReason"], "zero_cards_over_non_empty")


if __name__ == "__main__":
    unittest.main()
