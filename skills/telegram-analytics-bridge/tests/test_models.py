from __future__ import annotations

import unittest

from pydantic import ValidationError

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from models import AskInsightsRequest, GetQuestionClustersRequest, GetTopTopicsRequest


class ModelValidationTests(unittest.TestCase):
    def test_accepts_valid_window_and_limit(self) -> None:
        request = GetTopTopicsRequest(window="30d", limit=7)
        self.assertEqual(request.window, "30d")
        self.assertEqual(request.limit, 7)

    def test_rejects_invalid_window(self) -> None:
        with self.assertRaises(ValidationError):
            GetTopTopicsRequest(window="14d", limit=5)

    def test_normalizes_optional_topic(self) -> None:
        request = GetQuestionClustersRequest(window="7d", topic="  Residency permits  ")
        self.assertEqual(request.topic, "Residency permits")

    def test_question_is_trimmed(self) -> None:
        request = AskInsightsRequest(window="7d", question="  What is driving residency delays?   ")
        self.assertEqual(request.question, "What is driving residency delays?")


if __name__ == "__main__":
    unittest.main()
