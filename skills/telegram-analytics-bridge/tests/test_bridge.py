from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from bridge import build_parser


class BridgeParserTests(unittest.TestCase):
    def test_parser_defaults_match_runtime_tuning(self) -> None:
        args = build_parser().parse_args(["search_entities", "--query", "permits"])

        self.assertEqual(args.timeout, 40.0)
        self.assertEqual(args.max_retries, 3)
        self.assertEqual(args.backoff_base, 0.75)

    def test_add_source_parser_accepts_optional_source_fields(self) -> None:
        args = build_parser().parse_args(
            ["add_source", "--value", "@docschat", "--source-type", "telegram", "--title", "Docs Chat"]
        )

        self.assertEqual(args.action, "add_source")
        self.assertEqual(args.value, "@docschat")
        self.assertEqual(args.source_type, "telegram")
        self.assertEqual(args.title, "Docs Chat")

    def test_deep_analyze_parser_accepts_mode(self) -> None:
        args = build_parser().parse_args(
            ["deep_analyze", "--question", "What changed?", "--window", "7d", "--mode", "deep"]
        )

        self.assertEqual(args.action, "deep_analyze")
        self.assertEqual(args.question, "What changed?")
        self.assertEqual(args.window, "7d")
        self.assertEqual(args.mode, "deep")


if __name__ == "__main__":
    unittest.main()
