from __future__ import annotations

import unittest

from api.dashboard_v2_keys import build_dashboard_v2_coverage_row_key, build_dashboard_v2_row_key


class DashboardV2KeyTests(unittest.TestCase):
    def test_row_keys_are_deterministic_and_sanitized(self) -> None:
        first = build_dashboard_v2_row_key("topic_evidence", topic="Road And Transit", source="Post=42", content="Po|st")
        second = build_dashboard_v2_row_key("topic_evidence", content="Po|st", source="Post=42", topic="Road And Transit")
        self.assertEqual(first, second)
        self.assertEqual(first, "kind=topic_evidence|content=po_st|source=post_42|topic=road and transit")

    def test_coverage_row_key_is_stable(self) -> None:
        self.assertEqual(build_dashboard_v2_coverage_row_key(), "kind=coverage_marker|scope=all")


if __name__ == "__main__":
    unittest.main()
