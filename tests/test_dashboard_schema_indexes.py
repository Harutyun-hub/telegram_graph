from __future__ import annotations

import unittest

from ingester.neo4j_writer import SCHEMA_CONSTRAINTS


class DashboardSchemaIndexTests(unittest.TestCase):
    def test_dashboard_query_indexes_are_registered(self) -> None:
        combined = "\n".join(SCHEMA_CONSTRAINTS)
        self.assertIn("post_posted_at_idx", combined)
        self.assertIn("comment_posted_at_idx", combined)


if __name__ == "__main__":
    unittest.main()
