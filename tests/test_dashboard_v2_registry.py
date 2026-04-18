from __future__ import annotations

import unittest
from unittest.mock import patch

import config
from api.dashboard_v2_registry import (
    DIRECT_SOURCE_TRUTH_WIDGET_IDS,
    EXACT_FACT_BACKED_WIDGET_IDS,
    SECONDARY_MATERIALIZED_WIDGET_IDS,
    build_widget_coverage_report,
    validate_widget_coverage,
)


class DashboardV2RegistryTests(unittest.TestCase):
    def test_registry_covers_all_admin_widgets(self) -> None:
        with patch.object(config, "TELEGRAM_API_ID", 1), \
             patch.object(config, "TELEGRAM_API_HASH", "hash"), \
             patch.object(config, "SUPABASE_URL", "https://example.supabase.co"), \
             patch.object(config, "SUPABASE_SERVICE_ROLE_KEY", "key"), \
             patch.object(config, "NEO4J_URI", "neo4j+s://example.databases.neo4j.io"), \
             patch.object(config, "NEO4J_PASSWORD", "password"), \
             patch.object(config, "OPENAI_API_KEY", "sk-test"):
            from api import server  # Imported lazily to avoid env validation at test module load time.

        missing, unexpected = validate_widget_coverage(server.ADMIN_WIDGET_IDS)
        self.assertEqual(missing, [])
        self.assertEqual(unexpected, [])

    def test_secondary_widgets_do_not_overlap_exact_fact_backed_set(self) -> None:
        overlap = set(EXACT_FACT_BACKED_WIDGET_IDS) & set(SECONDARY_MATERIALIZED_WIDGET_IDS)
        self.assertEqual(overlap, set())

    def test_direct_truth_widgets_are_registered(self) -> None:
        coverage_report = {row["widget_id"] for row in build_widget_coverage_report()}
        self.assertTrue(set(DIRECT_SOURCE_TRUTH_WIDGET_IDS).issubset(coverage_report))


if __name__ == "__main__":
    unittest.main()
