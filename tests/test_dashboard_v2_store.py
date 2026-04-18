from __future__ import annotations

import unittest
from datetime import date, datetime, timezone

from api.dashboard_v2_store import (
    compute_max_fact_watermark,
    compute_stale_fact_families,
    same_key_last_known_good_allowed,
)


class DashboardV2StoreHelperTests(unittest.TestCase):
    def test_compute_max_fact_watermark_returns_latest_datetime(self) -> None:
        latest = compute_max_fact_watermark(
            {
                "content": "2026-04-18T10:00:00+00:00",
                "topics": "2026-04-18T12:30:00+00:00",
                "users": datetime(2026, 4, 18, 11, 45, tzinfo=timezone.utc),
            }
        )
        self.assertEqual(latest, datetime(2026, 4, 18, 12, 30, tzinfo=timezone.utc))

    def test_compute_stale_fact_families_detects_newer_dependencies(self) -> None:
        stale = compute_stale_fact_families(
            {
                "content": "2026-04-18T10:00:00+00:00",
                "topics": "2026-04-18T11:00:00+00:00",
            },
            {
                "content": "2026-04-18T10:00:00+00:00",
                "topics": "2026-04-18T12:00:00+00:00",
                "users": "2026-04-18T09:00:00+00:00",
            },
        )
        self.assertEqual(stale, ["topics", "users"])

    def test_same_key_last_known_good_requires_exact_range_match(self) -> None:
        self.assertTrue(
            same_key_last_known_good_allowed(
                request_from=date(2026, 4, 1),
                request_to=date(2026, 4, 7),
                artifact_from=date(2026, 4, 1),
                artifact_to=date(2026, 4, 7),
                artifact_is_stale=True,
                newer_exact_exists=False,
            )
        )
        self.assertFalse(
            same_key_last_known_good_allowed(
                request_from=date(2026, 4, 1),
                request_to=date(2026, 4, 7),
                artifact_from=date(2026, 4, 2),
                artifact_to=date(2026, 4, 7),
                artifact_is_stale=True,
                newer_exact_exists=False,
            )
        )
        self.assertFalse(
            same_key_last_known_good_allowed(
                request_from=date(2026, 4, 1),
                request_to=date(2026, 4, 7),
                artifact_from=date(2026, 4, 1),
                artifact_to=date(2026, 4, 7),
                artifact_is_stale=False,
                newer_exact_exists=False,
            )
        )


if __name__ == "__main__":
    unittest.main()
