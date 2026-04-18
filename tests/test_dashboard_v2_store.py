from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

from api.dashboard_v2_store import DashboardV2Store, compute_max_fact_watermark, compute_stale_fact_families, same_key_last_known_good_allowed


class _CoverageOnlyStore(DashboardV2Store):
    def __init__(self, rows_by_family: dict[str, list[dict]]) -> None:
        self.writer = None
        self._rows_by_family = rows_by_family

    def _coverage_rows_for_family(
        self,
        *,
        fact_family: str,
        from_date: date,
        to_date: date,
        min_fact_version: int,
    ) -> list[dict]:
        del from_date, to_date, min_fact_version
        return list(self._rows_by_family.get(fact_family, []))


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

    def test_get_range_readiness_reports_missing_dates_and_families(self) -> None:
        store = _CoverageOnlyStore(
            {
                "content": [
                    {
                        "fact_date": date(2026, 4, 17),
                        "fact_version": 2,
                        "materialized_at": "2026-04-18T10:00:00+00:00",
                        "source_watermark": "2026-04-18T10:00:00+00:00",
                        "payload_json": {"coverageReady": True},
                    }
                ],
                "topics": [
                    {
                        "fact_date": date(2026, 4, 17),
                        "fact_version": 1,
                        "materialized_at": "2026-04-18T09:00:00+00:00",
                        "source_watermark": "2026-04-18T09:00:00+00:00",
                        "payload_json": {"coverageReady": True},
                    }
                ],
            }
        )

        readiness = store.get_range_readiness(
            from_date=date(2026, 4, 17),
            to_date=date(2026, 4, 18),
            fact_families=("content", "topics"),
            min_fact_version=2,
        )

        self.assertFalse(readiness["ready"])
        self.assertEqual(readiness["missingFactFamilies"], ["content", "topics"])
        self.assertEqual(readiness["missingDates"], ["2026-04-17", "2026-04-18"])
        self.assertEqual(readiness["factFamilies"]["topics"]["latestFactVersion"], 1)

    def test_summarize_route_readiness_requires_continuous_lookback_window(self) -> None:
        store = _CoverageOnlyStore(
            {
                "content": [
                    {
                        "fact_date": date(2026, 4, 16),
                        "fact_version": 2,
                        "materialized_at": "2026-04-18T10:00:00+00:00",
                        "source_watermark": "2026-04-18T10:00:00+00:00",
                        "payload_json": {"coverageReady": True},
                    },
                    {
                        "fact_date": date(2026, 4, 18),
                        "fact_version": 2,
                        "materialized_at": "2026-04-18T10:00:00+00:00",
                        "source_watermark": "2026-04-18T10:00:00+00:00",
                        "payload_json": {"coverageReady": True},
                    },
                ],
                "topics": [
                    {
                        "fact_date": date(2026, 4, 16),
                        "fact_version": 2,
                        "materialized_at": "2026-04-18T10:00:00+00:00",
                        "source_watermark": "2026-04-18T10:00:00+00:00",
                        "payload_json": {"coverageReady": True},
                    }
                ],
            }
        )

        readiness = store.summarize_v2_route_readiness(
            min_fact_version=2,
            lookback_days=3,
            end_date=date(2026, 4, 18),
        )

        self.assertFalse(readiness["v2RouteReady"])
        self.assertEqual(readiness["routeReadyWindowStart"], "2026-04-18")
        self.assertEqual(readiness["routeReadyWindowEnd"], "2026-04-16")

    def test_summarize_route_readiness_uses_exact_window_when_from_to_are_provided(self) -> None:
        store = _CoverageOnlyStore(
            {
                "content": [
                    {
                        "fact_date": date(2026, 4, 17),
                        "fact_version": 2,
                        "materialized_at": "2026-04-18T10:00:00+00:00",
                        "source_watermark": "2026-04-18T10:00:00+00:00",
                        "payload_json": {"coverageReady": True},
                    },
                    {
                        "fact_date": date(2026, 4, 18),
                        "fact_version": 2,
                        "materialized_at": "2026-04-18T10:00:00+00:00",
                        "source_watermark": "2026-04-18T10:00:00+00:00",
                        "payload_json": {"coverageReady": True},
                    },
                ],
                "topics": [
                    {
                        "fact_date": date(2026, 4, 17),
                        "fact_version": 2,
                        "materialized_at": "2026-04-18T10:00:00+00:00",
                        "source_watermark": "2026-04-18T10:00:00+00:00",
                        "payload_json": {"coverageReady": True},
                    },
                    {
                        "fact_date": date(2026, 4, 18),
                        "fact_version": 2,
                        "materialized_at": "2026-04-18T10:00:00+00:00",
                        "source_watermark": "2026-04-18T10:00:00+00:00",
                        "payload_json": {"coverageReady": True},
                    },
                ],
            }
        )

        with patch("api.dashboard_v2_store.FULL_DASHBOARD_REQUIRED_FACT_FAMILIES", ("content", "topics")):
            readiness = store.summarize_v2_route_readiness(
                min_fact_version=2,
                from_date=date(2026, 4, 17),
                to_date=date(2026, 4, 18),
            )

        self.assertTrue(readiness["v2RouteReady"])
        self.assertEqual(readiness["routeReadyWindowStart"], "2026-04-17")
        self.assertEqual(readiness["routeReadyWindowEnd"], "2026-04-18")
        self.assertEqual(readiness["requestedFrom"], "2026-04-17")
        self.assertEqual(readiness["requestedTo"], "2026-04-18")

    def test_get_range_readiness_reports_degraded_coverage(self) -> None:
        store = _CoverageOnlyStore(
            {
                "content": [
                    {
                        "fact_date": date(2026, 4, 18),
                        "fact_version": 2,
                        "materialized_at": "2026-04-18T10:00:00+00:00",
                        "source_watermark": "2026-04-18T10:00:00+00:00",
                        "payload_json": {
                            "coverageReady": False,
                            "coverageDegraded": True,
                            "failedWidgets": ["community_brief"],
                        },
                    }
                ]
            }
        )

        readiness = store.get_range_readiness(
            from_date=date(2026, 4, 18),
            to_date=date(2026, 4, 18),
            fact_families=("content",),
            min_fact_version=2,
        )

        self.assertFalse(readiness["ready"])
        self.assertEqual(readiness["degradedFactFamilies"], ["content"])
        self.assertEqual(readiness["degradedDates"], ["2026-04-18"])
        self.assertEqual(readiness["factFamilies"]["content"]["failedWidgets"], ["community_brief"])


if __name__ == "__main__":
    unittest.main()
