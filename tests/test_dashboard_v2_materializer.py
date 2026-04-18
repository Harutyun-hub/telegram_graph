from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from api.dashboard_dates import build_dashboard_date_context
from api.dashboard_v2_materializer import (
    DashboardV2FactRow,
    plan_dashboard_v2_materialize_slices,
    _materialize_family_rows,
    materialize_dashboard_v2_foundation,
    run_dashboard_v2_materialize_slice_once,
)


class _FakeStore:
    def __init__(self) -> None:
        self.created_runs: list[tuple[str, str, str]] = []
        self.replaced_rows: list[tuple[str, date, int]] = []
        self.completed_runs: list[tuple[str, str, dict]] = []
        self.marked_stale: list[tuple[str, date]] = []

    def create_fact_run(self, *, fact_family, fact_version, coverage_start, coverage_end, meta_json=None):
        run_id = f"{fact_family}-run"
        self.created_runs.append((fact_family, coverage_start.isoformat(), coverage_end.isoformat()))
        return run_id

    def replace_daily_fact_rows(self, *, fact_family, fact_date, run_id, fact_version, rows, source_watermark=None):
        self.replaced_rows.append((fact_family, fact_date, len(rows)))
        return len(rows)

    def mark_overlapping_artifacts_stale(self, *, fact_family, changed_date, new_watermark=None, reason=None):
        self.marked_stale.append((fact_family, changed_date))
        return 1

    def complete_fact_run(self, run_id, *, status, source_watermark=None, error=None, meta_json=None):
        self.completed_runs.append((run_id, status, meta_json or {}))


class _SliceQueueStore:
    def __init__(self) -> None:
        self.claimed = False
        self.heartbeats: list[tuple[str, str, str, int]] = []
        self.completed: list[tuple[str, str, str, int, int, tuple[str, ...], tuple[str, ...], str | None]] = []
        self.failed: list[tuple[str, str, str, str]] = []

    def claim_next_materialize_slice(self, *, worker_id, lease_seconds):
        if self.claimed:
            return None
        self.claimed = True
        return {
            "job": {
                "jobId": "job-1",
                "mode": "backfill",
            },
            "slice": {
                "sliceId": "slice-1",
                "factFamily": "content",
                "sliceStart": "2026-04-16",
                "sliceEnd": "2026-04-18",
            },
        }

    def heartbeat_materialize_slice(self, *, job_id, slice_id, worker_id, lease_seconds):
        self.heartbeats.append((job_id, slice_id, worker_id, lease_seconds))

    def complete_materialize_slice(
        self,
        *,
        job_id,
        slice_id,
        worker_id,
        rows_inserted,
        days_processed,
        degraded_days,
        failed_widgets,
        fact_run_id,
    ):
        self.completed.append(
            (
                job_id,
                slice_id,
                worker_id,
                rows_inserted,
                days_processed,
                tuple(degraded_days or ()),
                tuple(failed_widgets or ()),
                fact_run_id,
            )
        )
        return {"jobId": job_id, "status": "running"}

    def fail_materialize_slice(self, *, job_id, slice_id, worker_id, error, **kwargs):
        self.failed.append((job_id, slice_id, worker_id, error))
        return {"jobId": job_id, "status": "failed"}


class DashboardV2MaterializerTests(unittest.TestCase):
    def test_slice_planner_uses_content_3_day_slices_and_others_7_day_slices(self) -> None:
        with patch("api.dashboard_v2_materializer.FACT_FAMILIES", ("content", "topics")):
            plans = plan_dashboard_v2_materialize_slices(
                mode="backfill",
                requested_start=date(2026, 4, 1),
                requested_end=date(2026, 4, 8),
            )

        self.assertEqual(
            [(plan.fact_family, plan.slice_start.isoformat(), plan.slice_end.isoformat()) for plan in plans],
            [
                ("content", "2026-04-01", "2026-04-03"),
                ("content", "2026-04-04", "2026-04-06"),
                ("content", "2026-04-07", "2026-04-08"),
                ("topics", "2026-04-01", "2026-04-07"),
                ("topics", "2026-04-08", "2026-04-08"),
            ],
        )

    def test_multi_family_widget_materializes_into_each_declared_family(self) -> None:
        ctx = build_dashboard_date_context("2026-04-18", "2026-04-18")
        with patch(
            "api.dashboard_v2_materializer._WIDGET_FACT_BUILDERS",
            {"community_brief": lambda _ctx: {"messagesAnalyzed": 10}},
        ):
            content_rows = _materialize_family_rows("content", ctx)
            topic_rows = _materialize_family_rows("topics", ctx)

        content_keys = [row.row_key for row in content_rows]
        topic_keys = [row.row_key for row in topic_rows]
        self.assertIn("kind=day_summary|scope=all", content_keys)
        self.assertIn("kind=topic_day|topic=_summary", topic_keys)
        self.assertIn("kind=coverage_marker|scope=all", content_keys)
        self.assertIn("kind=coverage_marker|scope=all", topic_keys)
        content_summary = next(row for row in content_rows if row.row_key == "kind=day_summary|scope=all")
        topic_summary = next(row for row in topic_rows if row.row_key == "kind=topic_day|topic=_summary")
        self.assertIn("communityBrief", content_summary.payload_json["factHints"]["widgetPayloads"])
        self.assertIn("communityBrief", topic_summary.payload_json["factHints"]["widgetPayloads"])

    def test_failed_widget_build_marks_family_coverage_degraded(self) -> None:
        ctx = build_dashboard_date_context("2026-04-18", "2026-04-18")
        with patch(
            "api.dashboard_v2_materializer._WIDGET_FACT_BUILDERS",
            {"community_brief": lambda _ctx: (_ for _ in ()).throw(RuntimeError("boom"))},
        ):
            rows = _materialize_family_rows("content", ctx)

        coverage = next(row for row in rows if row.row_key == "kind=coverage_marker|scope=all")
        self.assertFalse(coverage.payload_json["coverageReady"])
        self.assertTrue(coverage.payload_json["coverageDegraded"])
        self.assertEqual(coverage.payload_json["coverageState"], "degraded")
        self.assertIn("community_brief", coverage.payload_json["failedWidgets"])

    def test_foundation_materializer_records_runs_for_each_family(self) -> None:
        store = _FakeStore()
        with patch("api.dashboard_v2_materializer.FACT_FAMILIES", ("content", "topics")), \
             patch(
                 "api.dashboard_v2_materializer._materialize_family_rows",
                 side_effect=lambda family, ctx, *args, **kwargs: [
                     DashboardV2FactRow(row_key=f"{family}-{ctx.cache_key}", payload_json={"ok": True})
                 ],
             ), \
             patch(
                 "api.dashboard_v2_materializer._materialize_secondary_rows",
                 return_value=[{"widgetId": "question_cloud", "status": "ready"}],
             ):
            result = materialize_dashboard_v2_foundation(
                store,
                mode="incremental",
                end_date=date(2026, 4, 18),
                lookback_days=2,
            )

        self.assertEqual(result["coverage_start"], "2026-04-17")
        self.assertEqual(result["coverage_end"], "2026-04-18")
        self.assertEqual(len(store.created_runs), 2)
        self.assertEqual(len(store.replaced_rows), 4)
        self.assertEqual(len(store.completed_runs), 2)
        self.assertEqual(len(store.marked_stale), 4)
        self.assertEqual(result["secondary_runs"], [{"widgetId": "question_cloud", "status": "ready"}])

    def test_foundation_materializer_records_degraded_days_in_run_meta(self) -> None:
        store = _FakeStore()
        build_results = [
            {"community_brief": {"messagesAnalyzed": 3}},
            {"community_brief": None},
        ]
        failed_widgets = [(), ("community_brief",)]

        def fake_build(_ctx):
            index = len(store.replaced_rows)
            from api.dashboard_v2_materializer import DashboardV2WidgetBuildResult

            return DashboardV2WidgetBuildResult(
                outputs=build_results[index],
                failed_widget_ids=failed_widgets[index],
            )

        with patch("api.dashboard_v2_materializer.FACT_FAMILIES", ("content",)), \
             patch("api.dashboard_v2_materializer._build_exact_widget_outputs", side_effect=fake_build), \
             patch(
                 "api.dashboard_v2_materializer._materialize_secondary_rows",
                 return_value=[],
             ):
            result = materialize_dashboard_v2_foundation(
                store,
                mode="incremental",
                end_date=date(2026, 4, 18),
                lookback_days=2,
            )

        self.assertEqual(result["family_runs"][0]["degradedDays"], ["2026-04-18"])
        self.assertEqual(result["family_runs"][0]["failedWidgets"], ["community_brief"])
        self.assertEqual(store.completed_runs[0][2]["degradedDays"], ["2026-04-18"])

    def test_run_dashboard_v2_materialize_slice_once_completes_claimed_slice(self) -> None:
        store = _SliceQueueStore()

        with patch(
            "api.dashboard_v2_materializer._materialize_family_window",
            return_value={
                "factFamily": "content",
                "runId": "fact-run-1",
                "status": "completed",
                "rowsInserted": 9,
                "daysProcessed": 3,
                "degradedDays": [],
                "failedWidgets": [],
            },
        ):
            result = run_dashboard_v2_materialize_slice_once(store, worker_id="worker-1", lease_seconds=90)

        self.assertIsNotNone(result)
        self.assertEqual(store.completed[0][:5], ("job-1", "slice-1", "worker-1", 9, 3))
        self.assertEqual(result["job"]["status"], "running")


if __name__ == "__main__":
    unittest.main()
