from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from api.dashboard_dates import build_dashboard_date_context
from api.queries import actionable, pulse


class _RpcResponse:
    def __init__(self, data):
        self.data = data


class _RpcCall:
    def __init__(self, data):
        self._data = data

    def execute(self):
        return _RpcResponse(self._data)


class _PulseRpcClient:
    def __init__(self, data):
        self._data = data

    def rpc(self, name: str, params: dict):
        del params
        if name != "dashboard_ai_analysis_window_summary":
            raise AssertionError(name)
        return _RpcCall(self._data)


class _ActionableRpcClient:
    def __init__(self, data):
        self._data = data

    def rpc(self, name: str, params: dict):
        del params
        if name != "dashboard_batch_signal_summary":
            raise AssertionError(name)
        return _RpcCall(self._data)


class DashboardSupabaseSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        pulse._summary_cache.clear()
        pulse._health_cache.clear()
        actionable._work_signal_snapshot_cache.clear()

    def test_pulse_prefers_rpc_summary(self) -> None:
        fake_writer = SimpleNamespace(
            client=_PulseRpcClient([
                {
                    "analysis_units": 12,
                    "positive_rows": 5,
                    "negative_rows": 3,
                    "neutral_rows": 4,
                    "unique_users": 7,
                    "posts_analyzed": 6,
                    "comment_scopes_analyzed": 5,
                }
            ])
        )
        with patch.object(pulse, "_supabase", return_value=fake_writer):
            summary = pulse._fetch_analysis_summary(
                start=build_dashboard_date_context("2026-03-31", "2026-04-06").start_at,
                end=build_dashboard_date_context("2026-03-31", "2026-04-06").end_at,
            )

        self.assertEqual(summary["analysis_units"], 12)
        self.assertEqual(summary["positive"], 5)
        self.assertEqual(summary["comment_scopes_analyzed"], 5)

    def test_pulse_falls_back_to_row_scan_when_rpc_fails(self) -> None:
        rows = [
            {
                "content_type": "post",
                "content_id": "p-1",
                "channel_id": "c-1",
                "telegram_user_id": "u-1",
                "primary_intent": "Support / Help",
                "sentiment_score": 0.5,
                "created_at": "2026-04-01T00:00:00Z",
            },
            {
                "content_type": "batch",
                "content_id": "b-1",
                "channel_id": "c-1",
                "telegram_user_id": "u-2",
                "primary_intent": "Complaint",
                "sentiment_score": -0.6,
                "created_at": "2026-04-01T00:01:00Z",
            },
        ]

        class _BrokenClient:
            def rpc(self, name: str, params: dict):
                del name, params
                raise RuntimeError("rpc unavailable")

        with patch.object(pulse, "_supabase", return_value=SimpleNamespace(client=_BrokenClient())), \
             patch.object(pulse, "_fetch_analysis_rows", return_value=rows):
            summary = pulse._fetch_analysis_summary(
                start=build_dashboard_date_context("2026-03-31", "2026-04-06").start_at,
                end=build_dashboard_date_context("2026-03-31", "2026-04-06").end_at,
            )

        self.assertEqual(summary["analysis_units"], 2)
        self.assertEqual(summary["positive"], 1)
        self.assertEqual(summary["negative"], 1)

    def test_actionable_prefers_rpc_signal_summary(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")
        fake_writer = SimpleNamespace(
            client=_ActionableRpcClient(
                [
                    {"window_key": "current", "user_id": "1", "signal_type": "Job_Seeking", "signal_count": 3},
                    {"window_key": "previous", "user_id": "2", "signal_type": "Hiring", "signal_count": 1},
                ]
            )
        )
        with patch.object(actionable, "_get_supabase_writer", return_value=fake_writer):
            snapshot = actionable._build_work_signal_snapshot(ctx)

        self.assertEqual(snapshot["jobSeeking"][0]["signalType"], "Job_Seeking")
        self.assertEqual(snapshot["jobTrends"][0]["topic"], "Job_Seeking")


if __name__ == "__main__":
    unittest.main()
