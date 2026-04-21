from __future__ import annotations

import unittest
from unittest.mock import patch

from api.dashboard_dates import build_dashboard_date_context
from api.queries import comparative, network


class NetworkQueryScopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")

    def test_community_channels_uses_single_bounded_query(self) -> None:
        with patch.object(network, "run_query", return_value=[]) as run_query_mock:
            rows = network.get_community_channels(self.ctx)

        self.assertEqual(rows, [])
        run_query_mock.assert_called_once()
        query = run_query_mock.call_args.args[0]
        params = run_query_mock.call_args.args[1]
        self.assertIn("CALL {", query)
        self.assertIn("datetime($start)", query)
        self.assertIn("datetime($end)", query)
        self.assertEqual(params["start"], self.ctx.start_at.isoformat())
        self.assertEqual(params["end"], self.ctx.end_at.isoformat())

    def test_network_activity_queries_are_window_scoped(self) -> None:
        with patch.object(network, "run_query", return_value=[]) as run_query_mock:
            network.get_hourly_activity(self.ctx)
            network.get_weekly_activity(self.ctx)
            network.get_recommendations(self.ctx)
            network.get_viral_topics(self.ctx)

        self.assertEqual(run_query_mock.call_count, 4)
        for call in run_query_mock.call_args_list:
            query = call.args[0]
            params = call.args[1]
            self.assertIn("datetime($start)", query)
            self.assertIn("datetime($end)", query)
            self.assertEqual(params["start"], self.ctx.start_at.isoformat())
            self.assertEqual(params["end"], self.ctx.end_at.isoformat())


class ComparativeQueryScopeTests(unittest.TestCase):
    def test_vitality_indicators_use_selected_window(self) -> None:
        ctx = build_dashboard_date_context("2026-03-31", "2026-04-06")
        with patch.object(comparative, "run_single", return_value={"totalUsers": 10, "activeUsers": 5, "totalTopics": 7, "totalPosts": 8, "totalComments": 16}) as run_single_mock:
            payload = comparative.get_vitality_indicators(ctx)

        self.assertEqual(payload["totalUsers"], 10)
        self.assertEqual(payload["activeUsers7d"], 5)
        query = run_single_mock.call_args.args[0]
        params = run_single_mock.call_args.args[1]
        self.assertIn("datetime($start)", query)
        self.assertIn("datetime($end)", query)
        self.assertEqual(params["start"], ctx.start_at.isoformat())
        self.assertEqual(params["end"], ctx.end_at.isoformat())


if __name__ == "__main__":
    unittest.main()
