from __future__ import annotations

import io
import json
import socket
import sys
import unittest
from pathlib import Path
from unittest import mock
from urllib import error as urllib_error

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from client import AnalyticsAPIError, AnalyticsClient
from models import ClientConfig


class FakeResponse:
    def __init__(self, payload: object):
        self.payload = payload

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def build_http_error(code: int, payload: dict[str, object]) -> urllib_error.HTTPError:
    return urllib_error.HTTPError(
        url="https://example.com/api/dashboard",
        code=code,
        msg="error",
        hdrs=None,
        fp=io.BytesIO(json.dumps(payload).encode("utf-8")),
    )


class ClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = AnalyticsClient(
            ClientConfig(
                base_url="https://analytics.example.com",
                api_key="sk_test",
                timeout=1.0,
                max_retries=2,
                backoff_base=0.01,
            )
        )

    def test_client_config_defaults_match_production_tuning(self) -> None:
        config = ClientConfig(
            base_url="https://analytics.example.com",
            api_key="sk_test",
        )

        self.assertEqual(config.timeout, 35.0)
        self.assertEqual(config.max_retries, 2)
        self.assertEqual(config.backoff_base, 0.5)

    @mock.patch("client.time.sleep", autospec=True)
    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_retries_server_error_then_succeeds(self, mock_urlopen, mock_sleep) -> None:
        mock_urlopen.side_effect = [
            build_http_error(500, {"detail": "temporary failure"}),
            FakeResponse({"data": {"trendingTopics": []}}),
        ]

        payload = self.client.get_dashboard("7d")

        self.assertIn("data", payload)
        self.assertEqual(mock_urlopen.call_count, 2)
        mock_sleep.assert_called_once()

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_maps_auth_error(self, mock_urlopen) -> None:
        mock_urlopen.side_effect = build_http_error(401, {"detail": "unauthorized"})

        with self.assertRaises(AnalyticsAPIError) as ctx:
            self.client.get_dashboard("7d")

        self.assertEqual(ctx.exception.error_type, "auth_error")

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_preserves_backend_not_found_detail(self, mock_urlopen) -> None:
        mock_urlopen.side_effect = build_http_error(404, {"detail": "Topic not found for the selected window."})

        with self.assertRaises(AnalyticsAPIError) as ctx:
            self.client.get_topic_detail("Politics", None, "7d")

        self.assertEqual(ctx.exception.error_type, "not_found")
        self.assertEqual(ctx.exception.message, "Topic not found for the selected window.")

    @mock.patch("client.time.sleep", autospec=True)
    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_retries_timeout(self, mock_urlopen, mock_sleep) -> None:
        mock_urlopen.side_effect = [
            socket.timeout("timed out"),
            FakeResponse([{"label": "Positive", "count": 3}]),
        ]

        payload = self.client.get_sentiment_distribution("7d")

        self.assertEqual(payload[0]["label"], "Positive")
        self.assertEqual(mock_urlopen.call_count, 2)
        mock_sleep.assert_called_once()

    @mock.patch("client.time.sleep", autospec=True)
    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_retry_backoff_stays_exponential(self, mock_urlopen, mock_sleep) -> None:
        client = AnalyticsClient(
            ClientConfig(
                base_url="https://analytics.example.com",
                api_key="sk_test",
                timeout=1.0,
                max_retries=2,
                backoff_base=0.01,
            )
        )
        mock_urlopen.side_effect = [
            build_http_error(503, {"detail": "temporary failure"}),
            build_http_error(503, {"detail": "temporary failure"}),
            build_http_error(503, {"detail": "temporary failure"}),
        ]

        with self.assertRaises(AnalyticsAPIError) as ctx:
            client.get_dashboard("7d")

        self.assertEqual(ctx.exception.error_type, "upstream_error")
        self.assertEqual(mock_urlopen.call_count, 3)
        self.assertEqual(mock_sleep.call_args_list, [mock.call(0.01), mock.call(0.02)])

    @mock.patch("client.time.sleep", autospec=True)
    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_timeout_message_is_clean_after_retries_exhaust(self, mock_urlopen, mock_sleep) -> None:
        mock_urlopen.side_effect = [
            socket.timeout("timed out"),
            socket.timeout("timed out"),
            socket.timeout("timed out"),
        ]

        with self.assertRaises(AnalyticsAPIError) as ctx:
            self.client.get_dashboard("7d")

        self.assertEqual(ctx.exception.error_type, "timeout")
        self.assertEqual(
            ctx.exception.message,
            "The analytics backend is taking too long to respond right now. It may be waking up. Please try again in a moment.",
        )
        self.assertEqual(mock_sleep.call_args_list, [mock.call(0.01), mock.call(0.02)])

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_search_entities_builds_expected_url(self, mock_urlopen) -> None:
        mock_urlopen.return_value = FakeResponse([{"type": "topic", "name": "Residency permits"}])

        payload = self.client.search_entities("permit", 4)

        self.assertEqual(payload[0]["type"], "topic")
        request = mock_urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://analytics.example.com/api/search?query=permit&limit=4")

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_topic_detail_builds_date_range_query(self, mock_urlopen) -> None:
        mock_urlopen.return_value = FakeResponse({"name": "Residency permits"})

        self.client.get_topic_detail("Residency permits", "Documents", "7d")

        request = mock_urlopen.call_args.args[0]
        self.assertIn("/api/topics/detail?", request.full_url)
        self.assertIn("topic=Residency+permits", request.full_url)
        self.assertIn("category=Documents", request.full_url)
        self.assertIn("from=", request.full_url)
        self.assertIn("to=", request.full_url)

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_topic_evidence_builds_focus_query(self, mock_urlopen) -> None:
        mock_urlopen.return_value = FakeResponse({"items": []})

        self.client.get_topic_evidence(
            "Residency permits",
            "Documents",
            "questions",
            page=0,
            size=3,
            focus_id="comment:123",
            window="7d",
        )

        request = mock_urlopen.call_args.args[0]
        self.assertIn("/api/topics/evidence?", request.full_url)
        self.assertIn("view=questions", request.full_url)
        self.assertIn("size=3", request.full_url)
        self.assertIn("focusId=comment%3A123", request.full_url)

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_freshness_status_uses_force_query(self, mock_urlopen) -> None:
        mock_urlopen.return_value = FakeResponse({"health": {"status": "healthy"}})

        payload = self.client.get_freshness_status(True)

        self.assertEqual(payload["health"]["status"], "healthy")
        request = mock_urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://analytics.example.com/api/freshness?force=true")

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_graph_snapshot_posts_graph_payload(self, mock_urlopen) -> None:
        mock_urlopen.return_value = FakeResponse({"nodes": [], "meta": {}})

        self.client.get_graph_data("7d", category="Documents", signal_focus="needs", max_nodes=12)

        request = mock_urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://analytics.example.com/api/graph")
        self.assertEqual(request.method, "POST")
        self.assertIn(b'"timeframe": "Last 7 Days"', request.data)
        self.assertIn(b'"category": "Documents"', request.data)
        self.assertIn(b'"signalFocus": "needs"', request.data)

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_node_details_uses_timeframe_query(self, mock_urlopen) -> None:
        mock_urlopen.return_value = FakeResponse({"id": "topic:Residency permits"})

        self.client.get_node_details("topic:Residency permits", "topic", "7d")

        request = mock_urlopen.call_args.args[0]
        self.assertIn("/api/node-details?", request.full_url)
        self.assertIn("nodeId=topic%3AResidency+permits", request.full_url)
        self.assertIn("nodeType=topic", request.full_url)
        self.assertIn("timeframe=Last+7+Days", request.full_url)

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_channel_detail_builds_date_range_query(self, mock_urlopen) -> None:
        mock_urlopen.return_value = FakeResponse({"title": "Docs Chat"})

        self.client.get_channel_detail("Docs Chat", "7d")

        request = mock_urlopen.call_args.args[0]
        self.assertIn("/api/channels/detail?", request.full_url)
        self.assertIn("channel=Docs+Chat", request.full_url)
        self.assertIn("from=", request.full_url)
        self.assertIn("to=", request.full_url)

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_channel_posts_builds_limit_query(self, mock_urlopen) -> None:
        mock_urlopen.return_value = FakeResponse({"items": []})

        self.client.get_channel_posts("Docs Chat", limit=3, page=0, window="7d")

        request = mock_urlopen.call_args.args[0]
        self.assertIn("/api/channels/posts?", request.full_url)
        self.assertIn("size=3", request.full_url)
        self.assertIn("page=0", request.full_url)

    @mock.patch("client.urllib_request.urlopen", autospec=True)
    def test_graph_summary_helpers_use_timeframe_query(self, mock_urlopen) -> None:
        mock_urlopen.return_value = FakeResponse([{"name": "Docs Chat"}])

        self.client.get_top_channels(4, "7d")
        request = mock_urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://analytics.example.com/api/top-channels?limit=4&timeframe=Last+7+Days")

        self.client.get_trending_topics(3, "7d")
        request = mock_urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://analytics.example.com/api/trending-topics?limit=3&timeframe=Last+7+Days")

        self.client.get_graph_insights("7d")
        request = mock_urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://analytics.example.com/api/graph-insights?timeframe=Last+7+Days")


if __name__ == "__main__":
    unittest.main()
