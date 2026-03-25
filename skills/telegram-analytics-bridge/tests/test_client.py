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


if __name__ == "__main__":
    unittest.main()
