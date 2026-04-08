from __future__ import annotations

import subprocess
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from openclaw_bridge import app as bridge_app


class OpenClawBridgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(bridge_app.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()

    def _headers(self) -> dict[str, str]:
        return {"Authorization": "Bearer bridge-secret"}

    def _payload(self) -> dict[str, object]:
        return {
            "requestId": "req-123",
            "sessionId": "web_12345678",
            "messages": [{"role": "user", "text": "Reply with exactly WEB_HELPER_OK"}],
        }

    def test_requires_auth(self) -> None:
        with patch.object(bridge_app, "OPENCLAW_BRIDGE_TOKEN", "bridge-secret"):
            response = self.client.post("/openclaw-agent/chat", json=self._payload())

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "bridge_auth_failed")

    def test_rejects_large_request_body(self) -> None:
        with patch.object(bridge_app, "OPENCLAW_BRIDGE_TOKEN", "bridge-secret"), \
             patch.object(bridge_app, "OPENCLAW_BRIDGE_HTTP_MAX_BODY_BYTES", 64):
            response = self.client.post("/openclaw-agent/chat", json=self._payload(), headers=self._headers())

        self.assertEqual(response.status_code, 413)
        self.assertEqual(response.json()["error"]["code"], "bridge_validation_failed")

    def test_rejects_invalid_payload(self) -> None:
        payload = {
            "requestId": "req-123",
            "sessionId": "bad",
            "messages": [{"role": "assistant", "text": "No user turn"}],
        }
        with patch.object(bridge_app, "OPENCLAW_BRIDGE_TOKEN", "bridge-secret"):
            response = self.client.post("/openclaw-agent/chat", json=payload, headers=self._headers())

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "bridge_validation_failed")

    def test_subprocess_timeout_maps_to_bridge_timeout(self) -> None:
        with patch.object(bridge_app, "OPENCLAW_BRIDGE_TOKEN", "bridge-secret"), \
             patch("openclaw_bridge.app.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd=["docker"], timeout=90)):
            response = self.client.post("/openclaw-agent/chat", json=self._payload(), headers=self._headers())

        self.assertEqual(response.status_code, 504)
        self.assertEqual(response.json()["error"]["code"], "bridge_timeout")

    def test_subprocess_nonzero_exit_maps_to_bridge_subprocess_failed(self) -> None:
        completed = subprocess.CompletedProcess(args=["docker"], returncode=1, stdout="", stderr="boom")
        with patch.object(bridge_app, "OPENCLAW_BRIDGE_TOKEN", "bridge-secret"), \
             patch("openclaw_bridge.app.subprocess.run", return_value=completed):
            response = self.client.post("/openclaw-agent/chat", json=self._payload(), headers=self._headers())

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"]["code"], "bridge_subprocess_failed")

    def test_malformed_stdout_maps_to_bridge_parse_failed(self) -> None:
        completed = subprocess.CompletedProcess(args=["docker"], returncode=0, stdout="not-json", stderr="")
        with patch.object(bridge_app, "OPENCLAW_BRIDGE_TOKEN", "bridge-secret"), \
             patch("openclaw_bridge.app.subprocess.run", return_value=completed):
            response = self.client.post("/openclaw-agent/chat", json=self._payload(), headers=self._headers())

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"]["code"], "bridge_parse_failed")

    def test_runtime_error_maps_to_openclaw_runtime_error(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["docker"],
            returncode=0,
            stdout='{"error":{"message":"runtime blew up"}}',
            stderr="",
        )
        with patch.object(bridge_app, "OPENCLAW_BRIDGE_TOKEN", "bridge-secret"), \
             patch("openclaw_bridge.app.subprocess.run", return_value=completed):
            response = self.client.post("/openclaw-agent/chat", json=self._payload(), headers=self._headers())

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"]["code"], "openclaw_runtime_error")

    def test_success_returns_structured_message(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["docker"],
            returncode=0,
            stdout='{"choices":[{"message":{"role":"assistant","content":"WEB_HELPER_OK"}}]}',
            stderr="",
        )
        with patch.object(bridge_app, "OPENCLAW_BRIDGE_TOKEN", "bridge-secret"), \
             patch("openclaw_bridge.app.subprocess.run", return_value=completed):
            response = self.client.post("/openclaw-agent/chat", json=self._payload(), headers=self._headers())

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["message"]["text"], "WEB_HELPER_OK")


if __name__ == "__main__":
    unittest.main()
