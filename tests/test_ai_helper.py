from __future__ import annotations

import asyncio
import os
import time
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

import config
from api import server
from api.ai_helper import AIHelperError, AIHelperMessage, OpenClawAiHelperProvider, _compose_endpoint


class _FakeWriter:
    def __init__(self, user_id: str, email: str = "admin@example.com") -> None:
        self.client = SimpleNamespace(
            auth=SimpleNamespace(
                get_user=lambda _token: SimpleNamespace(
                    user=SimpleNamespace(id=user_id, email=email),
                )
            )
        )


class _FakeProvider:
    def __init__(self) -> None:
        self.chat_calls: list[str] = []
        self.reset_calls = 0

    async def chat(self, message: str) -> AIHelperMessage:
        self.chat_calls.append(message)
        return AIHelperMessage(role="assistant", text=f"Echo: {message}", timestamp="2026-03-27T00:00:00Z")

    async def history(self, limit: int = 50) -> list[AIHelperMessage]:
        return [
            AIHelperMessage(role="assistant", text="Stored reply", timestamp="2026-03-27T00:00:00Z"),
        ]

    async def reset(self) -> str:
        self.reset_calls += 1
        return "2026-03-27T00:00:00Z"


class AIHelperEndpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._startup_handlers = list(server.app.router.on_startup)
        cls._shutdown_handlers = list(server.app.router.on_shutdown)
        server.app.router.on_startup = []
        server.app.router.on_shutdown = []
        cls.client = TestClient(server.app)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.client.close()
        server.app.router.on_startup = cls._startup_handlers
        server.app.router.on_shutdown = cls._shutdown_handlers

    def test_chat_requires_auth(self) -> None:
        response = self.client.post("/api/ai-helper/chat", json={"message": "hello"})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "auth_required")

    def test_chat_does_not_accept_openclaw_analytics_token_as_admin_auth(self) -> None:
        class _FailingWriter:
            def __init__(self) -> None:
                self.client = SimpleNamespace(
                    auth=SimpleNamespace(
                        get_user=lambda _token: (_ for _ in ()).throw(RuntimeError("invalid session token"))
                    )
                )

        with patch.object(server.config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"), \
             patch.object(server.config, "AI_HELPER_ADMIN_EMAIL", ""), \
             patch.object(server.config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"), \
             patch.object(server, "get_supabase_writer", return_value=_FailingWriter()):
            response = self.client.post(
                "/api/ai-helper/chat",
                json={"message": "hello"},
                headers={"Authorization": "Bearer openclaw-secret"},
            )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "auth_invalid")

    def test_chat_accepts_frontend_proxy_token_in_staging(self) -> None:
        provider = _FakeProvider()
        with patch.object(server.config, "IS_STAGING", True), \
             patch.object(server.config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"), \
             patch.object(server.config, "AI_HELPER_ADMIN_EMAIL", ""), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server, "get_ai_helper_provider", return_value=provider):
            response = self.client.post(
                "/api/ai-helper/chat",
                json={"message": "hello"},
                headers={"Authorization": "Bearer frontend-secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["message"]["text"], "Echo: hello")
        self.assertEqual(provider.chat_calls, ["hello"])

    def test_chat_prefers_supabase_validation_when_supabase_token_present(self) -> None:
        class _FailingWriter:
            def __init__(self) -> None:
                self.client = SimpleNamespace(
                    auth=SimpleNamespace(
                        get_user=lambda _token: (_ for _ in ()).throw(RuntimeError("invalid session token"))
                    )
                )

        with patch.object(server.config, "IS_STAGING", True), \
             patch.object(server.config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"), \
             patch.object(server.config, "AI_HELPER_ADMIN_EMAIL", ""), \
             patch.object(server.config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"), \
             patch.object(server, "get_supabase_writer", return_value=_FailingWriter()):
            response = self.client.post(
                "/api/ai-helper/chat",
                json={"message": "hello"},
                headers={
                    "Authorization": "Bearer frontend-secret",
                    "X-Supabase-Authorization": "Bearer invalid-user-token",
                },
            )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "auth_invalid")

    def test_chat_rejects_non_admin_user(self) -> None:
        with patch.object(server.config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"), \
             patch.object(server.config, "AI_HELPER_ADMIN_EMAIL", ""), \
             patch.object(server, "get_supabase_writer", return_value=_FakeWriter("someone-else")):
            response = self.client.post(
                "/api/ai-helper/chat",
                json={"message": "hello"},
                headers={"X-Supabase-Authorization": "Bearer test-token"},
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["error"]["code"], "admin_only")

    def test_chat_returns_provider_message(self) -> None:
        provider = _FakeProvider()
        with patch.object(server.config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"), \
             patch.object(server.config, "AI_HELPER_ADMIN_EMAIL", ""), \
             patch.object(server, "get_supabase_writer", return_value=_FakeWriter("admin-user")), \
             patch.object(server, "get_ai_helper_provider", return_value=provider):
            response = self.client.post(
                "/api/ai-helper/chat",
                json={"message": "hello"},
                headers={"X-Supabase-Authorization": "Bearer test-token"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["message"]["text"], "Echo: hello")
        self.assertEqual(provider.chat_calls, ["hello"])

    def test_history_returns_normalized_messages(self) -> None:
        provider = _FakeProvider()
        with patch.object(server.config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"), \
             patch.object(server.config, "AI_HELPER_ADMIN_EMAIL", ""), \
             patch.object(server, "get_supabase_writer", return_value=_FakeWriter("admin-user")), \
             patch.object(server, "get_ai_helper_provider", return_value=provider):
            response = self.client.get(
                "/api/ai-helper/history?limit=10",
                headers={"X-Supabase-Authorization": "Bearer test-token"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["messages"][0]["role"], "assistant")

    def test_reset_returns_confirmation(self) -> None:
        provider = _FakeProvider()
        with patch.object(server.config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"), \
             patch.object(server.config, "AI_HELPER_ADMIN_EMAIL", ""), \
             patch.object(server, "get_supabase_writer", return_value=_FakeWriter("admin-user")), \
             patch.object(server, "get_ai_helper_provider", return_value=provider):
            response = self.client.post(
                "/api/ai-helper/reset",
                headers={"X-Supabase-Authorization": "Bearer test-token"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["reset"])
        self.assertEqual(provider.reset_calls, 1)

    def test_validation_errors_use_helper_error_shape(self) -> None:
        with patch.object(server.config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"), \
             patch.object(server.config, "AI_HELPER_ADMIN_EMAIL", ""), \
             patch.object(server, "get_supabase_writer", return_value=_FakeWriter("admin-user")):
            response = self.client.post(
                "/api/ai-helper/chat",
                json={"message": ""},
                headers={"X-Supabase-Authorization": "Bearer test-token"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "invalid_request")


class OpenClawProviderTests(unittest.TestCase):
    def _provider(self, **kwargs) -> OpenClawAiHelperProvider:
        session_key = kwargs.pop("session_key", f"tg-analyst-ru-web-admin-{time.time_ns()}")
        transport = kwargs.pop("transport", "openai_compatible")
        model = kwargs.pop("model", "openclaw/tg-analyst-ru")
        manage_transcript = kwargs.pop("manage_transcript", True)
        agent_id = kwargs.pop("agent_id", "tg-analyst-ru")
        return OpenClawAiHelperProvider(
            base_url="https://openclaw.example.com/v1",
            gateway_token="secret-token",
            agent_id=agent_id,
            session_key=session_key,
            timeout_seconds=3,
            connect_timeout_seconds=1,
            read_timeout_seconds=1,
            retry_attempts=1,
            transport=transport,
            model=model,
            manage_transcript=manage_transcript,
            **kwargs,
        )

    def test_compose_endpoint_avoids_double_v1(self) -> None:
        self.assertEqual(
            _compose_endpoint("https://openclaw.example.com/v1", "/chat/completions"),
            "https://openclaw.example.com/v1/chat/completions",
        )
        self.assertEqual(
            _compose_endpoint("https://openclaw.example.com", "/v1/responses"),
            "https://openclaw.example.com/v1/responses",
        )

    def test_openai_compatible_headers_omit_legacy_agent_headers(self) -> None:
        provider = self._provider()
        headers = provider._build_headers()
        self.assertNotIn("x-openclaw-agent-id", headers)
        self.assertNotIn("x-openclaw-session-key", headers)

    def test_legacy_headers_include_agent_and_session(self) -> None:
        provider = self._provider(
            transport="legacy",
            model="",
            manage_transcript=False,
            session_key="tg-analyst-ru-web-admin",
        )
        headers = provider._build_headers()
        self.assertEqual(headers["x-openclaw-agent-id"], "tg-analyst-ru")
        self.assertEqual(headers["x-openclaw-session-key"], "tg-analyst-ru-web-admin")

    def test_chat_completions_uses_configured_model_without_model_discovery(self) -> None:
        provider = self._provider()
        seen: dict[str, object] = {}

        def fake_request(_method, url, params=None, headers=None, json=None):
            seen["url"] = str(url)
            seen["payload"] = json
            request = httpx.Request("POST", str(url))
            return httpx.Response(
                200,
                request=request,
                json={
                    "choices": [
                        {"message": {"role": "assistant", "content": "OpenClaw says hi"}},
                    ]
                },
            )

        with patch("httpx.Client.request", side_effect=fake_request):
            reply = asyncio.run(provider.chat("Hello"))

        self.assertEqual(reply.text, "OpenClaw says hi")
        self.assertEqual(seen["url"], "https://openclaw.example.com/v1/chat/completions")
        self.assertEqual(seen["payload"]["model"], "openclaw/tg-analyst-ru")
        self.assertEqual(seen["payload"]["messages"][-1]["content"], "Hello")

    def test_chat_retries_once_on_transient_timeout(self) -> None:
        provider = self._provider()
        call_count = 0

        def fake_request(_method, url, params=None, headers=None, json=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectTimeout("timed out")
            request = httpx.Request("POST", str(url))
            return httpx.Response(
                200,
                request=request,
                json={
                    "choices": [
                        {"message": {"role": "assistant", "content": "Recovered"}},
                    ]
                },
            )

        with patch("httpx.Client.request", side_effect=fake_request):
            reply = asyncio.run(provider.chat("Hello"))

        self.assertEqual(call_count, 2)
        self.assertEqual(reply.text, "Recovered")

    def test_history_and_reset_use_local_transcript_store_in_openai_mode(self) -> None:
        provider = self._provider(session_key=f"transcript-{time.time_ns()}")

        def fake_request(_method, url, params=None, headers=None, json=None):
            request = httpx.Request("POST", str(url))
            return httpx.Response(
                200,
                request=request,
                json={
                    "choices": [
                        {"message": {"role": "assistant", "content": "Stored answer"}},
                    ]
                },
            )

        with patch("httpx.Client.request", side_effect=fake_request):
            asyncio.run(provider.chat("Question"))

        history = asyncio.run(provider.history(limit=10))
        self.assertEqual([message.role for message in history], ["user", "assistant"])
        self.assertEqual(history[-1].text, "Stored answer")

        timestamp = asyncio.run(provider.reset())
        self.assertTrue(timestamp)
        self.assertEqual(asyncio.run(provider.history(limit=10)), [])

    def test_history_trims_old_messages_to_bounds(self) -> None:
        session_key = f"trim-{time.time_ns()}"
        provider = self._provider(session_key=session_key)
        with ExitStack() as stack:
            stack.enter_context(patch.object(config, "OPENCLAW_HELPER_HISTORY_MAX_MESSAGES", 4))
            stack.enter_context(patch.object(config, "OPENCLAW_HELPER_HISTORY_MAX_CHARS", 1000))

            def fake_request(_method, url, params=None, headers=None, json=None):
                request = httpx.Request("POST", str(url))
                user_messages = [item["content"] for item in json["messages"] if item["role"] == "user"]
                return httpx.Response(
                    200,
                    request=request,
                    json={
                        "choices": [
                            {"message": {"role": "assistant", "content": f"Reply to {user_messages[-1]}"}},
                        ]
                    },
                )

            with patch("httpx.Client.request", side_effect=fake_request):
                for idx in range(3):
                    asyncio.run(provider.chat(f"Question {idx}"))

        history = asyncio.run(provider.history(limit=10))
        self.assertEqual(len(history), 4)
        self.assertEqual(history[0].text, "Question 1")
        self.assertEqual(history[-1].text, "Reply to Question 2")

    def test_history_trims_to_character_budget(self) -> None:
        session_key = f"char-trim-{time.time_ns()}"
        with ExitStack() as stack:
            stack.enter_context(patch.object(config, "OPENCLAW_HELPER_HISTORY_MAX_MESSAGES", 20))
            stack.enter_context(patch.object(config, "OPENCLAW_HELPER_HISTORY_MAX_CHARS", 50))
            provider = self._provider(session_key=session_key)

            def fake_request(_method, url, params=None, headers=None, json=None):
                request = httpx.Request("POST", str(url))
                return httpx.Response(
                    200,
                    request=request,
                    json={
                        "choices": [
                            {"message": {"role": "assistant", "content": "reply"}},
                        ]
                    },
                )

            with patch("httpx.Client.request", side_effect=fake_request):
                asyncio.run(provider.chat("A" * 30))
                asyncio.run(provider.chat("B" * 30))
                history = asyncio.run(provider.history(limit=10))

        self.assertEqual([message.text for message in history], ["B" * 30, "reply"])

    def test_openai_transport_requires_model(self) -> None:
        provider = self._provider(model="")
        with self.assertRaises(AIHelperError) as ctx:
            provider._validate_config()
        self.assertIn("OpenClaw model", str(ctx.exception))

    def test_legacy_transport_requires_agent_id(self) -> None:
        provider = self._provider(
            transport="legacy",
            model="",
            agent_id="",
            manage_transcript=False,
        )
        with self.assertRaises(AIHelperError) as ctx:
            provider._validate_config()
        self.assertIn("OpenClaw agent", str(ctx.exception))


class ConfigValidationTests(unittest.TestCase):
    def test_locked_env_requires_gateway_model_for_openai_transport(self) -> None:
        with ExitStack() as stack:
            stack.enter_context(patch.object(config, "IS_LOCKED_ENV", True))
            stack.enter_context(patch.object(config, "ANALYTICS_API_REQUIRE_AUTH", True))
            stack.enter_context(patch.object(config, "CORS_ALLOW_ORIGINS", ["https://app.example.com"]))
            stack.enter_context(patch.object(config, "REDIS_URL", "redis://localhost:6379/0"))
            stack.enter_context(patch.object(config, "ADMIN_API_KEY", "admin"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_FRONTEND", "front"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_BASE_URL", "https://openclaw.example.com/v1"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TOKEN", "token"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TRANSPORT", "openai_compatible"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_MODEL", ""))
            stack.enter_context(patch.object(config, "OPENCLAW_ANALYTICS_AGENT_ID", "tg-analyst-ru"))
            stack.enter_context(patch.object(config, "OPENCLAW_WEB_SESSION_KEY", "tg-analyst-ru-web-admin"))
            stack.enter_context(patch.object(config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"))

            with self.assertRaises(EnvironmentError) as ctx:
                config.validate()

        self.assertIn("OPENCLAW_GATEWAY_MODEL", str(ctx.exception))

    def test_locked_env_allows_legacy_transport_with_agent_id(self) -> None:
        with ExitStack() as stack:
            stack.enter_context(patch.object(config, "IS_LOCKED_ENV", True))
            stack.enter_context(patch.object(config, "ANALYTICS_API_REQUIRE_AUTH", True))
            stack.enter_context(patch.object(config, "CORS_ALLOW_ORIGINS", ["https://app.example.com"]))
            stack.enter_context(patch.object(config, "REDIS_URL", "redis://localhost:6379/0"))
            stack.enter_context(patch.object(config, "ADMIN_API_KEY", "admin"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_FRONTEND", "front"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_BASE_URL", "https://openclaw.example.com"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TOKEN", "token"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TRANSPORT", "legacy"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_MODEL", ""))
            stack.enter_context(patch.object(config, "OPENCLAW_ANALYTICS_AGENT_ID", "tg-analyst-ru"))
            stack.enter_context(patch.object(config, "OPENCLAW_WEB_SESSION_KEY", "tg-analyst-ru-web-admin"))
            stack.enter_context(patch.object(config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"))
            config.validate()


if __name__ == "__main__":
    unittest.main()
