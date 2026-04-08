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
        return OpenClawAiHelperProvider(
            base_url="https://openclaw.example.com/v1",
            gateway_token="secret-token",
            agent_id="tg-analyst-ru",
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
                json={"choices": [{"message": {"role": "assistant", "content": "hello back"}}]},
                request=request,
            )

        with patch("api.ai_helper.httpx.Client.request", side_effect=fake_request):
            message = asyncio.run(provider.chat("hello"))

        self.assertEqual(message.text, "hello back")
        self.assertEqual(seen["url"], "https://openclaw.example.com/v1/chat/completions")
        self.assertEqual(seen["payload"]["model"], "openclaw/tg-analyst-ru")
        self.assertEqual(seen["payload"]["messages"][-1]["content"], "hello")

    def test_timeout_retries_once_then_succeeds(self) -> None:
        provider = self._provider()
        request = httpx.Request("POST", "https://openclaw.example.com/v1/chat/completions")
        response = httpx.Response(
            200,
            json={"choices": [{"message": {"role": "assistant", "content": "retry ok"}}]},
            request=request,
        )
        with patch(
            "api.ai_helper.httpx.Client.request",
            side_effect=[httpx.ReadTimeout("slow", request=request), response],
        ):
            message = asyncio.run(provider.chat("hello"))

        self.assertEqual(message.text, "retry ok")

    def test_timeout_maps_to_retryable_timeout_after_retries(self) -> None:
        provider = self._provider()
        request = httpx.Request("POST", "https://openclaw.example.com/v1/chat/completions")
        with patch(
            "api.ai_helper.httpx.Client.request",
            side_effect=httpx.ReadTimeout("slow", request=request),
        ):
            with self.assertRaises(AIHelperError) as ctx:
                asyncio.run(provider.chat("hello"))

        self.assertEqual(ctx.exception.code, "upstream_timeout")
        self.assertTrue(ctx.exception.retryable)

    def test_history_uses_backend_managed_transcript(self) -> None:
        provider = self._provider()
        responses = [
            {"choices": [{"message": {"role": "assistant", "content": "one"}}]},
            {"choices": [{"message": {"role": "assistant", "content": "two"}}]},
        ]
        with patch.object(provider, "_request_json", side_effect=responses):
            asyncio.run(provider.chat("first"))
            asyncio.run(provider.chat("second"))

        history = asyncio.run(provider.history(limit=10))
        self.assertEqual([item.text for item in history], ["first", "one", "second", "two"])

    def test_second_turn_replays_prior_transcript(self) -> None:
        provider = self._provider()
        payloads: list[dict[str, object]] = []

        def fake_request_json(_method, _path, _query, payload):
            payloads.append(payload)
            replies = ["one", "two"]
            return {"choices": [{"message": {"role": "assistant", "content": replies[len(payloads) - 1]}}]}

        with patch.object(provider, "_request_json", side_effect=fake_request_json):
            asyncio.run(provider.chat("first"))
            asyncio.run(provider.chat("second"))

        self.assertEqual(len(payloads[0]["messages"]), 1)
        self.assertEqual(len(payloads[1]["messages"]), 3)
        self.assertEqual(payloads[1]["messages"][0]["content"], "first")
        self.assertEqual(payloads[1]["messages"][1]["content"], "one")
        self.assertEqual(payloads[1]["messages"][2]["content"], "second")

    def test_reset_clears_backend_managed_transcript(self) -> None:
        provider = self._provider()
        with patch.object(
            provider,
            "_request_json",
            return_value={"choices": [{"message": {"role": "assistant", "content": "one"}}]},
        ):
            asyncio.run(provider.chat("first"))

        asyncio.run(provider.reset())
        history = asyncio.run(provider.history())
        self.assertEqual(history, [])

    def test_transcript_trims_by_message_count(self) -> None:
        provider = self._provider()
        with patch.object(config, "OPENCLAW_HELPER_HISTORY_MAX_MESSAGES", 4), \
             patch.object(provider, "_request_json", side_effect=[
                 {"choices": [{"message": {"role": "assistant", "content": "a1"}}]},
                 {"choices": [{"message": {"role": "assistant", "content": "a2"}}]},
                 {"choices": [{"message": {"role": "assistant", "content": "a3"}}]},
             ]):
            asyncio.run(provider.chat("u1"))
            asyncio.run(provider.chat("u2"))
            asyncio.run(provider.chat("u3"))

        history = asyncio.run(provider.history(limit=10))
        self.assertEqual([item.text for item in history], ["u2", "a2", "u3", "a3"])

    def test_transcript_trims_by_character_budget(self) -> None:
        provider = self._provider()
        long_user = "a" * 600
        long_assistant = "b" * 600
        with patch.object(config, "OPENCLAW_HELPER_HISTORY_MAX_MESSAGES", 10), \
             patch.object(config, "OPENCLAW_HELPER_HISTORY_MAX_CHARS", 1000), \
             patch.object(provider, "_request_json", side_effect=[
                 {"choices": [{"message": {"role": "assistant", "content": long_assistant}}]},
                 {"choices": [{"message": {"role": "assistant", "content": "dd"}}]},
             ]):
            asyncio.run(provider.chat(long_user))
            asyncio.run(provider.chat("cc"))

        history = asyncio.run(provider.history(limit=10))
        self.assertEqual([item.text for item in history], ["cc", "dd"])

    def test_history_404_returns_empty_messages_in_legacy_mode(self) -> None:
        provider = self._provider(transport="legacy", model="", manage_transcript=False)
        not_found = AIHelperError(
            status_code=404,
            code="upstream_not_found",
            message="missing",
            retryable=False,
        )
        with patch.object(provider, "_request_json", side_effect=not_found):
            messages = asyncio.run(provider.history())

        self.assertEqual(messages, [])

    def test_chat_requests_are_serialized_per_session(self) -> None:
        provider = self._provider(transport="legacy", model="", manage_transcript=False)
        active = 0
        max_active = 0

        def fake_request_json(_method, _path, _query, payload):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            time.sleep(0.05)
            active -= 1
            return {
                "output_text": f"reply:{payload['input']}",
                "created_at": "2026-03-27T00:00:00Z",
            }

        async def run_requests():
            with patch.object(provider, "_request_json", side_effect=fake_request_json):
                return await asyncio.gather(
                    provider.chat("first"),
                    provider.chat("second"),
                )

        responses = asyncio.run(run_requests())
        self.assertEqual(max_active, 1)
        self.assertEqual([item.text for item in responses], ["reply:first", "reply:second"])

    def test_http_401_maps_to_upstream_auth(self) -> None:
        provider = self._provider()
        request = httpx.Request("POST", "https://openclaw.example.com/v1/chat/completions")
        response = httpx.Response(
            401,
            json={"detail": "bad token"},
            request=request,
        )
        with patch("api.ai_helper.httpx.Client.request", return_value=response):
            with self.assertRaises(AIHelperError) as ctx:
                asyncio.run(provider.chat("hello"))

        self.assertEqual(ctx.exception.code, "upstream_auth")
        self.assertFalse(ctx.exception.retryable)


class ConfigValidationTests(unittest.TestCase):
    def test_staging_web_does_not_require_telegram_runtime_credentials(self) -> None:
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {"APP_ROLE": "web"}, clear=False))
            stack.enter_context(patch.object(config, "IS_STAGING", True))
            stack.enter_context(patch.object(config, "IS_PRODUCTION", False))
            stack.enter_context(patch.object(config, "IS_LOCKED_ENV", True))
            stack.enter_context(patch.object(config, "TELEGRAM_API_ID", 0))
            stack.enter_context(patch.object(config, "TELEGRAM_API_HASH", ""))
            stack.enter_context(patch.object(config, "TELEGRAM_PHONE", ""))
            stack.enter_context(patch.object(config, "SUPABASE_URL", "https://example.supabase.co"))
            stack.enter_context(patch.object(config, "SUPABASE_SERVICE_ROLE_KEY", "service-role"))
            stack.enter_context(patch.object(config, "NEO4J_URI", "bolt://localhost:7687"))
            stack.enter_context(patch.object(config, "NEO4J_PASSWORD", "password"))
            stack.enter_context(patch.object(config, "OPENAI_API_KEY", "openai"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_REQUIRE_AUTH", True))
            stack.enter_context(patch.object(config, "CORS_ALLOW_ORIGINS", ["https://app.example.com"]))
            stack.enter_context(patch.object(config, "REDIS_URL", "redis://localhost:6379/0"))
            stack.enter_context(patch.object(config, "ADMIN_API_KEY", "admin-secret"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_BASE_URL", "https://openclaw.example.com/v1"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TOKEN", "gateway-token"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TRANSPORT", "openai_compatible"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_MODEL", "openclaw/tg-analyst-ru"))
            stack.enter_context(patch.object(config, "OPENCLAW_ANALYTICS_AGENT_ID", "agent-id"))
            stack.enter_context(patch.object(config, "OPENCLAW_WEB_SESSION_KEY", "web-session"))
            stack.enter_context(patch.object(config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"))

            config.validate()

    def test_production_runtime_requires_telegram_runtime_credentials(self) -> None:
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {"APP_ROLE": "all"}, clear=False))
            stack.enter_context(patch.object(config, "IS_STAGING", False))
            stack.enter_context(patch.object(config, "IS_PRODUCTION", True))
            stack.enter_context(patch.object(config, "IS_LOCKED_ENV", True))
            stack.enter_context(patch.object(config, "TELEGRAM_API_ID", 0))
            stack.enter_context(patch.object(config, "TELEGRAM_API_HASH", ""))
            stack.enter_context(patch.object(config, "TELEGRAM_PHONE", ""))
            stack.enter_context(patch.object(config, "SUPABASE_URL", "https://example.supabase.co"))
            stack.enter_context(patch.object(config, "SUPABASE_SERVICE_ROLE_KEY", "service-role"))
            stack.enter_context(patch.object(config, "NEO4J_URI", "bolt://localhost:7687"))
            stack.enter_context(patch.object(config, "NEO4J_PASSWORD", "password"))
            stack.enter_context(patch.object(config, "OPENAI_API_KEY", "openai"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_REQUIRE_AUTH", True))
            stack.enter_context(patch.object(config, "CORS_ALLOW_ORIGINS", ["https://app.example.com"]))
            stack.enter_context(patch.object(config, "REDIS_URL", "redis://localhost:6379/0"))
            stack.enter_context(patch.object(config, "ADMIN_API_KEY", "admin-secret"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_BASE_URL", "https://openclaw.example.com/v1"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TOKEN", "gateway-token"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TRANSPORT", "openai_compatible"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_MODEL", "openclaw/tg-analyst-ru"))
            stack.enter_context(patch.object(config, "OPENCLAW_WEB_SESSION_KEY", "web-session"))
            stack.enter_context(patch.object(config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"))
            with self.assertRaises(EnvironmentError) as ctx:
                config.validate()

        self.assertIn("TELEGRAM_API_ID", str(ctx.exception))

    def test_locked_environment_requires_openai_compatible_model(self) -> None:
        with ExitStack() as stack:
            stack.enter_context(patch.object(config, "IS_LOCKED_ENV", True))
            stack.enter_context(patch.object(config, "TELEGRAM_API_ID", 1))
            stack.enter_context(patch.object(config, "TELEGRAM_API_HASH", "hash"))
            stack.enter_context(patch.object(config, "TELEGRAM_PHONE", "phone"))
            stack.enter_context(patch.object(config, "SUPABASE_URL", "https://example.supabase.co"))
            stack.enter_context(patch.object(config, "SUPABASE_SERVICE_ROLE_KEY", "service-role"))
            stack.enter_context(patch.object(config, "NEO4J_URI", "bolt://localhost:7687"))
            stack.enter_context(patch.object(config, "NEO4J_PASSWORD", "password"))
            stack.enter_context(patch.object(config, "OPENAI_API_KEY", "openai"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_REQUIRE_AUTH", True))
            stack.enter_context(patch.object(config, "CORS_ALLOW_ORIGINS", ["https://app.example.com"]))
            stack.enter_context(patch.object(config, "REDIS_URL", "redis://localhost:6379/0"))
            stack.enter_context(patch.object(config, "ADMIN_API_KEY", "admin-secret"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_BASE_URL", "https://openclaw.example.com/v1"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TOKEN", "gateway-token"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TRANSPORT", "openai_compatible"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_MODEL", ""))
            stack.enter_context(patch.object(config, "OPENCLAW_WEB_SESSION_KEY", "web-session"))
            stack.enter_context(patch.object(config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"))
            with self.assertRaises(EnvironmentError) as ctx:
                config.validate()

        self.assertIn("OPENCLAW_GATEWAY_MODEL", str(ctx.exception))

    def test_locked_environment_requires_legacy_agent_for_legacy_transport(self) -> None:
        with ExitStack() as stack:
            stack.enter_context(patch.object(config, "IS_LOCKED_ENV", True))
            stack.enter_context(patch.object(config, "TELEGRAM_API_ID", 1))
            stack.enter_context(patch.object(config, "TELEGRAM_API_HASH", "hash"))
            stack.enter_context(patch.object(config, "TELEGRAM_PHONE", "phone"))
            stack.enter_context(patch.object(config, "SUPABASE_URL", "https://example.supabase.co"))
            stack.enter_context(patch.object(config, "SUPABASE_SERVICE_ROLE_KEY", "service-role"))
            stack.enter_context(patch.object(config, "NEO4J_URI", "bolt://localhost:7687"))
            stack.enter_context(patch.object(config, "NEO4J_PASSWORD", "password"))
            stack.enter_context(patch.object(config, "OPENAI_API_KEY", "openai"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_REQUIRE_AUTH", True))
            stack.enter_context(patch.object(config, "CORS_ALLOW_ORIGINS", ["https://app.example.com"]))
            stack.enter_context(patch.object(config, "REDIS_URL", "redis://localhost:6379/0"))
            stack.enter_context(patch.object(config, "ADMIN_API_KEY", "admin-secret"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_FRONTEND", "frontend-secret"))
            stack.enter_context(patch.object(config, "ANALYTICS_API_KEY_OPENCLAW", "openclaw-secret"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_BASE_URL", "https://openclaw.example.com"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TOKEN", "gateway-token"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_TRANSPORT", "legacy"))
            stack.enter_context(patch.object(config, "OPENCLAW_GATEWAY_MODEL", ""))
            stack.enter_context(patch.object(config, "OPENCLAW_ANALYTICS_AGENT_ID", ""))
            stack.enter_context(patch.object(config, "OPENCLAW_WEB_SESSION_KEY", "web-session"))
            stack.enter_context(patch.object(config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "admin-user"))
            with self.assertRaises(EnvironmentError) as ctx:
                config.validate()

        self.assertIn("OPENCLAW_ANALYTICS_AGENT_ID", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
