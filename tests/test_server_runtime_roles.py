from __future__ import annotations

import os
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import config
from api import server


class ServerRuntimeRoleTests(unittest.TestCase):
    def test_normalize_app_role_defaults_to_all(self) -> None:
        self.assertEqual(server._normalize_app_role(None), "all")
        self.assertEqual(server._normalize_app_role(""), "all")
        self.assertEqual(server._normalize_app_role("mystery"), "all")

    def test_should_run_background_jobs_for_all_and_worker_only(self) -> None:
        self.assertTrue(server._should_run_background_jobs("all"))
        self.assertTrue(server._should_run_background_jobs("worker"))
        self.assertFalse(server._should_run_background_jobs("web"))

    def test_topic_overview_materializer_follows_background_jobs_and_feature_flag(self) -> None:
        original_role = server.APP_ROLE
        original_feature = server.config.FEATURE_TOPIC_OVERVIEWS_AI
        try:
            server.APP_ROLE = "all"
            server.config.FEATURE_TOPIC_OVERVIEWS_AI = True
            self.assertTrue(server._should_run_topic_overviews_materializer())

            server.APP_ROLE = "web"
            self.assertFalse(server._should_run_topic_overviews_materializer())

            server.APP_ROLE = "all"
            server.config.FEATURE_TOPIC_OVERVIEWS_AI = False
            self.assertFalse(server._should_run_topic_overviews_materializer())
        finally:
            server.APP_ROLE = original_role
            server.config.FEATURE_TOPIC_OVERVIEWS_AI = original_feature

    def test_staging_web_validation_does_not_require_telegram_runtime_credentials(self) -> None:
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {"APP_ROLE": "web"}, clear=False))
            stack.enter_context(patch.object(config, "IS_STAGING", True))
            stack.enter_context(patch.object(config, "IS_PRODUCTION", False))
            stack.enter_context(patch.object(config, "TELEGRAM_API_ID", 0))
            stack.enter_context(patch.object(config, "TELEGRAM_API_HASH", ""))
            stack.enter_context(patch.object(config, "TELEGRAM_PHONE", ""))
            stack.enter_context(patch.object(config, "SUPABASE_URL", "https://example.supabase.co"))
            stack.enter_context(patch.object(config, "SUPABASE_SERVICE_ROLE_KEY", "service-role"))
            stack.enter_context(patch.object(config, "NEO4J_URI", "bolt://localhost:7687"))
            stack.enter_context(patch.object(config, "NEO4J_PASSWORD", "password"))
            stack.enter_context(patch.object(config, "OPENAI_API_KEY", "openai"))

            config.validate()

    def test_production_runtime_validation_requires_telegram_runtime_credentials(self) -> None:
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {"APP_ROLE": "all"}, clear=False))
            stack.enter_context(patch.object(config, "IS_STAGING", False))
            stack.enter_context(patch.object(config, "IS_PRODUCTION", True))
            stack.enter_context(patch.object(config, "TELEGRAM_API_ID", 0))
            stack.enter_context(patch.object(config, "TELEGRAM_API_HASH", ""))
            stack.enter_context(patch.object(config, "TELEGRAM_PHONE", ""))
            stack.enter_context(patch.object(config, "SUPABASE_URL", "https://example.supabase.co"))
            stack.enter_context(patch.object(config, "SUPABASE_SERVICE_ROLE_KEY", "service-role"))
            stack.enter_context(patch.object(config, "NEO4J_URI", "bolt://localhost:7687"))
            stack.enter_context(patch.object(config, "NEO4J_PASSWORD", "password"))
            stack.enter_context(patch.object(config, "OPENAI_API_KEY", "openai"))
            with self.assertRaises(EnvironmentError) as ctx:
                config.validate()

        self.assertIn("TELEGRAM_API_ID", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
