from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
